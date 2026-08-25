"""Functions to generate BMI config files."""

import logging
import multiprocessing
import os
import shutil
import sqlite3
from datetime import datetime
from itertools import chain
from pathlib import Path
from typing import Dict

import duckdb
import numpy as np
import pandas
import psutil
import s3fs
import xarray as xr
from data_processing.dask_utils import temp_cluster
from data_processing.file_paths import FilePaths
from data_processing.gpkg_utils import get_cat_to_nhd_feature_id, get_table_crs_short
from pyproj import Transformer
from tqdm.rich import tqdm

logger = logging.getLogger(__name__)


# TODO: this function doesn't work anymore, get rid of it? deprecate argument?
@temp_cluster
def _get_approximate_gw_storage(paths: FilePaths, start_date: datetime) -> Dict[str, int]:
    # get the gw levels from the NWM output on a given start date
    # this kind of works in place of warmstates for now
    year = start_date.strftime("%Y")
    formatted_dt = start_date.strftime("%Y%m%d%H%M")
    cat_to_feature = get_cat_to_nhd_feature_id(paths.geopackage_path)

    fs = s3fs.S3FileSystem(anon=True)
    nc_url = (
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/netcdf/GWOUT/{year}/{formatted_dt}"
        + ".GWOUT_DOMAIN1"
    )

    with fs.open(nc_url) as file_obj:
        ds = xr.open_dataset(file_obj)  # type: ignore

        water_levels: Dict[str, int] = dict()
        for cat, feature in tqdm(cat_to_feature.items()):
            # this value is in CM, we need meters to match max_gw_depth
            # xarray says it's in mm, with 0.1 scale factor. calling .values doesn't apply the scale
            water_level = ds.sel(feature_id=feature).depth.values / 100
            water_levels[cat] = water_level

    return water_levels


def _get_model_attributes(hydrofabric: Path, layer: str = "divides") -> pandas.DataFrame:
    with sqlite3.connect(hydrofabric) as conn:
        conf_df = pandas.read_sql_query(
            """
            SELECT
            d.areasqkm,
            d.lengthkm,
            da.*
            FROM divides AS d
            JOIN 'divide-attributes' AS da ON d.divide_id = da.divide_id
            """,
            conn,
        )
    source_crs = get_table_crs_short(hydrofabric, layer)
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(conf_df["centroid_x"].values, conf_df["centroid_y"].values)
    conf_df["longitude"] = lon
    conf_df["latitude"] = lat

    # convert elevation in cm in hf to m
    conf_df["mean.elevation"] = conf_df["mean.elevation"] / 100

    return conf_df


# CONFIG GENERATORS!!


def _make_cfe_config(
    divide_conf_df: pandas.DataFrame, files: FilePaths, water_levels: dict
) -> None:
    """
    Parses parameters from NOAHOWP_CFE DataFrame and returns a dictionary of catchment \
    configurations.
    """
    with open(FilePaths.template_cfe_config, "r", encoding="utf-8") as f:
        cfe_template = f.read()
    cat_config_dir = files.config_dir / "cat_config" / "CFE"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in divide_conf_df.iterrows():
        nwm_water_level = water_levels.get(row["divide_id"], None)
        # if we have the nwm output water level for that catchment, use it
        # otherwise, use 5%
        if nwm_water_level is not None:
            gw_storage_ratio = water_levels[row["divide_id"]] / row["mean.Zmax"]
        else:
            gw_storage_ratio = 0.05
        cat_config = cfe_template.format(
            bexp=row["mode.bexp_soil_layers_stag=2"],
            dksat=row["geom_mean.dksat_soil_layers_stag=2"],
            psisat=row["geom_mean.psisat_soil_layers_stag=2"],
            slope=row["mean.slope_1km"],
            smcmax=row["mean.smcmax_soil_layers_stag=2"],
            smcwlt=row["mean.smcwlt_soil_layers_stag=2"],
            max_gw_storage=row["mean.Zmax"] / 1000
            if row["mean.Zmax"] is not None
            else "0.011[m]",  # mean.Zmax is in mm!
            gw_Coeff=row["mean.Coeff"] if row["mean.Coeff"] is not None else "0.0018[m h-1]",
            gw_Expon=row["mode.Expon"],
            gw_storage=f"{gw_storage_ratio:.5}",
            refkdt=row["mean.refkdt"],
        )
        cat_ini_file = cat_config_dir / f"{row['divide_id']}.ini"
        with open(cat_ini_file, "w", encoding="utf-8") as f:
            f.write(cat_config)


def _make_noahowp_config(
    base_dir: Path, divide_conf_df: pandas.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    start_datetime = start_time.strftime("%Y%m%d%H%M")
    end_datetime = end_time.strftime("%Y%m%d%H%M")
    with open(FilePaths.template_noahowp_config, "r", encoding="utf-8") as file:
        template = file.read()

    cat_config_dir = base_dir / "cat_config" / "NOAH-OWP-M"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in divide_conf_df.iterrows():
        with open(cat_config_dir / f"{row['divide_id']}.input", "w", encoding="utf-8") as file:
            file.write(
                template.format(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    lat=row["latitude"],
                    lon=row["longitude"],
                    terrain_slope=row["mean.slope_1km"],
                    azimuth=row["circ_mean.aspect"],
                    ISLTYP=int(row["mode.ISLTYP"]),  # type: ignore
                    IVGTYP=int(row["mode.IVGTYP"]),  # type: ignore
                )
            )


def _make_snow17_config(
    base_dir: Path, divide_conf_df: pandas.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    snow17_atts = duckdb.sql(f"""
        SELECT * FROM '{FilePaths.snow17_attributes}'
        WHERE divide_id IN {tuple(divide_conf_df["divide_id"])}
    """).df()

    merged = snow17_atts.merge(
        divide_conf_df[["divide_id", "areasqkm", "lengthkm", "latitude", "mean.elevation"]],
        on="divide_id",
    )

    start_datetime = start_time.strftime("%Y%m%d%H")
    end_datetime = end_time.strftime("%Y%m%d%H")
    with open(FilePaths.template_snow17_config, "r", encoding="utf-8") as config_file:
        config_template = config_file.read()

    cat_config_dir = base_dir / "cat_config" / "SNOW17"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in merged.iterrows():
        with open(cat_config_dir / f"{row['divide_id']}.input", "w", encoding="utf-8") as file:
            file.write(
                config_template.format(
                    divide_id=row["divide_id"],
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            )

    with open(FilePaths.template_snow17_params, "r", encoding="utf-8") as params_file:
        params_template = params_file.read()

    for _, row in merged.iterrows():
        with open(cat_config_dir / f"params-{row['divide_id']}.txt", "w", encoding="utf-8") as file:
            file.write(
                params_template.format(
                    divide_id=row["divide_id"],
                    areasqkm=row["areasqkm"],
                    latitude=row["latitude"],
                    elevation=row["mean.elevation"],
                    scf=row["scf"],
                    mfmax=row["mfmax"],
                    mfmin=row["mfmin"],
                    uadj=row["uadj"],
                    si=row["si"],
                    pxtemp=row["pxtemp"],
                    nmf=row["nmf"],
                    tipm=row["tipm"],
                    mbase=row["mbase"],
                    plwhc=row["plwhc"],
                    daygm=row["daygm"],
                    adc1=row["adc1"],
                    adc2=row["adc2"],
                    adc3=row["adc3"],
                    adc4=row["adc4"],
                    adc5=row["adc5"],
                    adc6=row["adc6"],
                    adc7=row["adc7"],
                    adc8=row["adc8"],
                    adc9=row["adc9"],
                    adc10=row["adc10"],
                    adc11=row["adc11"],
                )
            )


def _make_sacsma_config(
    base_dir: Path, divide_conf_df: pandas.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    sacsma_atts = duckdb.sql(f"""
        SELECT * FROM '{FilePaths.sacsma_attributes}'
        WHERE divide_id IN {tuple(divide_conf_df["divide_id"])}
    """).df()

    merged = sacsma_atts.merge(divide_conf_df[["divide_id", "areasqkm"]], on="divide_id")

    start_datetime = start_time.strftime("%Y%m%d%H")
    end_datetime = end_time.strftime("%Y%m%d%H")
    with open(FilePaths.template_sac_config, "r", encoding="utf-8") as config_file:
        config_template = config_file.read()

    cat_config_dir = base_dir / "cat_config" / "SAC-SMA"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in merged.iterrows():
        with open(cat_config_dir / f"{row['divide_id']}.input", "w", encoding="utf-8") as file:
            file.write(
                config_template.format(
                    divide_id=row["divide_id"],
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                )
            )

    with open(FilePaths.template_sac_params, "r", encoding="utf-8") as params_file:
        params_template = params_file.read()

    for _, row in merged.iterrows():
        with open(cat_config_dir / f"params-{row['divide_id']}.txt", "w", encoding="utf-8") as file:
            file.write(
                params_template.format(
                    divide_id=row["divide_id"],
                    areasqkm=row["areasqkm"],
                    uztwm=row["uztwm"],
                    uzfwm=row["uzfwm"],
                    lztwm=row["lztwm"],
                    lzfpm=row["lzfpm"],
                    lzfsm=row["lzfsm"],
                    adimp=row["adimp"],
                    uzk=row["uzk"],
                    lzpk=row["lzpk"],
                    lzsk=row["lzsk"],
                    zperc=row["zperc"],
                    rexp=row["rexp"],
                    pctim=row["pctim"],
                    pfree=row["pfree"],
                    riva=row["riva"],
                    side=row["side"],
                    rserv=row["rserv"],
                )
            )


def _make_lstm_config(
    hydrofabric: Path,
    output_dir: Path,
    template_path: Path = FilePaths.template_lstm_config,
):
    divide_conf_df = _get_model_attributes(hydrofabric)

    cat_config_dir = output_dir / "cat_config" / "lstm"
    if cat_config_dir.exists():
        shutil.rmtree(cat_config_dir)
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    # convert the mean.slope from degrees 0-90 where 90 is flat and 0 is vertical to m/km
    # flip 0 and 90 degree values
    divide_conf_df["flipped_mean_slope"] = abs(divide_conf_df["mean.slope"] - 90)
    # Convert degrees to meters per kmmeter
    divide_conf_df["mean_slope_mpkm"] = (
        np.tan(np.radians(divide_conf_df["flipped_mean_slope"])) * 1000
    )

    with open(template_path, "r", encoding="utf-8") as file:
        template = file.read()

    for _, row in divide_conf_df.iterrows():
        divide = row["divide_id"]
        with open(cat_config_dir / f"{divide}.yml", "w", encoding="utf-8") as file:
            file.write(
                template.format(
                    area_sqkm=row["areasqkm"],
                    divide_id=divide,
                    lat=row["latitude"],
                    lon=row["longitude"],
                    slope_mean=row["mean_slope_mpkm"],
                    elevation_mean=row["mean.elevation"],  # convert cm in hf to m
                )
            )


def _make_dhbv2_config(
    hydrofabric: Path,
    output_dir: Path,
    start_time: datetime,
    end_time: datetime,
    template_path: Path = FilePaths.template_dhbv2_config,
    output_suffix: str = "",
    clear_dir: bool = True,
):
    divide_conf_df = _get_model_attributes(hydrofabric)
    dhbv_atts = duckdb.sql(f"""
        SELECT * FROM '{FilePaths.dhbv_attributes}'
        WHERE divide_id IN {tuple(divide_conf_df["divide_id"])}
    """).df()
    if template_path == FilePaths.template_dhbv2_daily_config:
        cat_config_dir = output_dir / "cat_config" / "dhbv2_daily"
    else:
        cat_config_dir = output_dir / "cat_config" / "dhbv2"
    if cat_config_dir.exists() and clear_dir:
        shutil.rmtree(cat_config_dir)
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    template = template_path.read_text()
    merged = dhbv_atts.merge(
        divide_conf_df[["divide_id", "areasqkm", "lengthkm", "latitude"]], on="divide_id"
    )

    for _, row in merged.iterrows():
        (cat_config_dir / f"{row['divide_id']}{output_suffix}.yml").write_text(
            template.format(
                **row,
                start_time=start_time,
                end_time=end_time,
                start_date=start_time.strftime("%Y/%m/%d"),
                start_time_str=start_time.strftime("%Y/%m/%d %H"),
            )
        )


def _make_casam_config(
    base_dir: Path,
    divide_conf_df: pandas.DataFrame,
    start_time: datetime,
    end_time: datetime,
    sft_coupled: bool,
) -> None:
    """Generates CASAM BMI config

    Args:
        base_dir (Path): ngen run directory, usually the name of the cat-ID
        divide_conf_df (pandas.DataFrame): Subsetted hydrofabric layer
        start_time (datetime): Start time of simulation
        end_time (datetime): End time of simulation
        sft_coupled (bool): True if SFT is coupled, False if not
    """
    casam_atts = duckdb.sql(f"""
        SELECT * FROM '{FilePaths.casam_attributes}'
        WHERE divide_id IN {tuple(divide_conf_df["divide_id"])}
    """).df()

    def fmt_list(value) -> str:
        return ",".join(str(v) for v in value)

    elapsed_time = end_time - start_time
    elapsed_hours = elapsed_time.total_seconds() / 3600

    with open(FilePaths.template_casam_config, "r", encoding="utf-8") as config_file:
        config_template = config_file.read()

    cat_config_dir = base_dir / "cat_config" / "CASAM"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in casam_atts.iterrows():
        with open(cat_config_dir / f"{row['divide_id']}.input", "w", encoding="utf-8") as file:
            file.write(
                config_template.format(
                    layer_thickness=fmt_list(row["layer_thickness"]),
                    initial_psi=row["initial_psi"],
                    ponded_depth_max=row["ponded_depth_max"],
                    endtime=elapsed_hours,
                    layer_soil_type=fmt_list(row["layer_soil_type"]),
                    max_valid_soil_types=row["max_valid_soil_types"],
                    wilting_point_psi=row["wilting_point_psi"],
                    field_capacity_psi=row["field_capacity_psi"],
                    giuh_ordinates=fmt_list(row["giuh_ordinates"]),
                    sft_coupled=str(sft_coupled).lower(),
                    soil_z=fmt_list(row["soil_z"]),
                )
            )


def _configure_troute(
    cat_id: str, config_dir: Path, start_time: datetime, end_time: datetime
) -> None:
    with open(FilePaths.template_troute_config, "r", encoding="utf-8") as file:
        troute_template = file.read()
    time_step_size = 300
    output_name = Path(cat_id).name
    gpkg_file_path = config_dir / f"{output_name}_subset.gpkg"
    nts = (end_time - start_time).total_seconds() / time_step_size
    with sqlite3.connect(gpkg_file_path) as conn:
        ncats_df = pandas.read_sql_query("SELECT COUNT(id) FROM 'divides';", conn)
        ncats = ncats_df["COUNT(id)"][0]

    est_bytes_required = nts * ncats * 45  # extremely rough calculation based on about 3 tests :)
    local_ram_available = (
        0.8 * psutil.virtual_memory().available
    )  # buffer to not accidentally explode machine

    if est_bytes_required > local_ram_available:
        max_loop_size = nts // (est_bytes_required // local_ram_available)
        binary_nexus_file_folder_comment = ""
        parent_dir = config_dir.parent
        output_parquet_path = Path(f"{parent_dir}/outputs/parquet/")

        if not output_parquet_path.exists():
            os.makedirs(output_parquet_path)
    else:
        max_loop_size = nts
        binary_nexus_file_folder_comment = "#"

    filled_template = troute_template.format(
        # hard coded to 5 minutes
        time_step_size=time_step_size,
        # troute seems to be ok with setting this to your cpu_count
        cpu_pool=multiprocessing.cpu_count(),
        geo_file_path=f"./config/{output_name}_subset.gpkg",
        start_datetime=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        nts=nts,
        max_loop_size=max_loop_size,
        binary_nexus_file_folder_comment=binary_nexus_file_folder_comment,
    )

    with open(config_dir / "troute.yaml", "w", encoding="utf-8") as file:
        file.write(filled_template)


# SUMMA specific functions


def _make_summa_config(hru_ids: list[int], output_dir: Path):
    with open(FilePaths.template_summa_config, "r", encoding="utf-8") as file:
        template = file.read()
    cat_config_dir = output_dir / "cat_config" / "SUMMA"
    cat_config_dir.mkdir(parents=True, exist_ok=True)
    for i, hru_id in enumerate(hru_ids):
        divide = f"cat-{hru_id}"
        with open(cat_config_dir / f"{divide}.input", "w", encoding="utf-8") as file:
            # + 1 because fortran's first index is 1 not 0
            file.write(template.format(divide_index=i + 1))


def _get_hru_order(forcing_path: Path) -> list[int]:
    # the SUMMA hru (hydrologic response unit) is like a nextgen hydrofabric catchment
    # to correctly format the input data we need the order of these ids to be consistent
    if not forcing_path.exists():
        raise FileNotFoundError(
            f"Unable to create SUMMA configuration without forcing file {forcing_path}"
        )
    forcings = xr.open_dataset(forcing_path)
    return [int(s.split("-")[-1]) for s in forcings.ids.values]


def _make_summa_attributes(hru_ids, hydrofabric):
    cat_ids = [f"cat-{id}" for id in hru_ids]
    divide_conf_df = _get_model_attributes(hydrofabric)
    divide_conf_df = divide_conf_df.set_index("divide_id")

    # Validate all IDs exist
    for hid in cat_ids:
        if hid not in divide_conf_df.index:
            raise ValueError(f"HRU ID {hid} is not present in hydrofabric model attributes")

    # Subset and preserve order
    df = divide_conf_df.loc[cat_ids]

    n_hru = len(hru_ids)
    hru_id_array = np.array(hru_ids, dtype=np.int32)

    # Convert area from sq km to sq m
    hru_area = df["areasqkm"].values * 1e6

    # Convert elevation from cm to m
    elevation = df["mean.elevation"].values

    # Convert mean.slope (degrees, 90=flat 0=vertical) to tan_slope (m/m)
    flipped = np.abs(df["mean.slope"].values - 90)
    tan_slope = np.tan(np.radians(flipped))

    ds = xr.Dataset(
        {
            "hruId": xr.DataArray(
                data=hru_id_array,
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index of hydrological response unit (HRU)"},
            ),
            "gruId": xr.DataArray(
                data=hru_id_array.copy(),
                dims=["gru"],
                attrs={"units": "-", "long_name": "Index of grouped response unit (GRU)"},
            ),
            "hru2gruId": xr.DataArray(
                data=hru_id_array.copy(),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index of GRU to which the HRU belongs"},
            ),
            "downHRUindex": xr.DataArray(
                data=np.full(n_hru, 0, dtype=np.int32),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index of downslope HRU (0 = basin outlet)"},
            ),
            "longitude": xr.DataArray(
                data=df["longitude"].values.astype(np.float64),
                dims=["hru"],
                attrs={"units": "Decimal degree east", "long_name": "Longitude of HRUs centroid"},
            ),
            "latitude": xr.DataArray(
                data=df["latitude"].values.astype(np.float64),
                dims=["hru"],
                attrs={"units": "Decimal degree north", "long_name": "Latitude of HRUs centroid"},
            ),
            "elevation": xr.DataArray(
                data=elevation.astype(np.float64),
                dims=["hru"],
                attrs={"units": "m", "long_name": "Mean HRU elevation"},
            ),
            "HRUarea": xr.DataArray(
                data=hru_area.astype(np.float64),
                dims=["hru"],
                attrs={"units": "m^2", "long_name": "Area of HRU"},
            ),
            "tan_slope": xr.DataArray(
                data=tan_slope.astype(np.float64),
                dims=["hru"],
                attrs={"units": "m m-1", "long_name": "Average tangent slope of HRU"},
            ),
            "contourLength": xr.DataArray(
                data=np.full(n_hru, 100.0, dtype=np.float64),
                dims=["hru"],
                attrs={"units": "m", "long_name": "Contour length of HRU"},
            ),
            "slopeTypeIndex": xr.DataArray(
                data=np.full(n_hru, 1, dtype=np.int32),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index defining slope"},
            ),
            "soilTypeIndex": xr.DataArray(
                data=df["mode.ISLTYP"].values.astype(np.int32),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index defining soil type"},
            ),
            "vegTypeIndex": xr.DataArray(
                data=df["mode.IVGTYP"].values.astype(np.int32),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index defining vegetation type"},
            ),
            "mHeight": xr.DataArray(
                data=np.full(n_hru, 1.5, dtype=np.float64),
                dims=["hru"],
                attrs={"units": "m", "long_name": "Measurement height above bare ground"},
            ),
        },
        attrs={
            "Author": "Created by ngiab preprocessor SUMMA workflow script",
            "History": f"Created {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
        },
    )

    return ds


def _make_summa_trialParams(hru_ids: list[int], timesteps: int) -> xr.Dataset:  # pylint: disable=invalid-name
    ds = xr.Dataset(
        {
            "hruId": xr.DataArray(
                data=np.array(hru_ids, dtype=np.int32),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index of hydrological response unit (HRU)"},
            ),
            "maxstep": xr.DataArray(
                data=np.full(len(hru_ids), timesteps * 3600, dtype=np.float64),
                dims=["hru"],
            ),
        },
        attrs={
            "Author": "Created by ngiab preprocessor SUMMA workflow script",
            "History": f"Created {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
            "Purpose": "Create a trial parameter .nc file for initial SUMMA runs",
        },
    )
    return ds


def _make_summa_coldState(hru_ids):  # pylint: disable=invalid-name
    n_hru = len(hru_ids)
    n_midToto = 3  # pylint: disable=invalid-name
    n_ifcToto = 4  # pylint: disable=invalid-name

    def scalar_var(fill_val, dtype=np.float64):
        return xr.DataArray(
            data=np.full((1, n_hru), fill_val, dtype=dtype),
            dims=["scalarv", "hru"],
        )

    def layer_var(fill_val, dim_name, dim_size):
        return xr.DataArray(
            data=np.full((dim_size, n_hru), fill_val, dtype=np.float64),
            dims=[dim_name, "hru"],
        )

    iLayerHeight_data = np.broadcast_to(  # pylint: disable=invalid-name
        np.array([0.0, 0.2, 0.5, 1.0])[:, np.newaxis],
        (n_ifcToto, n_hru),
    ).copy()

    mLayerDepth_data = np.broadcast_to(  # pylint: disable=invalid-name
        np.array([0.2, 0.3, 0.5])[:, np.newaxis],
        (n_midToto, n_hru),
    ).copy()

    ds = xr.Dataset(
        {
            "hruId": xr.DataArray(
                data=np.array(hru_ids, dtype=np.int32),
                dims=["hru"],
                attrs={"units": "-", "long_name": "Index of hydrological response unit (HRU)"},
            ),
            "dt_init": scalar_var(3600.0),
            "nSoil": scalar_var(3, dtype=np.int32),  # type: ignore
            "nSnow": scalar_var(0, dtype=np.int32),  # type: ignore
            "scalarCanopyIce": scalar_var(0.0),
            "scalarCanopyLiq": scalar_var(0.0),
            "scalarSnowDepth": scalar_var(0.0),
            "scalarSWE": scalar_var(0.0),
            "scalarSfcMeltPond": scalar_var(0.0),
            "scalarAquiferStorage": scalar_var(2.5),
            "scalarSnowAlbedo": scalar_var(0.0),
            "scalarCanairTemp": scalar_var(283.16),
            "scalarCanopyTemp": scalar_var(283.16),
            "mLayerTemp": layer_var(283.16, "midToto", n_midToto),
            "mLayerVolFracIce": layer_var(0.0, "midToto", n_midToto),
            "mLayerVolFracLiq": layer_var(0.2, "midToto", n_midToto),
            "mLayerMatricHead": layer_var(-1.0, "midToto", n_midToto),
            "iLayerHeight": xr.DataArray(data=iLayerHeight_data, dims=["ifcToto", "hru"]),
            "mLayerDepth": xr.DataArray(data=mLayerDepth_data, dims=["midToto", "hru"]),
        },
        attrs={
            "Author": "Created by SUMMA workflow scripts",
            "History": f"Created {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}",
            "Purpose": "Create a cold state .nc file for initial SUMMA runs",
        },
    )

    # Add midSoil dimension (SUMMA expects it even though no variable uses it)
    ds = ds.assign_coords(midSoil=np.arange(n_midToto))

    # Build encoding to suppress _FillValue on all variables
    encoding = {var: {"_FillValue": None} for var in ds.data_vars}

    return ds, encoding


def _make_summa_config_suite(cat_id: str, start_time: datetime, end_time: datetime):
    """Makes SUMMA configuration files: model config, attributes, trial params, and
    cold state, plus copies the static SUMMA file manager suite into the run directory.

    Args:
        cat_id (str): Catchment ID
        start_time (datetime): Start time of simulation
        end_time (datetime): End time of simulation
    """
    paths = FilePaths(cat_id)
    paths.summa_model_config.mkdir(parents=True, exist_ok=True)

    files = chain(
        FilePaths.summa_file_dir.glob("*.txt"),
        FilePaths.summa_file_dir.glob("*.TBL"),
        FilePaths.summa_file_dir.glob("*.md"),
    )
    for file in files:
        if file.name == "fileManager.txt":
            with open(file, "r", encoding="utf-8") as f:
                template = f.read()
            with open(paths.summa_model_config / file.name, "w", encoding="utf-8") as f:
                f.write(template.format(start_time=start_time, end_time=end_time))
        else:
            shutil.copy(file, paths.summa_model_config)

    max_timesteps = int((end_time - start_time).total_seconds() / 3600)
    hru_ids = _get_hru_order(paths.forcings_file)
    _make_summa_attributes(hru_ids, FilePaths.conus_hydrofabric).to_netcdf(
        paths.summa_model_config / "attributes.nc"
    )
    _make_summa_trialParams(hru_ids, max_timesteps).to_netcdf(
        paths.summa_model_config / "trialParams.nc"
    )
    ds, encoding = _make_summa_coldState(hru_ids)
    ds.to_netcdf(paths.summa_model_config / "coldState.nc", encoding=encoding)
    _make_summa_config(hru_ids, paths.config_dir)
    paths.setup_run_folders(["outputs/summa"])


def create_modular_configs(  # pylint: disable=too-many-branches
    output_folder: str,
    start_time: datetime,
    end_time: datetime,
    models: list[str],
    *,
    routing: bool,
):
    """Creates a BMI configuration files based on the specified models.

    Args:
        output_folder (str): Name of the output folder, usually the cat-id
        start_time (str): Start time of simulation in YYYY-MM-DD HH:MM:SS
        end_time (str): End time of simulation in YYYY-MM-DD HH:MM:SS
        models (list[str]): List of models to be coupled together
        routing (bool, optional): True if t-route is coupled. Defaults to False.

    Raises:
        NotImplementedError: Raised when user tries to generate a configuration for a model not
            supported by this module yet
    """
    paths = FilePaths(output_folder)
    conf_df = _get_model_attributes(paths.geopackage_path)

    for model in models:
        if model == "cfe":
            # currently does not support pulling GW from NWM
            # pretty sure that upstream implementation is broken right now
            _make_cfe_config(conf_df, paths, {})
        elif model == "nom":
            _make_noahowp_config(paths.config_dir, conf_df, start_time, end_time)
        elif model == "snow17":
            _make_snow17_config(paths.config_dir, conf_df, start_time, end_time)
        elif model == "sac-sma":
            _make_sacsma_config(paths.config_dir, conf_df, start_time, end_time)
        elif model in ("lstm", "lstm_rust"):
            _make_lstm_config(paths.geopackage_path, paths.config_dir)
        elif model == "dhbv2":
            _make_dhbv2_config(paths.geopackage_path, paths.config_dir, start_time, end_time)
        elif model == "dhbv2_daily":
            _make_dhbv2_config(
                paths.geopackage_path,
                paths.config_dir,
                start_time,
                end_time,
                template_path=FilePaths.template_dhbv2_daily_config,
            )
        elif model == "sloth":
            pass  # no config file needed for SLoTH
        elif model == "casam":
            sft_coupled = "sft" in models
            _make_casam_config(paths.config_dir, conf_df, start_time, end_time, sft_coupled)
        elif model == "summa":
            _make_summa_config_suite(output_folder, start_time, end_time)
        else:
            # config generation not supported for PET, SFT, SMP, TOPMODEL yet
            raise NotImplementedError(f"Config generation not yet supported for '{model}'")

    if routing:
        _configure_troute(output_folder, paths.config_dir, start_time, end_time)

    paths.setup_run_folders()  # creates config, output, and log folders if they don't exist
