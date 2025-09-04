import json
import logging
import multiprocessing
import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas
import psutil
import requests
import s3fs
import xarray as xr
from data_processing.dask_utils import temp_cluster
from data_processing.file_paths import FilePaths
from data_processing.gpkg_utils import (
    get_cat_to_nhd_feature_id,
    get_table_crs_short,
)
from pyproj import Transformer
from tqdm.rich import tqdm

logger = logging.getLogger(__name__)


@temp_cluster
def get_approximate_gw_storage(paths: FilePaths, start_date: datetime) -> Dict[str, int]:
    # get the gw levels from the NWM output on a given start date
    # this kind of works in place of warmstates for now
    year = start_date.strftime("%Y")
    formatted_dt = start_date.strftime("%Y%m%d%H%M")
    cat_to_feature = get_cat_to_nhd_feature_id(paths.geopackage_path)

    fs = s3fs.S3FileSystem(anon=True)
    nc_url = f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/netcdf/GWOUT/{year}/{formatted_dt}.GWOUT_DOMAIN1"

    with fs.open(nc_url) as file_obj:
        ds = xr.open_dataset(file_obj)  # type: ignore

        water_levels: Dict[str, int] = dict()
        for cat, feature in tqdm(cat_to_feature.items()):
            # this value is in CM, we need meters to match max_gw_depth
            # xarray says it's in mm, with 0.1 scale factor. calling .values doesn't apply the scale
            water_level = ds.sel(feature_id=feature).depth.values / 100
            water_levels[cat] = water_level

    return water_levels


def make_cfe_config(divide_conf_df: pandas.DataFrame, files: FilePaths, water_levels: dict) -> None:
    """Parses parameters from NOAHOWP_CFE DataFrame and returns a dictionary of catchment configurations."""
    with open(FilePaths.template_cfe_config, "r") as f:
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
            gw_storage="{:.5}".format(gw_storage_ratio),
            refkdt=row["mean.refkdt"],
        )
        cat_ini_file = cat_config_dir / f"{row['divide_id']}.ini"
        with open(cat_ini_file, "w") as f:
            f.write(cat_config)


def make_noahowp_config(
    base_dir: Path, divide_conf_df: pandas.DataFrame, start_time: datetime, end_time: datetime
) -> None:
    start_datetime = start_time.strftime("%Y%m%d%H%M")
    end_datetime = end_time.strftime("%Y%m%d%H%M")
    with open(FilePaths.template_noahowp_config, "r") as file:
        template = file.read()

    cat_config_dir = base_dir / "cat_config" / "NOAH-OWP-M"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in divide_conf_df.iterrows():
        with open(cat_config_dir / f"{row['divide_id']}.input", "w") as file:
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


def get_model_attributes(hydrofabric: Path) -> pandas.DataFrame:
    with sqlite3.connect(hydrofabric) as conn:
        conf_df = pandas.read_sql_query(
            """
            SELECT
            d.areasqkm,
            da.*
            FROM divides AS d
            JOIN 'divide-attributes' AS da ON d.divide_id = da.divide_id
            """,
            conn,
        )
    source_crs = get_table_crs_short(hydrofabric, "divides")
    transformer = Transformer.from_crs(source_crs, "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(conf_df["centroid_x"].values, conf_df["centroid_y"].values)
    conf_df["longitude"] = lon
    conf_df["latitude"] = lat
    return conf_df


def make_lstm_config(
    hydrofabric: Path,
    output_dir: Path,
    template_path: Path = FilePaths.template_lstm_config,
):
    # test if modspatialite is available

    divide_conf_df = get_model_attributes(hydrofabric)

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

    with open(template_path, "r") as file:
        template = file.read()

    for _, row in divide_conf_df.iterrows():
        divide = row["divide_id"]
        with open(cat_config_dir / f"{divide}.yml", "w") as file:
            file.write(
                template.format(
                    area_sqkm=row["areasqkm"],
                    divide_id=divide,
                    lat=row["latitude"],
                    lon=row["longitude"],
                    slope_mean=row["mean_slope_mpkm"],
                    elevation_mean=row["mean.elevation"] / 100,  # convert cm in hf to m
                )
            )


def configure_troute(
    cat_id: str, config_dir: Path, start_time: datetime, end_time: datetime
) -> None:
    with open(FilePaths.template_troute_config, "r") as file:
        troute_template = file.read()
    time_step_size = 300
    gpkg_file_path = f"{config_dir}/{cat_id}_subset.gpkg"
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
        geo_file_path=f"./config/{cat_id}_subset.gpkg",
        start_datetime=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        nts=nts,
        max_loop_size=max_loop_size,
        binary_nexus_file_folder_comment=binary_nexus_file_folder_comment,
    )

    with open(config_dir / "troute.yaml", "w") as file:
        file.write(filled_template)


def make_ngen_realization_json(
    config_dir: Path, template_path: Path, start_time: datetime, end_time: datetime
) -> None:
    with open(template_path, "r") as file:
        realization = json.load(file)

    realization["time"]["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["output_interval"] = 3600

    with open(config_dir / "realization.json", "w") as file:
        json.dump(realization, file, indent=4)


def create_lstm_realization(cat_id: str, start_time: datetime, end_time: datetime):
    paths = FilePaths(cat_id)
    template_path = FilePaths.template_lstm_realization_config
    configure_troute(cat_id, paths.config_dir, start_time, end_time)
    make_ngen_realization_json(paths.config_dir, template_path, start_time, end_time)
    make_lstm_config(paths.geopackage_path, paths.config_dir)
    # create some partitions for parallelization
    paths.setup_run_folders()


def create_realization(
    cat_id: str,
    start_time: datetime,
    end_time: datetime,
    use_nwm_gw: bool = False,
    gage_id: Optional[str] = None,
):
    paths = FilePaths(cat_id)

    template_path = paths.template_cfe_nowpm_realization_config

    if gage_id is not None:
        # try and download s3:communityhydrofabric/hydrofabrics/community/gage_parameters/gage_id
        # if it doesn't exist, use the default
        url = f"https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/gage_parameters/{gage_id}.json"
        response = requests.get(url)
        if response.status_code == 200:
            new_template = requests.get(url).json()
            template_path = paths.config_dir / "downloaded_params.json"
            with open(template_path, "w") as f:
                json.dump(new_template, f)
            logger.info(f"downloaded calibrated parameters for {gage_id}")
        else:
            logger.warning(f"could not download parameters for {gage_id}, using default template")

    conf_df = get_model_attributes(paths.geopackage_path)

    if use_nwm_gw:
        gw_levels = get_approximate_gw_storage(paths, start_time)
    else:
        gw_levels = dict()

    make_cfe_config(conf_df, paths, gw_levels)

    make_noahowp_config(paths.config_dir, conf_df, start_time, end_time)

    configure_troute(cat_id, paths.config_dir, start_time, end_time)

    make_ngen_realization_json(paths.config_dir, template_path, start_time, end_time)

    # create some partitions for parallelization
    paths.setup_run_folders()
