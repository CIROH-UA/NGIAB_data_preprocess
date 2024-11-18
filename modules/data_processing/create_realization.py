import json
import multiprocessing
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas
import s3fs
import xarray as xr
from dask.distributed import Client, LocalCluster
from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import (GeoPackage, get_cat_to_nex_flowpairs,
                                        get_cat_to_nhd_feature_id)
from tqdm.rich import tqdm


def get_approximate_gw_storage(paths: file_paths, start_date: datetime):
    # get the gw levels from the NWM output on a given start date
    # this kind of works in place of warmstates for now
    year = start_date.strftime("%Y")
    formatted_dt = start_date.strftime("%Y%m%d%H%M")
    cat_to_feature = get_cat_to_nhd_feature_id(paths.geopackage_path)

    fs = s3fs.S3FileSystem(anon=True)
    nc_url = f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/netcdf/GWOUT/{year}/{formatted_dt}.GWOUT_DOMAIN1"

    # make sure there's a dask cluster running
    try:
        client = Client.current()
    except ValueError:
        cluster = LocalCluster()
        client = Client(cluster)

    with fs.open(nc_url) as file_obj:
        ds = xr.open_dataset(file_obj)

        water_levels = dict()
        for cat, feature in tqdm(cat_to_feature.items()):
            # this value is in CM, we need meters to match max_gw_depth
            # xarray says it's in mm, with 0.1 scale factor. calling .values doesn't apply the scale
            water_level = ds.sel(feature_id=feature).depth.values / 100
            water_levels[cat] = water_level

    return water_levels


def make_cfe_config(
    divide_conf_df: pandas.DataFrame, files: file_paths, water_levels: dict
) -> None:
    """Parses parameters from NOAHOWP_CFE DataFrame and returns a dictionary of catchment configurations."""
    with open(file_paths.template_cfe_config, "r") as f:
        cfe_template = f.read()
    cat_config_dir = files.config_dir / "cat_config" / "CFE"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for _, row in divide_conf_df.iterrows():
        gw_storage_ratio = water_levels[row["divide_id"]] / row["mean.Zmax"]
        cat_config = cfe_template.format(
            bexp=row["mode.bexp_soil_layers_stag=2"],
            dksat=row["geom_mean.dksat_soil_layers_stag=2"],
            psisat=row["geom_mean.psisat_soil_layers_stag=2"],
            slope=row["mean.slope"],
            smcmax=row["mean.smcmax_soil_layers_stag=2"],
            smcwlt=row["mean.smcwlt_soil_layers_stag=2"],
            max_gw_storage=row["mean.Zmax"] if row["mean.Zmax"] is not None else "0.011[m]",
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

    divide_conf_df.set_index("divide_id", inplace=True)
    start_datetime = start_time.strftime("%Y%m%d%H%M")
    end_datetime = end_time.strftime("%Y%m%d%H%M")
    with open(file_paths.template_noahowp_config, "r") as file:
        template = file.read()

    cat_config_dir = base_dir / "cat_config" / "NOAH-OWP-M"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for divide in divide_conf_df.index:
        with open(cat_config_dir / f"{divide}.input", "w") as file:
            file.write(
                template.format(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    lat=divide_conf_df.loc[divide, "centroid_y"],
                    lon=divide_conf_df.loc[divide, "centroid_x"],
                    terrain_slope=divide_conf_df.loc[divide, "mean.slope"],
                    azimuth=divide_conf_df.loc[divide, "circ_mean.aspect"],
                    ISLTYP=int(divide_conf_df.loc[divide, "mode.ISLTYP"]),
                    IVGTYP=int(divide_conf_df.loc[divide, "mode.IVGTYP"]),
                )
            )


def make_dd_config(
    hydrofabric: Path,
    output_dir: Path,
    template_path: Path = file_paths.template_dd_config,
):
    # This incantation took a while
    with GeoPackage(hydrofabric) as conn:
        sql = """WITH source_crs AS (
        SELECT organization || ':' || organization_coordsys_id AS crs_string
        FROM gpkg_spatial_ref_sys 
        WHERE srs_id = (
            SELECT srs_id 
            FROM gpkg_geometry_columns 
            WHERE table_name = 'divides'
        )
        )
        SELECT 
        d.divide_id, 
        d.areasqkm, 
        da."mean.slope", 
        da."mean.elevation",
        ST_X(Transform(MakePoint(da.centroid_x, da.centroid_y), 4326, NULL, 
            (SELECT crs_string FROM source_crs), 'EPSG:4326')) AS longitude,
        ST_Y(Transform(MakePoint(da.centroid_x, da.centroid_y), 4326, NULL, 
            (SELECT crs_string FROM source_crs), 'EPSG:4326')) AS latitude
        FROM divides AS d 
        JOIN 'divide-attributes' AS da ON d.divide_id = da.divide_id 
        """
        divide_conf_df = pandas.read_sql_query(sql, conn)
    divide_conf_df.set_index("divide_id", inplace=True)

    cat_config_dir = output_dir / "cat_config" / "dd"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    with open(template_path, "r") as file:
        template = file.read()

    for divide in divide_conf_df.index:
        with open(cat_config_dir / f"{divide}.yml", "w") as file:
            file.write(
                template.format(
                    area_sqkm=divide_conf_df.loc[divide, "areasqkm"],
                    divide_id=divide,
                    lat=divide_conf_df.loc[divide, "latitude"],
                    lon=divide_conf_df.loc[divide, "longitude"],
                    slope_mean=divide_conf_df.loc[divide, "mean.slope"],
                    elevation_mean=divide_conf_df.loc[divide, "mean.slope"],
                )
            )


def configure_troute(
    cat_id: str, config_dir: Path, start_time: datetime, end_time: datetime
) -> int:

    with open(file_paths.template_troute_config, "r") as file:
        troute_template = file.read()
    time_step_size = 300
    nts = (end_time - start_time).total_seconds() / time_step_size
    seconds_in_hour = 3600
    number_of_hourly_steps = nts * time_step_size / seconds_in_hour
    filled_template = troute_template.format(
        # hard coded to 5 minutes
        time_step_size=time_step_size,
        # troute seems to be ok with setting this to your cpu_count
        cpu_pool=multiprocessing.cpu_count(),
        geo_file_path=f"./config/{cat_id}_subset.gpkg",
        start_datetime=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        nts=nts,
        max_loop_size=nts,
        stream_output_time=number_of_hourly_steps,
    )

    with open(config_dir / "troute.yaml", "w") as file:
        file.write(filled_template)

    return nts


def make_ngen_realization_json(
    config_dir: Path, template_path: Path, start_time: datetime, end_time: datetime, nts: int
) -> None:
    with open(template_path, "r") as file:
        realization = json.load(file)

    realization["time"]["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["output_interval"] = 3600

    with open(config_dir / "realization.json", "w") as file:
        json.dump(realization, file, indent=4)


def create_dd_realization(cat_id: str, start_time: datetime, end_time: datetime):
    paths = file_paths(cat_id)
    template_path = file_paths.template_dd_realization_config
    dd_config = file_paths.template_dd_model_config
    # move dd_config to paths.config_dir
    with open(dd_config, "r") as f:
        dd_config = f.read()
    with open(paths.config_dir / "dd-config.yml", "w") as f:
        f.write(dd_config)

    num_timesteps = configure_troute(cat_id, paths.config_dir, start_time, end_time)
    make_ngen_realization_json(
        paths.config_dir, template_path, start_time, end_time, num_timesteps
    )
    make_dd_config(paths.geopackage_path, paths.config_dir)
    # create some partitions for parallelization
    paths.setup_run_folders()
    create_partitions(paths)


def create_realization(cat_id: str, start_time: datetime, end_time: datetime):
    paths = file_paths(cat_id)

    # get approximate groundwater levels from nwm output
    template_path = paths.template_cfe_nowpm_realization_config
    with sqlite3.connect(paths.geopackage_path) as conn:
        conf_df = pandas.read_sql_query("SELECT * FROM 'divide-attributes';", conn)
    gw_levels = get_approximate_gw_storage(paths, start_time)
    make_cfe_config(conf_df, paths, gw_levels)

    make_noahowp_config(paths.config_dir, conf_df, start_time, end_time)

    num_timesteps = configure_troute(cat_id, paths.config_dir, start_time, end_time)

    make_ngen_realization_json(
        paths.config_dir, template_path, start_time, end_time, num_timesteps
    )

    # create some partitions for parallelization
    paths.setup_run_folders()
    create_partitions(paths)


def create_partitions(paths: Path, num_partitions: int = None) -> None:
    if num_partitions is None:
        num_partitions = multiprocessing.cpu_count()

    cat_to_nex_pairs = get_cat_to_nex_flowpairs(hydrofabric=paths.geopackage_path)
    nexus = defaultdict(list)

    for cat, nex in cat_to_nex_pairs:
        nexus[nex].append(cat)

    num_partitions = min(num_partitions, len(nexus))
    # partition_size = ceil(len(nexus) / num_partitions)
    # num_nexus = len(nexus)
    # nexus = list(nexus.items())
    # partitions = []
    # for i in range(0, num_nexus, partition_size):
    #     part = {}
    #     part["id"] = i // partition_size
    #     part["cat-ids"] = []
    #     part["nex-ids"] = []
    #     part["remote-connections"] = []
    #     for j in range(i, i + partition_size):
    #         if j < num_nexus:
    #             part["cat-ids"].extend(nexus[j][1])
    #             part["nex-ids"].append(nexus[j][0])
    #     partitions.append(part)

    # with open(paths.subset_dir / f"partitions_{num_partitions}.json", "w") as f:
    #     f.write(json.dumps({"partitions": partitions}, indent=4))

    # write this to a metadata file to save on repeated file io to recalculate
    with open(paths.metadata_dir / "num_partitions", "w") as f:
        f.write(str(num_partitions))


if __name__ == "__main__":
    cat_id = "cat-1643991"
    start_time = datetime(2010, 1, 1, 0, 0, 0)
    end_time = datetime(2010, 1, 2, 0, 0, 0)
    # output_interval = 3600
    # nts = 2592
    create_realization(cat_id, start_time, end_time)
