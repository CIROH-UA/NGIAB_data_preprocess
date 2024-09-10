from datetime import datetime
from pathlib import Path
from collections import defaultdict
from statistics import mean
import multiprocessing
import pandas
import json
import s3fs
import xarray as xr

from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import get_cat_to_nex_flowpairs, get_cat_to_nhd_feature_id


def get_approximate_gw_storage(paths: file_paths, start_date: datetime):
    # get the gw levels from the NWM output on a given start date
    # this kind of works in place of warmstates for now
    year = start_date.strftime("%Y")
    formatted_dt = start_date.strftime("%Y%m%d%H%M")
    cat_to_feature = get_cat_to_nhd_feature_id(paths.geopackage_path)

    fs = s3fs.S3FileSystem(anon=True)
    nc_url = f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/netcdf/GWOUT/{year}/{formatted_dt}.GWOUT_DOMAIN1"
    with fs.open(nc_url) as file_obj:
        ds = xr.open_dataset(file_obj)

        water_levels = dict()
        for cat, feature in cat_to_feature.items():
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
        gw_storage_ratio = water_levels[row["divide_id"]] / row["gw_Zmax"]
        cat_config = cfe_template.format(
            bexp=row["bexp_soil_layers_stag=2"],
            dksat=row["dksat_soil_layers_stag=2"],
            psisat=row["psisat_soil_layers_stag=2"],
            slope=row["slope"],
            smcmax=row["smcmax_soil_layers_stag=2"],
            smcwlt=row["smcwlt_soil_layers_stag=2"],
            max_gw_storage=row["gw_Zmax"] if row["gw_Zmax"] is not None else "0.011[m]",
            gw_Coeff=row["gw_Coeff"] if row["gw_Coeff"] is not None else "0.0018[m h-1]",
            gw_Expon=row["gw_Expon"],
            gw_storage="{:.5}".format(gw_storage_ratio),
            refkdt=row["refkdt"],
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
                    lat=divide_conf_df.loc[divide, "Y"],
                    lon=divide_conf_df.loc[divide, "X"],
                    terrain_slope=divide_conf_df.loc[divide, "slope_mean"],
                    azimuth=divide_conf_df.loc[divide, "aspect_c_mean"],
                    ISLTYP=divide_conf_df.loc[divide, "ISLTYP"],
                    IVGTYP=divide_conf_df.loc[divide, "IVGTYP"],
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
        geo_file_path=f"/ngen/ngen/data/config/{cat_id}_subset.gpkg",
        start_datetime=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        nts=nts,
        max_loop_size=nts,
        stream_output_time=number_of_hourly_steps,
    )

    with open(config_dir / "troute.yaml", "w") as file:
        file.write(filled_template)

    return nts


def make_ngen_realization_json(
    config_dir: Path, start_time: datetime, end_time: datetime, nts: int
) -> None:
    with open(file_paths.template_realization_config, "r") as file:
        realization = json.load(file)

    realization["time"]["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["output_interval"] = 3600
    realization["time"]["nts"] = nts

    with open(config_dir / "realization.json", "w") as file:
        json.dump(realization, file, indent=4)


def create_realization(cat_id: str, start_time: datetime, end_time: datetime):
    # quick wrapper to get the cfe realization working
    # without having to refactor this whole thing
    paths = file_paths(cat_id)

    # get approximate groundwater levels from nwm output
    gw_levels = get_approximate_gw_storage(paths, start_time)

    conf_df = pandas.read_csv(paths.config_dir / "cfe_noahowp_attributes.csv")
    make_cfe_config(conf_df, paths, gw_levels)

    make_noahowp_config(paths.config_dir, conf_df, start_time, end_time)

    num_timesteps = configure_troute(cat_id, paths.config_dir, start_time, end_time)

    make_ngen_realization_json(paths.config_dir, start_time, end_time, num_timesteps)

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
