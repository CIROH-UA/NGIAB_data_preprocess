import logging

import s3fs
import xarray as xr
from data_processing.dask_utils import use_cluster
from data_processing.dataset_utils import validate_dataset_format
from data_processing.s3fs_utils import S3ParallelFileSystem

logger = logging.getLogger(__name__)


@use_cluster
def load_v3_retrospective_zarr(location: str, forcing_vars: list[str] = None) -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    # if a LocalCluster is not already running, start one
    if not forcing_vars:
        forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]

    if location == "conus":
        s3_urls = [
            f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr" for var in forcing_vars
        ]
    elif location == "hi":
        s3_urls = ["s3://noaa-nwm-retrospective-3-0-pds/Hawaii/zarr/forcing.zarr"]
        # only one forcing zarr store for hawaii, contains all variables
    else:
        raise ValueError(f"Invalid location: {location}. Must be 'conus' or 'hi'.")
    # default cache is readahead which is detrimental to performance in this case
    fs = S3ParallelFileSystem(anon=True, default_cache_type="none")  # default_block_size
    s3_stores = [s3fs.S3Map(url, s3=fs) for url in s3_urls]
    # the cache option here just holds accessed data in memory to prevent s3 being queried multiple times
    # most of the data is read once and written to disk but some of the coordinate data is read multiple times
    dataset = xr.open_mfdataset(s3_stores, parallel=True, engine="zarr", cache=True)

    # set the crs attribute to conform with the format
    esri_pe_string = dataset.crs.esri_pe_string
    dataset = dataset.drop_vars(["crs"])
    dataset.attrs["crs"] = esri_pe_string
    dataset.attrs["name"] = "v3_retrospective_zarr"

    # rename the data vars to work with ngen
    variables = {
        "LWDOWN": "DLWRF_surface",
        "PSFC": "PRES_surface",
        "Q2D": "SPFH_2maboveground",
        "RAINRATE": "precip_rate",
        "SWDOWN": "DSWRF_surface",
        "T2D": "TMP_2maboveground",
        "U2D": "UGRD_10maboveground",
        "V2D": "VGRD_10maboveground",
    }
    dataset = dataset.rename_vars(variables)

    validate_dataset_format(dataset)
    return dataset


@use_cluster
def load_aorc_zarr(start_year: int = None, end_year: int = None) -> xr.Dataset:
    """Load the aorc zarr dataset from S3."""
    if not start_year or not end_year:
        logger.warning("No start or end year provided, defaulting to 1979-2023")
        logger.warning("To reduce the time taken to load the data, provide a smaller range")
    if not start_year:
        start_year = 1979
    if not end_year:
        end_year = 2023

    logger.info(f"Loading AORC zarr datasets from {start_year} to {end_year}")
    estimated_time_s = ((end_year - start_year) * 2.5) + 3.5
    # from testing, it's about 2.1s per year + 3.5s overhead
    logger.info(f"This should take roughly {estimated_time_s} seconds")
    fs = S3ParallelFileSystem(anon=True, default_cache_type="none")
    s3_url = "s3://noaa-nws-aorc-v1-1-1km/"
    urls = [f"{s3_url}{i}.zarr" for i in range(start_year, end_year + 1)]
    filestores = [s3fs.S3Map(url, s3=fs) for url in urls]
    dataset = xr.open_mfdataset(filestores, parallel=True, engine="zarr", cache=True)
    dataset.attrs["crs"] = "+proj=longlat +datum=WGS84 +no_defs"
    dataset.attrs["name"] = "aorc_1km_zarr"
    # rename latitude and longitude to x and y
    dataset = dataset.rename({"latitude": "y", "longitude": "x"})

    validate_dataset_format(dataset)
    return dataset


@use_cluster
def load_swe_zarr() -> xr.Dataset:
    """Load the swe zarr dataset from S3."""
    s3_urls = ["s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/ldasout.zarr"]
    # default cache is readahead which is detrimental to performance in this case
    fs = S3ParallelFileSystem(anon=True, default_cache_type="none")  # default_block_size
    s3_stores = [s3fs.S3Map(url, s3=fs) for url in s3_urls]
    # the cache option here just holds accessed data in memory to prevent s3 being queried multiple times
    # most of the data is read once and written to disk but some of the coordinate data is read multiple times
    dataset = xr.open_mfdataset(s3_stores, parallel=True, engine="zarr", cache=True)

    # set the crs attribute to conform with the format
    esri_pe_string = dataset.crs.esri_pe_string
    dataset = dataset.drop_vars(["crs"])
    dataset.attrs["crs"] = esri_pe_string
    # drop everything except SNEQV
    vars_to_drop = list(dataset.data_vars)
    vars_to_drop.remove("SNEQV")
    dataset = dataset.drop_vars(vars_to_drop)
    dataset.attrs["name"] = "v3_swe_zarr"

    # rename the data vars to work with ngen
    variables = {"SNEQV": "swe"}
    dataset = dataset.rename_vars(variables)

    validate_dataset_format(dataset)
    return dataset
