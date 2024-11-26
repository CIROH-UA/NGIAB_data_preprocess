import logging
import os
from pathlib import Path
from typing import Tuple

import geopandas as gpd
import numpy as np
import s3fs
from data_processing.s3fs_utils import S3ParallelFileSystem
import xarray as xr
from dask.distributed import Client, LocalCluster, progress
from data_processing.file_paths import file_paths
from fsspec.mapping import FSMap


logger = logging.getLogger(__name__)


def load_zarr_datasets() -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    # if a LocalCluster is not already running, start one
    try:
        client = Client.current()
    except ValueError:
        cluster = LocalCluster()
        client = Client(cluster)
    forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    s3_urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr"
        for var in forcing_vars
    ]
    # default cache is readahead which is detrimental to performance in this case
    fs = S3ParallelFileSystem(anon=True, default_cache_type="none")  # default_block_size
    s3_stores = [s3fs.S3Map(url, s3=fs) for url in s3_urls]
    # the cache option here just holds accessed data in memory to prevent s3 being queried multiple times
    # most of the data is read once and written to disk but some of the coordinate data is read multiple times
    dataset = xr.open_mfdataset(s3_stores, parallel=True, engine="zarr", cache=True)
    return dataset


def validate_time_range(dataset: xr.Dataset, start_time: str, end_time: str) -> Tuple[str, str]:
    end_time_in_dataset = dataset.time.isel(time=-1).values
    start_time_in_dataset = dataset.time.isel(time=0).values
    if np.datetime64(start_time) < start_time_in_dataset:
        logger.warning(
            f"provided start {start_time} is before the start of the dataset {start_time_in_dataset}, selecting from {start_time_in_dataset}"
        )
        start_time = start_time_in_dataset
    if np.datetime64(end_time) > end_time_in_dataset:
        logger.warning(
            f"provided end {end_time} is after the end of the dataset {end_time_in_dataset}, selecting until {end_time_in_dataset}"
        )
        end_time = end_time_in_dataset
    return start_time, end_time


def clip_dataset_to_bounds(
    dataset: xr.Dataset, bounds: Tuple[float, float, float, float], start_time: str, end_time: str
) -> xr.Dataset:
    """Clip the dataset to specified geographical bounds."""
    # check time range here in case just this function is imported and not the whole module
    start_time, end_time = validate_time_range(dataset, start_time, end_time)
    dataset = dataset.sel(
        x=slice(bounds[0], bounds[2]),
        y=slice(bounds[1], bounds[3]),
        time=slice(start_time, end_time),
    )
    logger.info("Selected time range and clipped to bounds")
    return dataset


def compute_store(stores: xr.Dataset, cached_nc_path: Path) -> xr.Dataset:
    """Compute the store and save it to a cached netCDF file."""
    logger.info("Downloading and caching forcing data, this may take a while")

    # sort of terrible work around for half downloaded files
    temp_path = cached_nc_path.with_suffix(".downloading.nc")
    if os.path.exists(temp_path):
        os.remove(temp_path)

    ## Drop crs that's included with one of the datasets
    stores = stores.drop_vars("crs")

    ## Cast every single variable to float32 to save space to save a lot of memory issues later
    ## easier to do it now in this slow download step than later in the steps without dask
    for var in stores.data_vars:
        stores[var] = stores[var].astype("float32")

    client = Client.current()
    future = client.compute(stores.to_netcdf(temp_path, compute=False))
    # Display progress bar
    progress(future)
    future.result()

    os.rename(temp_path, cached_nc_path)

    data = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")
    return data


def get_forcing_data(
    forcing_paths: file_paths, start_time: str, end_time: str, gdf: gpd.GeoDataFrame
) -> xr.Dataset:
    merged_data = None
    if os.path.exists(forcing_paths.cached_nc_file):
        logger.info("Found cached nc file")
        # open the cached file and check that the time range is correct
        cached_data = xr.open_mfdataset(
            forcing_paths.cached_nc_file, parallel=True, engine="h5netcdf"
        )
        range_in_cache = cached_data.time[0].values <= np.datetime64(
            start_time
        ) and cached_data.time[-1].values >= np.datetime64(end_time)

        if not range_in_cache:
            # only do this if the time range is not in the cache as it is slow
            # this catches cases where a user entered 2030 as the end on the first run and the cache only goes to 2023
            # it will prevent the cache from being deleted and reloaded every time
            lazy_store = load_zarr_datasets()
            start_time, end_time = validate_time_range(lazy_store, start_time, end_time)

        if range_in_cache:
            logger.info("Time range is within cached data")
            logger.debug(f"Opened cached nc file: [{forcing_paths.cached_nc_file}]")
            merged_data = clip_dataset_to_bounds(
                cached_data, gdf.total_bounds, start_time, end_time
            )
            logger.debug("Clipped stores")
        else:
            logger.info("Time range is incorrect")
            os.remove(forcing_paths.cached_nc_file)
            logger.debug("Removed cached nc file")

    if merged_data is None:
        logger.info("Loading zarr stores")
        # create new event loop
        lazy_store = load_zarr_datasets()
        logger.debug("Got zarr stores")
        clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds, start_time, end_time)
        logger.info("Clipped forcing data to bounds")
        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file)
        logger.info("Forcing data loaded and cached")
        # close the event loop

    return merged_data
