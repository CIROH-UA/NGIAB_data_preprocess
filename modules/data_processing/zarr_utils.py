import os
from pathlib import Path
from typing import Tuple
import s3fs
import xarray as xr
import logging
from dask.distributed import Client, LocalCluster, progress
import numpy as np
import geopandas as gpd
from data_processing.file_paths import file_paths
import time

logger = logging.getLogger(__name__)

def open_s3_store(url: str) -> s3fs.S3Map:
    """Open an s3 store from a given url."""
    return s3fs.S3Map(url, s3=s3fs.S3FileSystem(anon=True))

def load_zarr_datasets() -> xr.Dataset:
    """Load zarr datasets from S3 within the specified time range."""
    # if a LocalCluster is not already running, start one
    if not Client(timeout="2s"):
        cluster = LocalCluster()
    forcing_vars = ["lwdown", "precip", "psfc", "q2d", "swdown", "t2d", "u2d", "v2d"]
    s3_urls = [
        f"s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/forcing/{var}.zarr"
        for var in forcing_vars
    ]
    s3_stores = [open_s3_store(url) for url in s3_urls]
    dataset = xr.open_mfdataset(s3_stores, parallel=True, engine="zarr")
    return dataset

def clip_dataset_to_bounds(
    dataset: xr.Dataset, bounds: Tuple[float, float, float, float], start_time: str, end_time: str
) -> xr.Dataset:
    """Clip the dataset to specified geographical bounds."""
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

    client = Client.current()
    future = client.compute(stores.to_netcdf(cached_nc_path, compute=False))
    # Display progress bar
    progress(future)
    future.result()

    data = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")
    return data


def get_forcing_data(
    forcing_paths: file_paths, start_time: str, end_time: str, gdf: gpd.GeoDataFrame
) -> xr.Dataset:
    merged_data = None
    if os.path.exists(forcing_paths.cached_nc_file()):
        logger.info("Found cached nc file")
        # open the cached file and check that the time range is correct
        cached_data = xr.open_mfdataset(
            forcing_paths.cached_nc_file(), parallel=True, engine="h5netcdf"
        )
        if cached_data.time[0].values <= np.datetime64(start_time) and cached_data.time[
            -1
        ].values >= np.datetime64(end_time):
            logger.info("Time range is correct")
            logger.debug(f"Opened cached nc file: [{forcing_paths.cached_nc_file()}]")
            merged_data = clip_dataset_to_bounds(
                cached_data, gdf.total_bounds, start_time, end_time
            )
            logger.debug("Clipped stores")
        else:
            logger.info("Time range is incorrect")
            os.remove(forcing_paths.cached_nc_file())
            logger.debug("Removed cached nc file")

    if merged_data is None:
        logger.info("Loading zarr stores")
        lazy_store = load_zarr_datasets()
        logger.debug("Got zarr stores")
        clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds, start_time, end_time)
        logger.info("Clipped forcing data to bounds")
        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file())
        logger.info("Forcing data loaded and cached")

    return merged_data
