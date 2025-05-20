import datetime
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import xarray as xr
from dask.distributed import Client, progress
from data_processing.dask_utils import use_cluster

logger = logging.getLogger(__name__)

# known ngen variable names
# https://github.com/CIROH-UA/ngen/blob/4fb5bb68dc397298bca470dfec94db2c1dcb42fe/include/forcing/AorcForcing.hpp#L77


def validate_dataset_format(dataset: xr.Dataset) -> None:
    """
    Validate the format of the dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to be validated.

    Raises
    ------
    ValueError
        If the dataset is not in the correct format.
    """
    if "time" not in dataset.coords:
        raise ValueError("Dataset must have a 'time' coordinate")
    if not np.issubdtype(dataset.time.dtype, np.datetime64):
        raise ValueError("Time coordinate must be a numpy datetime64 type")
    if "x" not in dataset.coords:
        raise ValueError("Dataset must have an 'x' coordinate")
    if "y" not in dataset.coords:
        raise ValueError("Dataset must have a 'y' coordinate")
    if "crs" not in dataset.attrs:
        raise ValueError("Dataset must have a 'crs' attribute")
    if "name" not in dataset.attrs:
        raise ValueError("Dataset must have a name attribute to identify it")


def validate_time_range(dataset: xr.Dataset, start_time: str, end_time: str) -> Tuple[str, str]:
    """
    Ensure that all selected times are in the passed dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset with a time coordinate.
    start_time : str
        Desired start time in YYYY/MM/DD HH:MM:SS format.
    end_time : str
        Desired end time in YYYY/MM/DD HH:MM:SS format.

    Returns
    -------
    str
        start_time, or if not available, earliest available timestep in dataset.
    str
        end_time, or if not available, latest available timestep in dataset.
    """
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
    """
    Clip the dataset to specified geographical bounds.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to be clipped.
    bounds : tuple[float, float, float, float]
        Corners of bounding box. bounds[0] is x_min, bounds[1] is y_min,
        bounds[2] is x_max, bounds[3] is y_max.
    start_time : str
        Desired start time in YYYY/MM/DD HH:MM:SS format.
    end_time : str
        Desired end time in YYYY/MM/DD HH:MM:SS format.

    Returns
    -------
    xr.Dataset
        Clipped dataset.
    """
    # check time range here in case just this function is imported and not the whole module
    start_time, end_time = validate_time_range(dataset, start_time, end_time)
    dataset = dataset.sel(
        x=slice(bounds[0], bounds[2]),
        y=slice(bounds[1], bounds[3]),
        time=slice(start_time, end_time),
    )
    logger.info("Selected time range and clipped to bounds")
    return dataset


def interpolate_nan_values(
    dataset: xr.Dataset,
    variables: Optional[List[str]] = None,
    dim: str = "time",
    method: str = "nearest",
    fill_value: str = "extrapolate",
    verbosity: int = 0,
) -> xr.Dataset:
    """
    Interpolates NaN values in specified (or all numeric time-dependent)
    variables of an xarray.Dataset. Operates on a copy of the dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        The input dataset.
    variables : Optional[List[str]], optional
        A list of variable names to process. If None (default),
        all numeric variables containing the specified dimension will be processed.
    dim : str, optional
        The dimension along which to interpolate (default is "time").
    method : str, optional
        Interpolation method to use (e.g., "linear", "nearest", "cubic").
        Default is "nearest".
    fill_value : str, optional
        Method for filling NaNs at the start/end of the series after interpolation.
        Set to "extrapolate" to fill with the nearest valid value when using 'nearest' or 'linear'.
        Default is "extrapolate".
    verbosity : int, optional
        Verbosity level for logging. 0 = silent (except warnings/errors),
        1 = info, 2 = debug using the module's logger. Default is 0.

    Returns
    -------
    xr.Dataset
        A new dataset with NaN values interpolated in the specified variables.
    """
    # This function uses the module-level 'logger'
    if verbosity >= 1:
        logger.info(f"Starting NaN interpolation for dimension '{dim}' using method '{method}'.")

    processed_ds = dataset.copy(deep=True)
    nan_found_overall = False
    actual_vars_considered_for_nan = []

    target_vars = variables if variables is not None else list(processed_ds.data_vars)

    for var_name in target_vars:
        if var_name not in processed_ds.data_vars:
            if verbosity >= 1 and variables is not None:
                logger.warning(
                    f"Variable '{var_name}' specified for interpolation not found. Skipping."
                )
            continue

        data_array = processed_ds[var_name]

        if dim not in data_array.dims:
            if verbosity >= 2 and (variables is not None or verbosity >= 2):
                logger.debug(f"Skipping variable '{var_name}': dimension '{dim}' not found.")
            continue

        if not np.issubdtype(data_array.dtype, np.number):
            if verbosity >= 2 and (variables is not None or verbosity >= 2):
                logger.debug(
                    f"Skipping variable '{var_name}': not a numeric data type ({data_array.dtype})."
                )
            continue

        actual_vars_considered_for_nan.append(var_name)
        # Check for NaNs, .compute() if it's a Dask array to get a boolean
        has_nans = data_array.isnull().any()
        if isinstance(has_nans, xr.DataArray):  # If Dask-backed, it will be a DataArray
            has_nans = has_nans.compute()

        if has_nans:
            nan_found_overall = True
            if verbosity >= 1:
                logger.info(f"NaNs found in variable '{var_name}'. Interpolating...")

            processed_ds[var_name] = data_array.interpolate_na(
                dim=dim,
                method=method,
                fill_value=fill_value if method in ["nearest", "linear"] else None,
            )

            # Re-check for NaNs after interpolation
            still_has_nans = processed_ds[var_name].isnull().any()
            if isinstance(still_has_nans, xr.DataArray):
                still_has_nans = still_has_nans.compute()

            if verbosity >= 1 and still_has_nans:
                logger.warning(
                    f"NaNs still present in '{var_name}' after interpolation. "
                    "This might occur if all values along the dimension are NaN "
                    "or due to limitations of the extrapolation for the chosen method."
                )
        elif verbosity >= 2:
            logger.debug(f"No NaNs found in variable '{var_name}'.")

    if not nan_found_overall and verbosity >= 1 and actual_vars_considered_for_nan:
        logger.info("No NaNs found needing interpolation in the processed variables.")
    elif nan_found_overall and verbosity >= 1:
        logger.info("NaN interpolation process completed for relevant variables.")
    elif not actual_vars_considered_for_nan and verbosity >= 1:
        logger.info("No suitable variables found for NaN interpolation with current settings.")

    return processed_ds


@use_cluster
def save_dataset(ds_to_save: xr.Dataset, target_path: Path, engine: str = "h5netcdf"):
    """
    Helper function to compute and save an xarray.Dataset to a NetCDF file.
    Uses a temporary file and rename for atomicity.
    """
    if not target_path.parent.exists():
        target_path.parent.mkdir(parents=True, exist_ok=True)

    temp_file_path = target_path.with_name(target_path.name + ".saving.nc")
    if temp_file_path.exists():
        os.remove(temp_file_path)

    client = Client.current()
    future = client.compute(ds_to_save.to_netcdf(temp_file_path, engine=engine, compute=False))
    logger.debug(
        f"NetCDF write task submitted to Dask. Waiting for completion to {temp_file_path}..."
    )
    progress(future)
    future.result()
    os.rename(str(temp_file_path), str(target_path))
    logger.info(f"Successfully saved data to: {target_path}")


@use_cluster
def check_for_nans(ds: xr.Dataset) -> bool:
    logger.debug("Checking for NaNs to determine if interpolation is necessary...")
    nans_present = False
    for name, var in ds.data_vars.items():
        if np.issubdtype(var.dtype, np.number):
            if var.isnull().any().compute():
                nans_present = True
                logger.warning(
                    f"NaNs detected in variable '{name}' that may require interpolation."
                )
                break  # Found one, no need to check further
    return nans_present


@use_cluster
def save_to_cache(
    stores: xr.Dataset, cached_nc_path: Path, interpolate_nans: bool = True
) -> xr.Dataset:
    """
    Compute the store and save it to a cached netCDF file. This is not required but will save time and bandwidth.
    """
    logger.info(f"Processing dataset for caching. Final cache target: {cached_nc_path}")

    # lasily cast all numbers to f32
    for name, var in stores.data_vars.items():
        if np.issubdtype(var.dtype, np.number):
            stores[name] = var.astype("float32", casting="same_kind")

    # save dataset locally before manipulating it
    save_dataset(stores, cached_nc_path)
    stores = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")

    # returns true if nans are present
    if check_for_nans(stores) and interpolate_nans:
        final_stores_to_save = interpolate_nan_values(dataset=stores)
        save_dataset(final_stores_to_save, cached_nc_path)
        stores = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")

    return stores


def check_local_cache(
    cached_nc_path: Path,
    start_time: str,
    end_time: str,
    gdf: gpd.GeoDataFrame,
    remote_dataset: xr.Dataset,
) -> Union[xr.Dataset, None]:
    merged_data = None

    if not os.path.exists(cached_nc_path):
        logger.info("No cache found")
        return

    logger.info("Found cached nc file")
    # open the cached file and check that the time range is correct
    cached_data = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")

    if "name" not in cached_data.attrs or "name" not in remote_dataset.attrs:
        logger.warning("No name attribute found to compare datasets")
        return
    if cached_data.name != remote_dataset.name:
        logger.warning("Cached data from different source, .name attr doesn't match")
        return

    range_in_cache = cached_data.time[0].values <= np.datetime64(start_time) and cached_data.time[
        -1
    ].values >= np.datetime64(end_time)

    if not range_in_cache:
        # the cache does not contain the desired time range
        logger.warning("Requested time range not in cache")
        return

    cached_vars = cached_data.data_vars.keys()
    forcing_vars = remote_dataset.data_vars.keys()
    # replace rainrate with precip
    missing_vars = set(forcing_vars) - set(cached_vars)
    if len(missing_vars) > 0:
        logger.warning(f"Missing forcing vars in cache: {missing_vars}")
        return

    if range_in_cache:
        logger.info("Time range is within cached data")
        logger.debug(f"Opened cached nc file: [{cached_nc_path}]")
        merged_data = clip_dataset_to_bounds(cached_data, gdf.total_bounds, start_time, end_time)
        logger.debug("Clipped stores")

    return merged_data


def save_and_clip_dataset(
    dataset: xr.Dataset,
    gdf: gpd.GeoDataFrame,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    cache_location: Path,
) -> xr.Dataset:
    """convenience function clip the remote dataset, and either load from cache or save to cache if it's not present"""
    gdf = gdf.to_crs(dataset.crs)

    cached_data = check_local_cache(cache_location, start_time, end_time, gdf, dataset)

    if not cached_data:
        clipped_data = clip_dataset_to_bounds(dataset, gdf.total_bounds, start_time, end_time)
        cached_data = save_to_cache(clipped_data, cache_location)
    return cached_data
