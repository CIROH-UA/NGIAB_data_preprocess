# 1. IMPORTS
import logging
import os
from pathlib import Path
import shutil # NECESSARY ADDITION: For shutil.copy and shutil.move
from typing import Tuple, Union, List, Optional # NECESSARY ADDITION: List, Optional

import geopandas as gpd
import numpy as np
import pandas as pd # NECESSARY ADDITION: For pd.Timestamp in existing functions
import xarray as xr
from dask.distributed import Client, progress # Corrected: Client, progress
import datetime

# 2. MODULE-LEVEL LOGGER
logger = logging.getLogger(__name__)

# known ngen variable names
# https://github.com/CIROH-UA/ngen/blob/4fb5bb68dc397298bca470dfec94db2c1dcb42fe/include/forcing/AorcForcing.hpp#L77

# 3. CORE VALIDATION AND GENERAL UTILITIES
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
    if "crs" not in dataset.attrs: # This was original, rioxarray can also provide CRS
        # Consider also: if not (("crs" in dataset.attrs) or (hasattr(dataset, "rio") and dataset.rio.crs is not None)):
        raise ValueError("Dataset must have a 'crs' attribute")
    if "name" not in dataset.attrs:
        raise ValueError("Dataset must have a name attribute to identify it")

def validate_time_range(dataset: xr.Dataset, start_time: str, end_time: str) -> Tuple[str, str]:
    '''
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
    '''
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


# ========================================================================
# >>>>> NEW FUNCTIONS missing values imputation <<<<<
# ========================================================================

def interpolate_nan_values(
    dataset: xr.Dataset,
    variables: Optional[List[str]] = None,
    dim: str = "time",
    method: str = "nearest",
    fill_value: str = "extrapolate",
    verbosity: int = 0
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
                logger.warning(f"Variable '{var_name}' specified for interpolation not found. Skipping.")
            continue

        data_array = processed_ds[var_name]

        if dim not in data_array.dims:
            if verbosity >= 2 and (variables is not None or verbosity >=2):
                logger.debug(f"Skipping variable '{var_name}': dimension '{dim}' not found.")
            continue

        if not np.issubdtype(data_array.dtype, np.number):
            if verbosity >= 2 and (variables is not None or verbosity >=2):
                logger.debug(f"Skipping variable '{var_name}': not a numeric data type ({data_array.dtype}).")
            continue
        
        actual_vars_considered_for_nan.append(var_name)
        # Check for NaNs, .compute() if it's a Dask array to get a boolean
        has_nans = data_array.isnull().any()
        if isinstance(has_nans, xr.DataArray): # If Dask-backed, it will be a DataArray
            has_nans = has_nans.compute()

        if has_nans:
            nan_found_overall = True
            if verbosity >= 1:
                logger.info(f"NaNs found in variable '{var_name}'. Interpolating...")

            processed_ds[var_name] = data_array.interpolate_na(
                dim=dim,
                method=method,
                fill_value=fill_value if method in ['nearest', 'linear'] else None
            )

            # Re-check for NaNs after interpolation
            still_has_nans = processed_ds[var_name].isnull().any()
            if isinstance(still_has_nans, xr.DataArray):
                still_has_nans = still_has_nans.compute()

            if verbosity >= 1 and still_has_nans:
                logger.warning(f"NaNs still present in '{var_name}' after interpolation. "
                               "This might occur if all values along the dimension are NaN "
                               "or due to limitations of the extrapolation for the chosen method.")
        elif verbosity >= 2:
            logger.debug(f"No NaNs found in variable '{var_name}'.")

    if not nan_found_overall and verbosity >= 1 and actual_vars_considered_for_nan:
        logger.info("No NaNs found needing interpolation in the processed variables.")
    elif nan_found_overall and verbosity >= 1:
        logger.info("NaN interpolation process completed for relevant variables.")
    elif not actual_vars_considered_for_nan and verbosity >=1:
        logger.info("No suitable variables found for NaN interpolation with current settings.")

    return processed_ds


# 4. HELPER FOR CACHING
def _save_dataset_to_netcdf(ds_to_save: xr.Dataset, target_path: Path, engine: str = "h5netcdf"):
    """
    Helper function to compute and save an xarray.Dataset to a NetCDF file.
    Uses a temporary file and rename for atomicity.
    """
    if not target_path.parent.exists():
        target_path.parent.mkdir(parents=True, exist_ok=True)

    temp_file_path = target_path.with_name(target_path.name + ".saving_temp.nc")
    if temp_file_path.exists():
        try:
            os.remove(temp_file_path)
        except OSError as e:
            logger.error(f"Could not remove existing temporary file {temp_file_path}: {e}")
            # Depending on desired robustness, this could raise an error or try to proceed.

    logger.debug(f"Attempting to save dataset to temporary path: {temp_file_path}")
    dask_client_locally_started = False
    client = None
    try:
        client = Client.current()
        logger.debug("Using existing Dask client for saving.")
    except ValueError: # No global client found
        logger.info("No Dask client found for saving, starting a temporary local one.")
        try:
            # Using processes=False makes it a thread-based local cluster, often better for I/O bound tasks
            cluster = LocalCluster(processes=False, n_workers=None, threads_per_worker=None)
            client = Client(cluster)
            dask_client_locally_started = True
        except Exception as e_cluster:
            logger.error(f"Failed to start local Dask client: {e_cluster}. Saving will be serial.")
            client = None # Ensure client is None if startup fails

    try:
        if client:
            netcdf_write_future = ds_to_save.to_netcdf(temp_file_path, engine=engine, compute=False)
            logger.debug(f"NetCDF write task submitted to Dask. Waiting for completion to {temp_file_path}...")
            progress(netcdf_write_future) # This is dask.distributed.progress
            netcdf_write_future.result()
        else:
            logger.info(f"No Dask client. Saving serially to {temp_file_path}...")
            ds_to_save.to_netcdf(temp_file_path, engine=engine, compute=True)

        if target_path.exists(): # Necessary for os.rename on some OS, and shutil.move robustness
            os.remove(target_path)
        shutil.move(str(temp_file_path), str(target_path))
        logger.info(f"Successfully saved data to: {target_path}")

    except Exception as e:
        logger.error(f"Failed to save dataset to {target_path}. Error: {e}", exc_info=True)
        if temp_file_path.exists():
            try:
                os.remove(temp_file_path)
            except OSError as ose:
                logger.error(f"Failed to remove temporary file {temp_file_path} after error: {ose}")
        raise
    finally:
        if dask_client_locally_started and client:
            try:
                client.close()
                if hasattr(client, 'cluster') and client.cluster is not None: # Check if cluster object exists
                    client.cluster.close()
                logger.debug("Temporary Dask client and cluster for saving closed.")
            except Exception as ce:
                logger.warning(f"Could not close temporary Dask client/cluster: {ce}")


# 5. MAIN CACHING FUNCTIONS AND WORKFLOW
def save_to_cache(
    stores: xr.Dataset,
    cached_nc_path: Path,
    perform_nan_interpolation: bool = True,
    interpolation_vars: Optional[List[str]] = None,
    interpolation_dim: str = "time",
    interpolation_method: str = "nearest",
    interpolation_fill_value: str = "extrapolate",
    interpolation_verbosity: int = 0 # Matches verbosity of interpolate_nan_values
) -> xr.Dataset:
    """
    Casts data to float32. If imputation is enabled AND relevant NaNs are detected,
    it first saves a float32 raw version (`*_raw.nc`), then imputes NaNs on the
    original data (preserving original precision for interpolation step),
    casts the imputed data to float32, and saves it as the primary cache file (`cached_nc_path`).
    If imputation is disabled or no NaNs requiring imputation are found,
    only the float32 cast version of the original data is saved to `cached_nc_path`,
    and no `*_raw.nc` file is created.

    Parameters
    ----------
    stores : xr.Dataset
        Dataset to be cached. Can be Dask-backed.
    cached_nc_path : Path
        Base path for the cached NetCDF file. The final data will be saved here.
    perform_nan_interpolation : bool, optional
        Whether to perform NaN interpolation if NaNs are detected. Default is True.
    interpolation_vars : Optional[List[str]], optional
        List of variable names for NaN interpolation. Default is None (all suitable vars).
    interpolation_dim : str, optional
        Dimension for NaN interpolation. Default is "time".
    interpolation_method : str, optional
        Method for NaN interpolation. Default is "nearest".
    interpolation_fill_value : str, optional
        Fill value for NaN interpolation boundaries. Default is "extrapolate".
    interpolation_verbosity : int, optional
        Verbosity for NaN interpolation logging. Default is 0.

    Returns
    -------
    xr.Dataset
        The dataset, re-opened from the primary cache path (`cached_nc_path`).
    """
    logger.info(f"Processing dataset for caching. Final cache target: {cached_nc_path}")

    # Check if any relevant NaNs exist before deciding on the workflow
    needs_interpolation_check = False
    if perform_nan_interpolation:
        logger.debug("Checking for NaNs to determine if interpolation is necessary...")
        # Create a shallow copy for checking to avoid modifying original 'stores' during check
        # Dask operations like .isnull().any().compute() will trigger computation on this copy.
        stores_check_copy = stores.copy(deep=False)
        target_check_vars = interpolation_vars if interpolation_vars is not None else list(stores_check_copy.data_vars)
        for var_name_check in target_check_vars:
            if var_name_check in stores_check_copy.data_vars:
                data_array_check = stores_check_copy[var_name_check]
                if interpolation_dim in data_array_check.dims and \
                   np.issubdtype(data_array_check.dtype, np.number):
                    # For Dask arrays, .isnull().any() returns a Dask scalar, so .compute() it.
                    if data_array_check.isnull().any().compute():
                        needs_interpolation_check = True
                        logger.info(f"NaNs detected in variable '{var_name_check}' that may require interpolation.")
                        break # Found one, no need to check further
        if not needs_interpolation_check:
            logger.info("Imputation is enabled, but no NaNs requiring interpolation were detected with current settings.")

    # This is the dataset that will eventually be saved to cached_nc_path
    final_stores_to_save = None

    if perform_nan_interpolation and needs_interpolation_check:
        # --- Path 1: Imputation is ON and NaNs REQUIRING imputation were detected ---
        raw_cached_nc_path = cached_nc_path.with_name(cached_nc_path.stem + "_raw.nc")

        # 1a. Prepare and save the "_raw.nc" version (float32 cast of original `stores`)
        logger.info("Preparing raw data (casting to float32 for _raw.nc)...")
        stores_for_raw_save = stores.copy(deep=False)
        for var_name_raw in stores_for_raw_save.data_vars:
            if np.issubdtype(stores_for_raw_save[var_name_raw].dtype, np.number):
                stores_for_raw_save[var_name_raw] = stores_for_raw_save[var_name_raw].astype("float32")
        
        logger.info(f"Saving raw (float32 cast) version to: {raw_cached_nc_path}")
        _save_dataset_to_netcdf(stores_for_raw_save, raw_cached_nc_path)
        del stores_for_raw_save # Free memory if possible

        # 1b. Perform NaN Interpolation on the original `stores` (to use original precision if method sensitive)
        # `interpolate_nan_values` works on its own copy of `stores`.
        final_stores_to_save = interpolate_nan_values(
            dataset=stores,
            variables=interpolation_vars,
            dim=interpolation_dim,
            method=interpolation_method,
            fill_value=interpolation_fill_value,
            verbosity=interpolation_verbosity # Pass down verbosity
        )
        # Cast the imputed data to float32
        logger.info("Casting imputed data to float32 for final cache...")
        for var_name_final in final_stores_to_save.data_vars:
            if np.issubdtype(final_stores_to_save[var_name_final].dtype, np.number) and \
               final_stores_to_save[var_name_final].dtype != np.float32:
                final_stores_to_save[var_name_final] = final_stores_to_save[var_name_final].astype("float32")
    else:
        # --- Path 2: Imputation is OFF or NO NaNs requiring imputation were detected ---
        if not perform_nan_interpolation:
            logger.info("NaN interpolation is disabled by user.")
        # else: (already logged that no NaNs were found requiring interpolation)
            
        logger.info("Preparing final cache (casting to float32, no NaN interpolation performed).")
        final_stores_to_save = stores.copy(deep=False)
        for var_name_final in final_stores_to_save.data_vars:
            if np.issubdtype(final_stores_to_save[var_name_final].dtype, np.number):
                final_stores_to_save[var_name_final] = final_stores_to_save[var_name_final].astype("float32")
        # No "_raw.nc" file is explicitly created here, as it would be identical to cached_nc_path.
        # The user's request was "1st save the raw data ... then do the imputation and save it with the same naming"
        # This implies *_raw.nc is an artifact of the imputation path.

    # --- Save the `final_stores_to_save` version to the main `cached_nc_path` ---
    logger.info(f"Saving final processed version to: {cached_nc_path}")
    _save_dataset_to_netcdf(final_stores_to_save, cached_nc_path)

    # --- Re-open and return the dataset from the final (primary) cache path ---
    logger.info(f"Re-opening dataset from final cache: {cached_nc_path}")
    try:
        # Use chunks={} to allow Dask to manage chunking upon read if desired later
        data = xr.open_dataset(cached_nc_path, engine="h5netcdf", chunks={})
    except Exception as e_h5:
        logger.warning(f"Could not open cache {cached_nc_path} with h5netcdf (error: {e_h5}), trying default engine.")
        try:
            data = xr.open_dataset(cached_nc_path, chunks={}) # Fallback
        except Exception as e_def:
            logger.error(f"Failed to re-open cached file {cached_nc_path} with any engine: {e_def}")
            raise
    return data

# ========================================================================
# >>>>> END OF NEW OR MODIFIED FUNCTIONS <<<<<
# ========================================================================

def check_local_cache(
    cached_nc_path: Path,
    start_time: str,
    end_time: str,
    gdf: gpd.GeoDataFrame,
    remote_dataset: xr.Dataset
) -> Union[xr.Dataset, None]:

    merged_data = None

    if not os.path.exists(cached_nc_path):
        logger.info("No cache found")
        return

    logger.info("Found cached nc file")
    # open the cached file and check that the time range is correct
    cached_data = xr.open_mfdataset(
        cached_nc_path, parallel=True, engine="h5netcdf"
    )

    if "name" not in cached_data.attrs or "name" not in remote_dataset.attrs:
        logger.warning("No name attribute found to compare datasets")
        return
    if cached_data.name != remote_dataset.name:
        logger.warning("Cached data from different source, .name attr doesn't match")
        return

    range_in_cache = cached_data.time[0].values <= np.datetime64(
        start_time
    ) and cached_data.time[-1].values >= np.datetime64(end_time)

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
        merged_data = clip_dataset_to_bounds(
            cached_data, gdf.total_bounds, start_time, end_time
        )
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


# #Testing block

# if __name__ == "__main__":
#     # === Imports for Testing ===
#     import tempfile
#     from dask.distributed import LocalCluster, Client as DaskClient # Alias for clarity
#     # Ensure these are available for creating test data
#     import numpy as np
#     import pandas as pd
#     import xarray as xr
#     from pathlib import Path
#     import logging # Need logging again for test-specific logger
#     # Note: geopandas and rioxarray are NOT needed for this simplified test block

#     # === Setup Basic Logging for Testing ===
#     # This configures logging if the script is run directly.
#     # If the module logger is already configured by an importing script, this might be redundant.
#     if not logging.getLogger().hasHandlers(): # Avoid adding handlers multiple times
#         logging.basicConfig(
#             level=logging.DEBUG, # Show debug messages for detailed test output
#             format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#         )
#     # Use the logger defined at the module level (top of the file)
#     logger_test = logger
#     logger_test.info("--- Running Standalone Test for dataset_utils.py ---")

#     # === Setup a Dask LocalCluster ===
#     # Necessary for testing functions using dask.distributed.progress and Client.current()
#     dask_client_for_test = None
#     dask_cluster_for_test = None
#     logger_test.info("Setting up Dask LocalCluster for testing...")
#     try:
#         # Use threads (processes=False) as caching is often I/O bound
#         dask_cluster_for_test = LocalCluster(processes=False, n_workers=2, threads_per_worker=1)
#         dask_client_for_test = DaskClient(dask_cluster_for_test)
#         logger_test.info(f"Dask client started: {dask_client_for_test.dashboard_link}")
#     except Exception as e:
#         logger_test.error(f"Failed to start Dask client: {e}. Some Dask-dependent tests might fail or run serially.")

#     # === Create a Temporary Directory for Test Outputs ===
#     # This ensures test files don't clutter your main directories and get cleaned up.
#     with tempfile.TemporaryDirectory() as temp_dir_name:
#         temp_dir_path = Path(temp_dir_name)
#         logger_test.info(f"Created temporary directory for test outputs: {temp_dir_path}")

#         # === Synthetic Test Data ===
#         # Create data that passes the `validate_dataset_format` checks
#         times_dt64 = pd.date_range("2023-01-01", periods=5, freq="h").values # Ensure np.datetime64
#         x_coords = np.arange(0.5, 3.5, 1.0)
#         y_coords = np.arange(10.5, 13.5, 1.0)

#         # Dataset with NaNs
#         temp_data_nan = np.random.rand(len(times_dt64), len(y_coords), len(x_coords)) * 30
#         temp_data_nan[1, 1, 1] = np.nan # Inject NaN 1
#         temp_data_nan[3, 0, 0] = np.nan # Inject NaN 2
#         precip_data_nan = np.random.rand(len(times_dt64), len(y_coords), len(x_coords)) * 5
#         precip_data_nan[2, 1, 0] = np.nan # Inject NaN 3

#         ds_with_nans = xr.Dataset(
#             {
#                 "temperature": (("time", "y", "x"), temp_data_nan),
#                 "precipitation": (("time", "y", "x"), precip_data_nan),
#                 "non_numeric_var": (("time",), [f"event_{i}" for i in range(len(times_dt64))]) # Add non-numeric var
#             },
#             coords={"time": times_dt64, "y": y_coords, "x": x_coords},
#             attrs={"name": "test_dataset_with_nans", "crs": "EPSG:4326"} # Satisfy validation
#         )

#         # Dataset without NaNs (numeric vars only)
#         ds_no_nans = ds_with_nans.copy(deep=True)
#         ds_no_nans["temperature"] = ds_no_nans["temperature"].fillna(0.0) # Replace NaNs
#         ds_no_nans["precipitation"] = ds_no_nans["precipitation"].fillna(0.0)
#         ds_no_nans.attrs["name"] = "test_dataset_no_nans"

#         # Check initial state
#         num_nans_temp_start = ds_with_nans["temperature"].isnull().sum().item()
#         num_nans_precip_start = ds_with_nans["precipitation"].isnull().sum().item()
#         logger_test.info(f"Synthetic ds_with_nans 'temperature' initially has {num_nans_temp_start} NaNs")
#         logger_test.info(f"Synthetic ds_with_nans 'precipitation' initially has {num_nans_precip_start} NaNs")
#         logger_test.info(f"Synthetic ds_no_nans 'temperature' initially has {ds_no_nans['temperature'].isnull().sum().item()} NaNs")

#         # === 1. Test `interpolate_nan_values` ===
#         logger_test.info("\n--- Testing interpolate_nan_values ---")
#         try:
#             # Use a fresh copy for the test
#             test_ds_interp = ds_with_nans.copy(deep=True)
#             interpolated_ds = interpolate_nan_values(test_ds_interp, verbosity=1) # Test with verbosity=1

#             # Check numeric variables that should have been interpolated
#             temp_nans_after = interpolated_ds["temperature"].isnull().sum().item()
#             precip_nans_after = interpolated_ds["precipitation"].isnull().sum().item()
#             logger_test.info(f"NaNs after interpolation: Temp={temp_nans_after}, Precip={precip_nans_after}")
#             assert temp_nans_after == 0, "Temp NaNs remain after interpolation"
#             assert precip_nans_after == 0, "Precip NaNs remain after interpolation"

#             # Check that non-numeric variable was not modified (optional check)
#             assert interpolated_ds["non_numeric_var"].equals(ds_with_nans["non_numeric_var"]), "Non-numeric var changed"

#             logger_test.info("--> interpolate_nan_values test PASSED.")
#         except Exception as e:
#             logger_test.error(f"--> interpolate_nan_values test FAILED: {e}", exc_info=True)

#         # === 2. Test `save_to_cache` Scenarios ===
#         # These tests implicitly test _save_dataset_to_netcdf as well

#         # Scenario 2.1: Imputation ON, NaNs present -> Expect main cache (clean) + raw cache (NaNs)
#         logger_test.info("\n--- Testing save_to_cache (Imputation ON, NaNs PRESENT) ---")
#         cache_path_s1 = temp_dir_path / "cache_imputed.nc"
#         raw_cache_path_s1 = temp_dir_path / "cache_imputed_raw.nc"
#         try:
#             logger_test.debug(f"Calling save_to_cache for: {cache_path_s1}")
#             # Use a fresh copy to avoid side effects between tests
#             reopened_s1 = save_to_cache(ds_with_nans.copy(deep=True), cache_path_s1, perform_nan_interpolation=True, interpolation_verbosity=1)

#             logger_test.debug(f"Checking existence of {cache_path_s1} and {raw_cache_path_s1}")
#             assert cache_path_s1.exists(), f"Main cache file {cache_path_s1} not created for S1."
#             assert raw_cache_path_s1.exists(), f"Raw cache file {raw_cache_path_s1} not created for S1."

#             # Verify content
#             with xr.open_dataset(cache_path_s1, engine='h5netcdf') as ds_final_s1:
#                  assert ds_final_s1["temperature"].isnull().sum().item() == 0, f"NaNs in final imputed cache S1 ({cache_path_s1})"
#                  assert ds_final_s1["temperature"].dtype == np.float32, f"Final imputed cache S1 not float32 ({cache_path_s1})"
#             with xr.open_dataset(raw_cache_path_s1, engine='h5netcdf') as ds_raw_s1:
#                  assert ds_raw_s1["temperature"].isnull().sum().item() == num_nans_temp_start, f"Expected {num_nans_temp_start} NaNs in raw cache S1 ({raw_cache_path_s1})"
#                  assert ds_raw_s1["temperature"].dtype == np.float32, f"Raw cache S1 not float32 ({raw_cache_path_s1})"

#             logger_test.info("--> save_to_cache (Imputation ON, NaNs PRESENT) test PASSED.")
#             reopened_s1.close() # Close the handle returned by save_to_cache
#         except Exception as e:
#             logger_test.error(f"--> save_to_cache (Imputation ON, NaNs PRESENT) test FAILED: {e}", exc_info=True)

#         # Scenario 2.2: Imputation ON, NO NaNs present -> Expect main cache (clean), NO raw cache
#         logger_test.info("\n--- Testing save_to_cache (Imputation ON, NO NaNs) ---")
#         cache_path_s2 = temp_dir_path / "cache_no_nans.nc"
#         raw_cache_path_s2 = temp_dir_path / "cache_no_nans_raw.nc"
#         try:
#             logger_test.debug(f"Calling save_to_cache for: {cache_path_s2}")
#             reopened_s2 = save_to_cache(ds_no_nans.copy(deep=True), cache_path_s2, perform_nan_interpolation=True, interpolation_verbosity=1)

#             logger_test.debug(f"Checking existence of {cache_path_s2} and non-existence of {raw_cache_path_s2}")
#             assert cache_path_s2.exists(), f"Main cache file {cache_path_s2} not created for S2."
#             assert not raw_cache_path_s2.exists(), f"Raw cache file {raw_cache_path_s2} WAS created for S2 (should not exist)."

#             with xr.open_dataset(cache_path_s2, engine='h5netcdf') as ds_final_s2:
#                  assert ds_final_s2["temperature"].isnull().sum().item() == 0, f"NaNs found in no-NaN cache S2 ({cache_path_s2})"
#                  assert ds_final_s2["temperature"].dtype == np.float32, f"Final no-NaN cache S2 not float32 ({cache_path_s2})"

#             logger_test.info("--> save_to_cache (Imputation ON, NO NaNs) test PASSED.")
#             reopened_s2.close()
#         except Exception as e:
#             logger_test.error(f"--> save_to_cache (Imputation ON, NO NaNs) test FAILED: {e}", exc_info=True)

#         # Scenario 2.3: Imputation OFF, NaNs present -> Expect main cache (NaNs), NO raw cache
#         logger_test.info("\n--- Testing save_to_cache (Imputation OFF, NaNs PRESENT) ---")
#         cache_path_s3 = temp_dir_path / "cache_imputation_off.nc"
#         raw_cache_path_s3 = temp_dir_path / "cache_imputation_off_raw.nc"
#         try:
#             logger_test.debug(f"Calling save_to_cache for: {cache_path_s3}")
#             reopened_s3 = save_to_cache(ds_with_nans.copy(deep=True), cache_path_s3, perform_nan_interpolation=False, interpolation_verbosity=1)

#             logger_test.debug(f"Checking existence of {cache_path_s3} and non-existence of {raw_cache_path_s3}")
#             assert cache_path_s3.exists(), f"Main cache file {cache_path_s3} not created for S3."
#             assert not raw_cache_path_s3.exists(), f"Raw cache file {raw_cache_path_s3} WAS created for S3 (should not exist)."

#             with xr.open_dataset(cache_path_s3, engine='h5netcdf') as ds_final_s3:
#                  assert ds_final_s3["temperature"].isnull().sum().item() == num_nans_temp_start, f"Expected {num_nans_temp_start} NaNs in imputation-off cache S3 ({cache_path_s3})"
#                  assert ds_final_s3["temperature"].dtype == np.float32, f"Final imputation-off cache S3 not float32 ({cache_path_s3})"

#             logger_test.info("--> save_to_cache (Imputation OFF, NaNs PRESENT) test PASSED.")
#             reopened_s3.close()
#         except Exception as e:
#             logger_test.error(f"--> save_to_cache (Imputation OFF, NaNs PRESENT) test FAILED: {e}", exc_info=True)


#         logger_test.info(f"\n--- Standalone test finished. Temporary files were in {temp_dir_path} ---")
#         # Temporary directory is automatically cleaned up by the 'with' statement exit
#         input(f"PAUSING SCRIPT. Files are in {temp_dir_path}. Press Enter to cleanup temporary files and exit...")

#     # === Shutdown Dask client/cluster ===
#     if dask_client_for_test:
#         try:
#             dask_client_for_test.close()
#             if dask_cluster_for_test:
#                 dask_cluster_for_test.close()
#             logger_test.info("Dask client and cluster for test shut down.")
#         except Exception as e:
#             logger_test.warning(f"Error shutting down Dask client/cluster for test: {e}")