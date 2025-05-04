import logging
import multiprocessing
import os
import time
import warnings
from datetime import datetime
from functools import partial
from math import ceil
from multiprocessing import shared_memory
from pathlib import Path
import glob # Added import (as requested, though not used by corrected function)

from dask.distributed import Client, LocalCluster

import geopandas as gpd
import numpy as np
import pandas as pd
import psutil
import xarray as xr
from data_processing.file_paths import file_paths
from data_processing.dataset_utils import validate_dataset_format
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource
from rich.progress import (
    Progress,
    BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
)
from typing import Tuple, Dict # Added Dict
import rioxarray # Ensure rioxarray is imported for CRS handling

# # Assuming file_paths and validate_dataset_format are correctly importable
# # --- Mocking file_paths and validate_dataset_format for demonstration and testing [uncomment line 33 to 67 for testing]
# class file_paths:
#     def __init__(self, cat_id):
#         base_dir = Path("./data") / str(cat_id)
#         self.forcings_dir = base_dir / "forcings"
#         self.geopackage_path = base_dir / "hydrofabric.gpkg"
#         self.cached_nc_file = self.forcings_dir / "cached_source.nc"
#         self.forcings_dir.mkdir(parents=True, exist_ok=True)
#         if not self.geopackage_path.exists():
#              print(f"WARNING: Dummy geopackage created at {self.geopackage_path}")
#              # --- CORRECTED GEOMETRY DEFINITION ---
#              dummy_gdf = gpd.GeoDataFrame(
#                 {'divide_id': [1, 2]},
#                 geometry=gpd.GeoSeries.from_wkt([  # Create ONE GeoSeries with both WKT strings
#                     'POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))',
#                     'POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))'
#                 ]),
#                 crs="EPSG:4326"
#              )
#              # --- END CORRECTION ---
#              try:
#                  dummy_gdf.to_file(self.geopackage_path, layer='divides', driver='GPKG')
#              except Exception as e:
#                  # Add more specific error logging if writing fails
#                  logger.error(f"Failed to write dummy geopackage: {e}", exc_info=True)
#                  raise # Re-raise the exception to stop execution clearly

# def validate_dataset_format(dataset):
#     print("Dataset format validation called (mocked).")
#     if "time" not in dataset.dims: raise ValueError("Dataset missing 'time' dimension")
#     if not any(d in dataset.dims for d in ['x', 'longitude']): raise ValueError("Dataset missing 'x' or 'longitude' dimension")
#     if not any(d in dataset.dims for d in ['y', 'latitude']): raise ValueError("Dataset missing 'y' or 'latitude' dimension")
#     if dataset.rio.crs is None:
#         print("Warning: Assigning dummy CRS EPSG:4326 to dataset for testing.")
#         dataset.rio.write_crs("EPSG:4326", inplace=True)
#     pass
# # --- End Mocking ---


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

warnings.filterwarnings("ignore", message="'DataFrame.swapaxes' is deprecated", category=FutureWarning)
warnings.filterwarnings("ignore", message="'GeoDataFrame.swapaxes' is deprecated", category=FutureWarning)
warnings.filterwarnings("ignore", message="Unable to find spatial coordinates", category=UserWarning)


# ========================================================================
# >>>>> NEW FUNCTION: Adapted from forcing_data_correction <<<<<
# ========================================================================
def correct_input_dataset(dataset: xr.Dataset, verbosity: int = 0) -> xr.Dataset:
    '''
    Corrects the input forcing dataset in memory *before* processing.
    1. Ensures 'APCP_surface' units attribute is 'mm h^-1' (consistent with write_outputs).
    2. Interpolates NaN values in all time-dependent data variables using nearest neighbor.

    Parameters
    ----------
    dataset : xr.Dataset
        The input gridded forcing dataset.
    verbosity : int, optional
        Verbosity level. >= 1 prints basic messages, >= 2 prints NaN details. Default is 0 (silent).

    Returns
    -------
    xr.Dataset
        A new dataset instance with corrections applied.
    '''
    if verbosity >= 1:
        logger.info("Applying input dataset corrections (NaN interpolation, unit standardization)...")

    # Work on a copy to avoid modifying the original object in place
    corrected_ds = dataset.copy(deep=True)

    # 1. Correct APCP_surface units (if variable exists)
    if 'APCP_surface' in corrected_ds.data_vars:
        # Target unit string consistent with later script parts
        target_units_str = 'mm h^-1'
        current_units = corrected_ds['APCP_surface'].attrs.get('units', '')

        # Check if units need changing (case-insensitive comparison for robustness)
        if not current_units or current_units.lower().replace(" ","") != target_units_str.lower().replace(" ",""):
            if verbosity >= 1:
                logger.info(f"Standardizing 'APCP_surface' units from '{current_units}' to '{target_units_str}'.")
            corrected_ds['APCP_surface'].attrs['units'] = target_units_str
             # Add a note if the original unit suggests a value conversion might be needed elsewhere
            if any(u in current_units.lower() for u in ['/s', 's^-1']):
                 logger.warning(f"Original APCP_surface units ('{current_units}') look like mm/s. "
                                f"Only the unit attribute was changed to '{target_units_str}'. "
                                "Ensure values were already in mm/hr or handled previously.")
        elif verbosity >= 2:
            logger.debug(f"'APCP_surface' units already match target '{target_units_str}'.")
    elif verbosity >= 2:
         logger.debug("'APCP_surface' variable not found in input dataset. Skipping unit standardization.")

    # 2. Interpolate NaNs
    if verbosity >= 1:
        logger.info("Checking for and interpolating NaNs in time-dependent variables...")

    nan_found_in_any = False
    for name in list(corrected_ds.data_vars): # Iterate over variable names
        # Check if variable has a 'time' dimension and is numeric
        if 'time' in corrected_ds[name].dims and np.issubdtype(corrected_ds[name].dtype, np.number):
            if corrected_ds[name].isnull().any():
                nan_found_in_any = True
                if verbosity >= 1:
                    logger.info(f"NaNs found in '{name}'. Interpolating using 'nearest' along time dimension...")
                # Interpolate along 'time' dim using 'nearest'.
                # fill_value="extrapolate" fills NaNs at the start/end of the series.
                corrected_ds[name] = corrected_ds[name].interpolate_na(
                    dim='time',
                    method='nearest',
                    fill_value="extrapolate"
                )
                # Optional verification step
                if corrected_ds[name].isnull().any() and verbosity >= 1:
                     logger.warning(f"NaNs still present in '{name}' after interpolation. Check input or interpolation limits.")
            elif verbosity >= 2:
                logger.debug(f"No NaNs found in time-dependent variable '{name}'.")
        elif verbosity >= 2:
             if 'time' not in corrected_ds[name].dims:
                 logger.debug(f"Skipping NaN check for '{name}': No 'time' dimension.")
             else:
                 logger.debug(f"Skipping NaN check for '{name}': Not a numeric data type ({corrected_ds[name].dtype}).")


    if not nan_found_in_any and verbosity >= 1:
         logger.info("No NaNs found needing interpolation in any time-dependent data variables.")

    if verbosity >=1:
        logger.info("Input dataset correction finished.")

    return corrected_ds

# ========================================================================
# >>>>> END OF NEW FUNCTION <<<<<
# ========================================================================


# --- Original Functions (No changes below unless marked) ---

def weighted_sum_of_cells(flat_raster: np.ndarray,
                          cell_ids: np.ndarray,
                          factors: np.ndarray) -> np.ndarray:
    '''
    Take an average of each forcing variable in a catchment. Create an output
    array initialized with zeros, and then sum up the forcing variable and
    divide by the sum of the cell weights to get an averaged forcing variable
    for the entire catchment.

    Parameters
    ----------
    flat_raster : np.ndarray
        An array of dimensions (time, x*y) containing forcing variable values
        in each cell. Each element in the array corresponds to a cell ID.
    cell_ids : np.ndarray
        A list of the raster cell IDs that intersect the study catchment.
    factors : np.ndarray
        A list of the weights (coverages) of each cell in cell_ids.

    Returns
    -------
    np.ndarray
        An one-dimensional array, where each element corresponds to a timestep.
        Each element contains the averaged forcing value for the whole catchment
        over one timestep.
    '''
    result = np.zeros(flat_raster.shape[0])
    # Ensure factors is broadcastable: shape (1, n_cells)
    factors_broadcast = factors[np.newaxis, :]
    # Perform weighted sum: sum(data[time, cells] * weights[cells]) along cell axis
    result = np.sum(flat_raster[:, cell_ids] * factors_broadcast, axis=1)
    sum_of_weights = np.sum(factors)
    # Avoid division by zero if sum_of_weights is zero (e.g., no overlap)
    if sum_of_weights > 1e-9: # Use a small threshold for floating point comparison
        result /= sum_of_weights
    else:
        # Handle cases with zero weight sum - perhaps set to NaN or 0
        result[:] = np.nan # Or set to 0: result[:] = 0
        # Logging this might be too verbose if it happens often
        # logger.warning(f"Sum of weights is near zero ({sum_of_weights}). Averaged result set to NaN.")
    return result


def get_cell_weights(raster: xr.Dataset,
                     gdf: gpd.GeoDataFrame,
                     wkt: str) -> pd.DataFrame:
    '''
    Get the cell weights (coverage) for each cell in a divide. Coverage is
    defined as the fraction (a float in [0,1]) of a raster cell that overlaps
    with the polygon in the passed gdf.

    Parameters
    ----------
    raster : xr.Dataset
        One timestep of a gridded forcings dataset. Should have spatial dims (x, y)
        and CRS information interpretable by rioxarray.
    gdf : gpd.GeoDataFrame
        A GeoDataFrame with a polygon feature and a 'divide_id' column.
    wkt : str
        Well-known text (WKT) representation of gdf's coordinate reference
        system (CRS)

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by divide_id that contains information about coverage
        for each raster cell in gridded forcing file. Columns: 'cell_id', 'coverage'.
    '''
    # Use rioxarray to get transform and dimensions if possible
    try:
        grid_mapping_name = raster.rio.grid_mapping_name()
        if grid_mapping_name:
             raster_var = raster[grid_mapping_name]
        else:
             data_vars = list(raster.data_vars)
             if not data_vars: raise ValueError("Raster dataset has no data variables.")
             raster_var = raster[data_vars[0]] # Fallback to first data var
    except Exception: # Broader catch if rio attributes fail
         logger.warning("rioxarray extensions not fully available/grid mapping not found. Falling back.")
         data_vars = list(raster.data_vars)
         if not data_vars: raise ValueError("Raster dataset has no data variables.")
         raster_var = raster[data_vars[0]]

    if "divide_id" not in gdf.columns:
        raise ValueError("Input GeoDataFrame must have a 'divide_id' column.")

    try:
         output = exact_extract(
            raster_var, gdf, ["cell_id", "coverage"],
            include_cols=["divide_id"], output="pandas",
         )
    except Exception as e:
        logger.error(f"exact_extract failed: {e}. Check CRS alignment and geometry validity.")
        # Fallback to NumPyRasterSource might be needed here if direct xarray fails often
        raise # Re-raise the error for now

    if output.empty and not gdf.empty:
        logger.warning("No overlapping cells found between raster and any features in GeoDataFrame.")
        return pd.DataFrame(columns=["cell_id", "coverage", "divide_id"]).set_index("divide_id")
    elif output.empty:
         return pd.DataFrame(columns=["cell_id", "coverage", "divide_id"]).set_index("divide_id")

    return output.set_index("divide_id")


def add_APCP_SURFACE_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    '''Convert precipitation rate (mm/s) to APCP_surface (mm/hr).'''
    if "precip_rate" in dataset.data_vars:
        logger.info("Converting precip_rate (mm/s) to APCP_surface (mm/hr) in dataset.")
        dataset["APCP_surface"] = dataset["precip_rate"] * 3600
        dataset["APCP_surface"].attrs["units"] = "mm h^-1" # Consistent unit format
        dataset["APCP_surface"].attrs["source_note"] = "Converted from precip_rate (mm/s) by multiplying by 3600"
        dataset["APCP_surface"].attrs["long_name"] = "Surface Precipitation Rate"
    else:
        # This might be okay if APCP_surface was already present and corrected earlier
        logger.debug("Variable 'precip_rate' not found. Cannot create 'APCP_surface'.")
    return dataset


def add_precip_rate_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    '''Convert APCP_surface (mm/hr) to precipitation rate (mm/s).'''
    if "APCP_surface" in dataset.data_vars:
        logger.info("Converting APCP_surface (mm/hr) to precip_rate (mm/s) in dataset.")
        dataset["precip_rate"] = dataset["APCP_surface"] / 3600
        dataset["precip_rate"].attrs["units"] = "mm s^-1"
        dataset["precip_rate"].attrs["source_note"] = "Converted from APCP_surface (mm/hr) by dividing by 3600"
        dataset["precip_rate"].attrs["long_name"] = "Precipitation Rate"
    else:
        logger.debug("Variable 'APCP_surface' not found. Cannot create 'precip_rate'.")
    return dataset


def get_index_chunks(data: xr.DataArray) -> list[tuple[int, int]]:
    '''
    Take a DataArray and calculate the start and end index for each chunk based
    on the available memory. (No changes from original)
    '''
    array_memory_usage = data.nbytes
    available_memory = psutil.virtual_memory().available
    free_memory_limit = min(available_memory * 0.8, 20 * 1024**3)
    # logger.info(f"Total array size: {array_memory_usage / 1024**2:.2f} MB. Memory limit per chunk: {free_memory_limit / 1024**2:.2f} MB.")
    num_chunks = max(1, ceil(array_memory_usage / free_memory_limit))
    # logger.info(f"Splitting data into {num_chunks} time chunks for processing.")
    max_index = data.shape[0]
    stride = max(1, max_index // num_chunks)
    index_chunks = []
    for start in range(0, max_index, stride):
        end = min(start + stride, max_index)
        if start < end: index_chunks.append((start, end))
    if index_chunks and index_chunks[-1][1] < max_index:
         last_start = index_chunks[-1][0]
         if len(index_chunks) == 1: index_chunks[0] = (index_chunks[0][0], max_index)
         else: index_chunks.append((index_chunks[-1][1], max_index))
    # logger.debug(f"Calculated index chunks: {index_chunks}")
    return index_chunks


def create_shared_memory(lazy_array: xr.DataArray) -> Tuple[
    shared_memory.SharedMemory, tuple, np.dtype
]:
    '''
    Create a shared memory object and load data into it chunk by chunk.
    (No changes from original, assuming float32 target is okay)
    '''
    target_dtype = np.float32
    if lazy_array.dtype != target_dtype:
        # logger.warning(f"Input array dtype is {lazy_array.dtype}. Will convert to {target_dtype} for shared memory.")
        nbytes_estimate = np.prod(lazy_array.shape) * np.dtype(target_dtype).itemsize
    else:
        nbytes_estimate = lazy_array.nbytes

    # logger.debug(f"Attempting to create shared memory of size {nbytes_estimate / 1024**2:.2f} MB.")
    shm = shared_memory.SharedMemory(create=True, size=nbytes_estimate) # Add error handling?

    shared_array = np.ndarray(lazy_array.shape, dtype=target_dtype, buffer=shm.buf)

    # logger.debug("Loading data into shared memory...")
    load_timer = time.time()
    load_chunk_memory_limit = 5 * 1024**3 # Limit load chunks to 5GB
    num_load_chunks = max(1, ceil(lazy_array.nbytes / load_chunk_memory_limit))
    load_stride = max(1, lazy_array.shape[0] // num_load_chunks)
    load_indices = []
    for start in range(0, lazy_array.shape[0], load_stride):
         end = min(start + load_stride, lazy_array.shape[0])
         if start < end: load_indices.append((start, end))
    if load_indices and load_indices[-1][1] < lazy_array.shape[0]:
        load_indices.append((load_indices[-1][1], lazy_array.shape[0]))

    for start, end in load_indices:
        # logger.debug(f"Loading time slice {start}-{end} into shared memory...")
        data_slice = lazy_array[start:end].values.astype(target_dtype)
        shared_array[start:end] = data_slice
        del data_slice
    # logger.debug(f"Data loaded into shared memory in {time.time() - load_timer:.2f} seconds.")

    time_dim, *spatial_dims = shared_array.shape
    shared_array = shared_array.reshape(time_dim, -1)
    return shm, shared_array.shape, shared_array.dtype


def process_chunk_shared(variable: str, times: np.ndarray, shm_name: str,
                         shape: tuple, dtype: np.dtype, chunk: pd.DataFrame
                         ) -> xr.DataArray:
    '''
    Process a chunk of catchments using data from a SharedMemory block.
    (No changes from original)
    '''
    existing_shm = None; raster = None
    try:
        existing_shm = shared_memory.SharedMemory(name=shm_name)
        raster = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)
        results = []
        catchment_ids_in_chunk = chunk.index.unique()
        # logger.debug(f"Processing {len(catchment_ids_in_chunk)} catchments in worker {os.getpid()} using shm {shm_name}")

        for catchment_id in catchment_ids_in_chunk:
            catchment_data = chunk.loc[[catchment_id]]
            if catchment_data.empty: continue
            cell_ids = catchment_data["cell_id"].to_numpy(dtype=int)
            weights = catchment_data["coverage"].to_numpy(dtype=np.float32)

            if np.any(cell_ids >= shape[1]) or np.any(cell_ids < 0):
                 logger.error(f"Catchment {catchment_id}: Cell IDs out of bounds. Skipping.")
                 continue
            if len(cell_ids) == 0 or len(weights) == 0: continue

            mean_at_timesteps = weighted_sum_of_cells(raster, cell_ids, weights)
            temp_da = xr.DataArray(mean_at_timesteps, dims=["time"], coords={"time": times})
            temp_da = temp_da.assign_coords(catchment=catchment_id)
            results.append(temp_da)

        if not results: return None
        concatenated_results = xr.concat(results, dim="catchment")
        # logger.debug(f"Worker {os.getpid()} finished processing chunk for shm {shm_name}.")
        return concatenated_results
    except FileNotFoundError: logger.error(f"Shm block '{shm_name}' not found in worker {os.getpid()}."); return None
    except Exception as e: logger.exception(f"Error in worker {os.getpid()} shm {shm_name}: {e}"); return None
    finally:
        del raster
        if existing_shm: existing_shm.close()


def get_cell_weights_parallel(gdf: gpd.GeoDataFrame,
                              input_forcings: xr.Dataset,
                              num_partitions: int) -> pd.DataFrame:
    '''
    Execute get_cell_weights with multiprocessing, chunking the GeoDataFrame.
    (No changes from original)
    '''
    if "divide_id" not in gdf.columns: raise ValueError("GDF needs 'divide_id' column.")
    if gdf.empty: return pd.DataFrame(columns=["cell_id", "coverage"]).set_index("divide_id")

    raster_crs = input_forcings.rio.crs
    if raster_crs and gdf.crs != raster_crs:
         logger.info(f"Reprojecting GeoDataFrame from {gdf.crs} to {raster_crs}")
         gdf = gdf.to_crs(raster_crs)
    wkt = gdf.crs.to_wkt()

    actual_num_partitions = max(1, min(num_partitions if num_partitions > 0 else 1, len(gdf)))
    gdf_chunks = np.array_split(gdf, actual_num_partitions)
    # logger.info(f"Calculating cell weights in parallel using {actual_num_partitions} partitions.")

    one_timestep = input_forcings.isel(time=0).compute()
    results = []
    with multiprocessing.Pool(processes=actual_num_partitions) as pool:
        args = [(one_timestep, gdf_chunk, wkt) for gdf_chunk in gdf_chunks if not gdf_chunk.empty]
        if not args: return pd.DataFrame(columns=["cell_id", "coverage"]).set_index("divide_id")
        try:
            chunk_results = pool.starmap(get_cell_weights, args)
            results = [res for res in chunk_results if res is not None and not res.empty]
        except Exception as e:
            logger.error(f"Error during parallel cell weight calculation: {e}")
            results = [res for res in pool.starmap(get_cell_weights, args) if res is not None and not res.empty]

    if not results: raise RuntimeError("Failed to calculate any cell weights.")
    all_catchment_weights = pd.concat(results)
    # logger.info(f"Computed cell weights for {all_catchment_weights.index.nunique()} unique catchments.")
    return all_catchment_weights


def get_units(dataset: xr.Dataset) -> dict:
    '''
    Return dictionary of units for each variable in dataset.
    (No changes from original)
    '''
    units = {}
    for var_name in dataset.data_vars:
        try:
            unit_attr = dataset[var_name].attrs.get("units")
            if unit_attr: units[var_name] = unit_attr
            # else: logger.warning(f"Var '{var_name}' has no 'units' attribute.") # Reduce verbosity
        except Exception as e:
            logger.warning(f"Could not get units for var '{var_name}': {e}")
            units[var_name] = "error"
    return units


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, gridded_data: xr.Dataset, forcings_dir: Path
) -> None:
    '''
    Compute zonal statistics in parallel. (No changes from original)
    '''
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    max_cpus = multiprocessing.cpu_count()
    num_partitions = max(1, min(max_cpus - 1, len(gdf)))

    catchments_weights = get_cell_weights_parallel(gdf, gridded_data, num_partitions)
    if catchments_weights.empty: raise RuntimeError("Cell weight calculation returned empty.")

    missing_catchments = set(gdf['divide_id']) - set(catchments_weights.index.unique())
    if missing_catchments: logger.warning(f"Divide_ids w/ no overlapping cells: {missing_catchments}")

    units = get_units(gridded_data)
    valid_catchment_ids = catchments_weights.index.unique()
    cat_chunks = np.array_split(catchments_weights.loc[valid_catchment_ids], num_partitions)
    cat_chunks = [chunk for chunk in cat_chunks if not chunk.empty]
    if not cat_chunks: raise RuntimeError("No valid catchment chunks to process.")
    actual_num_partitions = len(cat_chunks)

    progress = Progress(
        TextColumn("[progress.description]{task.description}"), BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%", TextColumn("{task.completed}/{task.total}"),
        "•", TextColumn("Elapsed:"), TimeElapsedColumn(),
        TextColumn("Remaining:"), TimeRemainingColumn(), transient=False # Keep progress visible
    )

    variables_to_process = list(gridded_data.data_vars)
    temp_var_files = {} # Track final temp files per variable

    with progress:
        variable_task = progress.add_task("[cyan]Processing variables...", total=len(variables_to_process))

        for variable in variables_to_process:
            progress.update(variable_task, description=f"Processing [bold]{variable}[/]", advance=0)
            var_timer_start = time.time()
            logger.info(f"--- Processing variable: {variable} ---")
            variable_da = gridded_data[variable]
            time_chunks_indices = get_index_chunks(variable_da)
            if not time_chunks_indices: logger.warning(f"No time chunks for {variable}. Skipping."); continue

            time_chunk_task = progress.add_task(f"[purple] {variable} time chunks", total=len(time_chunks_indices))
            temp_nc_files_for_var = []
            shm = None

            for i, (start_time_idx, end_time_idx) in enumerate(time_chunks_indices):
                progress.update(time_chunk_task, advance=1, description=f"[purple]{variable} chunk {i+1}/{len(time_chunks_indices)}")
                # logger.info(f"Processing time chunk {i+1}/{len(time_chunks_indices)} for {variable}")
                data_chunk = variable_da.isel(time=slice(start_time_idx, end_time_idx))
                try:
                    shm, shm_shape, shm_dtype = create_shared_memory(data_chunk)
                    times_in_chunk = data_chunk.time.values
                    partial_process_chunk = partial(process_chunk_shared, variable, times_in_chunk, shm.name, shm_shape, shm_dtype)
                    # logger.debug(f"Mapping catchment chunks to workers for time chunk {i+1}")
                    with multiprocessing.Pool(processes=actual_num_partitions) as pool:
                         chunk_results = pool.map(partial_process_chunk, cat_chunks)
                    valid_results = [res for res in chunk_results if res is not None]; del chunk_results
                    if not valid_results: logger.error(f"No valid results for time chunk {i+1} of {variable}."); continue # Maybe raise error?

                    # logger.debug(f"Concatenating {len(valid_results)} results for time chunk {i+1}")
                    concatenated_da = xr.concat(valid_results, dim="catchment"); del valid_results
                    temp_filename = forcings_dir / "temp" / f"{variable}_timechunk_{i}.nc"
                    # logger.info(f"Saving intermediate result: {temp_filename.name}")
                    try:
                         concatenated_da.to_dataset(name=variable).to_netcdf(temp_filename, engine="netcdf4")
                         temp_nc_files_for_var.append(temp_filename)
                    except Exception as e: logger.error(f"Failed to write temp file {temp_filename}: {e}")
                    del concatenated_da
                except Exception as e: logger.exception(f"Error processing time chunk {i+1} for {variable}: {e}")
                finally:
                    if shm:
                        try: shm.close(); shm.unlink()
                        except Exception: pass # Ignore cleanup errors
                    del data_chunk
                # End of time chunk loop
            progress.remove_task(time_chunk_task)

            if not temp_nc_files_for_var:
                logger.warning(f"No temp files created for {variable}. Skipping merge.")
                progress.update(variable_task, advance=1); continue

            # logger.info(f"Merging {len(temp_nc_files_for_var)} time chunks for {variable}...")
            try:
                datasets_to_merge = [xr.open_dataset(f, chunks={'time': -1, 'catchment': 'auto'}) for f in temp_nc_files_for_var] # Chunking strategy for merge
                merged_variable_ds = xr.concat(datasets_to_merge, dim="time", coords="minimal", data_vars="minimal", compat="override", join="override")
                merged_variable_ds = merged_variable_ds.sortby('time')
                variable_final_temp_path = forcings_dir / "temp" / f"{variable}.nc"
                # logger.info(f"Saving merged dataset for {variable} to {variable_final_temp_path.name}")
                merged_variable_ds.compute().to_netcdf(variable_final_temp_path, engine="netcdf4")
                temp_var_files[variable] = variable_final_temp_path # Track final temp file
                for ds in datasets_to_merge: ds.close()
                merged_variable_ds.close(); del datasets_to_merge, merged_variable_ds
                # logger.debug(f"Cleaning up {len(temp_nc_files_for_var)} time chunk files for {variable}.")
                for file in temp_nc_files_for_var:
                    try: file.unlink()
                    except OSError as e: logger.warning(f"Could not delete temp file {file}: {e}")
            except Exception as e: logger.error(f"Failed to merge/save chunks for {variable}: {e}")

            logger.info(f"Variable {variable} processed in {time.time() - var_timer_start:.2f} seconds.")
            progress.update(variable_task, advance=1)
        # End of variable loop

    logger.info(f"Zonal stats computed in {time.time() - timer_start:.2f} seconds")
    write_outputs(forcings_dir, units, temp_var_files) # Pass tracked files


def write_outputs(forcings_dir: Path, units: dict, temp_var_files: Dict[str, Path]) -> None:
    '''
    Merge individual variable NetCDF files, perform final formatting,
    and save the consolidated forcings file.
    (No file correction call here anymore).

    Parameters
    ----------
    forcings_dir : Path
        Path to the directory containing the temporary variable files
        (in forcings_dir / "temp").
    units : dict
        Dictionary mapping variable names to their units (from original input).
    temp_var_files : Dict[str, Path]
        Dictionary mapping variable names to their final temporary file paths.
    '''
    logger.info("Writing final output NetCDF file.")
    temp_forcings_dir = forcings_dir / "temp"
    final_output_path = forcings_dir / "forcings.nc"

    temp_files = list(temp_var_files.values()) # Use tracked files
    if not temp_files:
        logger.error(f"No temporary variable files provided to merge. Cannot write final output.")
        return
    logger.info(f"Merging {len(temp_files)} final variable files.")

    final_ds = None; results_datasets = []
    client = None; cluster = None # Keep track of dask client/cluster

    try:
        try: client = Client.current(); logger.info("Using existing Dask client.")
        except ValueError:
            logger.info("Starting local Dask cluster."); cluster = LocalCluster(); client = Client(cluster)
            logger.info(f"Dask dashboard: {client.dashboard_link}")

        logger.info("Opening final temporary variable files with dask...")
        results_datasets = [xr.open_dataset(file, chunks="auto") for file in temp_files]
        logger.info("Merging datasets...")
        final_ds = xr.merge(results_datasets, compat='override', join='override')

        logger.info("Applying units and performing final conversions...")
        for var in final_ds.data_vars:
            if var in units: final_ds[var].attrs["units"] = units[var]
            else: logger.warning(f"'{var}' in final dataset has no original units info."); final_ds[var].attrs["units"] = "unknown"

        # Precipitation unit handling (may seem redundant if pre-correction worked, but acts as safeguard/standardization)
        if "APCP_surface" in final_ds.data_vars and "precip_rate" in final_ds.data_vars:
            logger.warning("Both 'APCP_surface' and 'precip_rate' present. Dropping 'precip_rate'.")
            final_ds = final_ds.drop_vars("precip_rate")
        elif "precip_rate" in final_ds.data_vars and "APCP_surface" not in final_ds.data_vars:
             final_ds = add_APCP_SURFACE_to_dataset(final_ds) # Create APCP from rate
             if "precip_rate" in final_ds.data_vars: final_ds = final_ds.drop_vars("precip_rate")
        elif "APCP_surface" in final_ds.data_vars and "precip_rate" not in final_ds.data_vars:
             # Ensure units are as expected ('mm h^-1') even if pre-correction ran
             apcp_units = final_ds["APCP_surface"].attrs.get("units", "")
             if apcp_units.lower().replace(" ","") != "mm h^-1":
                 logger.warning(f"APCP_surface units are '{apcp_units}'. Forcing to 'mm h^-1'.")
                 final_ds["APCP_surface"].attrs["units"] = "mm h^-1"
        # else: logger.warning("Neither 'APCP_surface' nor 'precip_rate' found.") # Less critical now

        logger.info("Converting data variables to float32...")
        for var in final_ds.data_vars:
            if final_ds[var].dtype != np.float32:
                final_ds[var] = final_ds[var].astype(np.float32)

        logger.info("Reformatting NetCDF structure for legacy format...")
        if "catchment" in final_ds.coords:
             final_ds["ids"] = final_ds["catchment"].astype(str)
             final_ds = final_ds.rename_dims({"catchment": "catchment-id"})
             final_ds = final_ds.drop_vars(["catchment"])
        else: raise ValueError("Missing 'catchment' coordinate for final formatting.")

        if "time" in final_ds.coords:
            try:
                 time_coord_np = final_ds.time.values
                 if not np.issubdtype(time_coord_np.dtype, np.datetime64):
                      final_ds['time'] = final_ds.indexes['time'].to_datetimeindex()
                      time_coord_np = final_ds.time.values
                 time_unix_seconds = (time_coord_np - np.datetime64('1970-01-01T00:00:00')) / np.timedelta64(1, 's')
                 time_array_int32 = time_unix_seconds.astype(np.int32)
            except Exception as e: raise ValueError("Time coordinate conversion failed.") from e

            num_catchments = final_ds.dims["catchment-id"]
            final_ds["Time"] = (("catchment-id", "time"), [time_array_int32] * num_catchments)
            final_ds["Time"].attrs["units"] = "seconds since 1970-01-01 00:00:00"
            final_ds["Time"].attrs["long_name"] = "Time axis"; final_ds["Time"].attrs["standard_name"] = "time"
            final_ds = final_ds.drop_vars(["time"])
        else: raise ValueError("Missing 'time' coordinate for final formatting.")

        logger.info(f"Saving formatted data to: {final_output_path}")
        forcings_dir.mkdir(parents=True, exist_ok=True)
#        encoding = {var: {'zlib': True, 'complevel': 4, '_FillValue': -9999.0} for var in final_ds.data_vars}
        # --- CORRECTED ENCODING DEFINITION ---
        encoding = {} # Start empty
        fill_value_float = -9999.0 # Define fill value for float variables

        # Identify main data variables (excluding coordinates turned into variables like 'ids', 'Time')
        main_data_vars = [v for v, da in final_ds.data_vars.items() if v not in ['ids', 'Time']]

        for var in final_ds.data_vars:
             if var in main_data_vars and np.issubdtype(final_ds[var].dtype, np.number):
                 # Apply compression and fill value only to numeric main data variables
                 encoding[var] = {'zlib': True, 'complevel': 4, '_FillValue': fill_value_float}
             # else: Define encoding for 'ids', 'Time', or other non-numeric vars if needed
             # For 'ids' (string) and 'Time' (int), we typically don't need zlib.
             # We might want a fill value for Time, but None is often acceptable.
             # String variables ('ids') don't use numeric fill values.
             # So, we can often just skip defining encoding for them, or explicitly set defaults:
             # elif var == 'Time':
             #    encoding[var] = {'_FillValue': -9999} # Optional integer fill value
             # elif var == 'ids':
             #    encoding[var] = {'_FillValue': None} # Explicitly no fill value needed

        # If no specific encoding is in the dict for 'ids' or 'Time', xarray/netcdf4 use defaults.
        # --- END CORRECTION ---

        logger.info(f"Saving formatted data to: {final_output_path}")
        forcings_dir.mkdir(parents=True, exist_ok=True)
        # Use the corrected encoding dictionary
        write_job = final_ds.to_netcdf(final_output_path, engine="netcdf4", compute=True, encoding=encoding)
        logger.info("Final forcings file saved successfully.")

    except Exception as e:
        logger.exception(f"An error occurred during the final writing process: {e}")
        return # Exit on error, do not cleanup temp files
    finally:
        logger.debug("Closing open datasets.")
        if final_ds is not None: final_ds.close()
        for ds in results_datasets: ds.close()
        if client and cluster: # Only close if we started it
            try: client.close(); cluster.close(); logger.info("Dask client/cluster closed.")
            except Exception as dask_e: logger.warning(f"Error closing dask: {dask_e}")

    # Clean up temporary files *after* successful write
    logger.info(f"Cleaning up temporary files in {temp_forcings_dir}...")
    try:
        for file in temp_files: # Use the tracked list of final temp files
            if file.exists(): file.unlink()
        # Only remove temp dir if it's empty (safer)
        if temp_forcings_dir.exists() and not any(temp_forcings_dir.iterdir()):
             temp_forcings_dir.rmdir()
        logger.info("Temporary files cleaned up.")
    except OSError as e:
        logger.warning(f"Could not completely clean up {temp_forcings_dir}: {e}")

    # <<< NO CORRECTION CALL NEEDED HERE ANYMORE >>>


def setup_directories(output_folder_name: str) -> file_paths:
    """Sets up output directories and returns paths object. (No changes)"""
    logger.info(f"Setting up directories for output: {output_folder_name}")
    forcing_paths = file_paths(output_folder_name)
    forcing_paths.forcings_dir.parent.mkdir(parents=True, exist_ok=True)
    if forcing_paths.forcings_dir.exists():
        logger.warning(f"Cleaning existing forcing output directory: {forcing_paths.forcings_dir}")
        for item in forcing_paths.forcings_dir.iterdir():
            if item.is_file() and item != forcing_paths.cached_nc_file:
                 if item.name.endswith(".nc"): item.unlink()
            elif item.is_dir() and item.name == "temp":
                 import shutil; shutil.rmtree(item, ignore_errors=True)
    forcing_paths.forcings_dir.mkdir(exist_ok=True)
    (forcing_paths.forcings_dir / "temp").mkdir(exist_ok=True)
    logger.info(f"Forcing output will be in: {forcing_paths.forcings_dir}")
    return forcing_paths


# ========================================================================
# >>>>> MODIFIED FUNCTION: create_forcings <<<<<
# ========================================================================
def create_forcings(dataset: xr.Dataset, output_folder_name: str) -> None:
    """
    Main function to create forcing files from a gridded dataset. Applies
    corrections to the input dataset *before* processing.

    Parameters
    ----------
    dataset : xr.Dataset
        Input gridded dataset (e.g., from NetCDF). Must have time, spatial dimensions,
        and CRS information usable by rioxarray.
    output_folder_name : str
        Name for the output folder structure (e.g., catchment set ID).
    """
    logger.info(f"--- Starting forcing creation for: {output_folder_name} ---")
    # 1. Validate initial dataset format
    try:
        validate_dataset_format(dataset)
    except Exception as e:
         logger.error(f"Input dataset validation failed: {e}")
         return

    # ====================================================================
    # >>>>> INTEGRATION POINT: Correct the input dataset <<<<<
    # ====================================================================
    try:
        # Call the correction function, use verbosity=1 for basic logging
        corrected_dataset = correct_input_dataset(dataset, verbosity=1)
        # Use the corrected dataset from now on
    except Exception as e:
        logger.error(f"Input dataset correction failed: {e}")
        return # Stop if correction fails
    # ====================================================================

    # 2. Setup directories
    try:
        forcing_paths = setup_directories(output_folder_name)
    except Exception as e:
        logger.error(f"Failed to set up directories: {e}")
        return

    # 3. Load catchment boundaries
    try:
        logger.info(f"Loading catchment boundaries from: {forcing_paths.geopackage_path}")
        gdf = gpd.read_file(forcing_paths.geopackage_path, layer="divides")
        if "divide_id" not in gdf.columns:
             raise ValueError("Geopackage 'divides' layer needs 'divide_id' column.")
        logger.info(f"Loaded {len(gdf)} catchment polygons.")
    except Exception as e:
        logger.error(f"Failed to load catchment geometries: {e}")
        return

    # 4. Ensure GeoDataFrame CRS matches the (potentially corrected) dataset CRS
    try:
        # Use the corrected_dataset here
        dataset_crs = corrected_dataset.rio.crs
        if not dataset_crs:
             logger.error("Corrected dataset lacks CRS information. Cannot proceed.")
             return
        if gdf.crs != dataset_crs:
            logger.info(f"Reprojecting catchments from {gdf.crs} to dataset CRS {dataset_crs}")
            gdf = gdf.to_crs(dataset_crs)
    except Exception as e:
        logger.error(f"Error aligning CRS between dataset and geometries: {e}")
        return

    # 5. Compute zonal statistics using the *corrected* dataset
    try:
        # Pass the corrected_dataset to compute_zonal_stats
        compute_zonal_stats(gdf, corrected_dataset, forcing_paths.forcings_dir)
    except Exception as e:
         logger.error(f"Compute_zonal_stats failed: {e}", exc_info=True) # Log traceback

    logger.info(f"--- Forcing creation finished for: {output_folder_name} ---")


# # Example Usage (Optional - for testing) [uncomment this (line 827 to 878) and the mocking class for testing]
# if __name__ == "__main__":
#     print("Running example usage...")
#     logging.getLogger().setLevel(logging.INFO)

#     # 1. Create dummy input dataset with NaNs and potentially inconsistent units
#     times = pd.date_range("2023-01-01", periods=10, freq="h")
#     x_coords = np.arange(0.5, 5.5, 1.0); y_coords = np.arange(0.5, 4.5, 1.0)
#     dummy_apcp_data = np.random.rand(len(times), len(y_coords), len(x_coords)) * 5
#     dummy_apcp_data[1, 1, 1] = np.nan # Add a NaN
#     dummy_temp_data = np.random.rand(len(times), len(y_coords), len(x_coords)) * 30 + 273.15
#     dummy_temp_data[3, 2, 0] = np.nan # Add another NaN

#     dummy_ds = xr.Dataset(
#         {
#             "APCP_surface": (("time", "y", "x"), dummy_apcp_data), # Start with APCP
#             "temperature": (("time", "y", "x"), dummy_temp_data),
#         },
#         coords={"time": times, "y": y_coords, "x": x_coords},
#     )
#     # Assign slightly off units to test correction
#     dummy_ds["APCP_surface"].attrs["units"] = "mm/hr" # Unit to be standardized
#     dummy_ds["temperature"].attrs["units"] = "K"
#     dummy_ds.attrs["description"] = "Dummy dataset for testing pre-correction"
#     dummy_ds = dummy_ds.rio.write_crs("EPSG:4326") # Matches dummy hydrofabric
#     dummy_ds = dummy_ds.rio.set_spatial_dims(x_dim="x", y_dim="y")

#     # 2. Define output folder name
#     output_name = "test_catchment_set_precorrected"

#     # 3. Run the forcing creation process
#     try:
#         create_forcings(dummy_ds, output_name)
#         final_file = Path("./data") / output_name / "forcings" / "forcings.nc"
#         if final_file.exists():
#              print(f"\nSuccessfully created final output: {final_file}")
#              # Check contents of final file for corrected units / no NaNs
#              with xr.open_dataset(final_file) as ds_final:
#                  print("\nFinal dataset info:")
#                  print(ds_final)
#                  if 'APCP_surface' in ds_final:
#                       print(f"Final APCP units: {ds_final['APCP_surface'].attrs.get('units')}")
#                  nan_check = {v: ds_final[v].isnull().any().item() for v in ds_final.data_vars if v not in ['ids','Time']}
#                  print(f"NaNs present in final data vars: {nan_check}")
#         else: print(f"\nError: Final output file missing: {final_file}")
#         # Note: No "_corrected.nc" file is expected with this approach
#         corrected_file_path = Path("./data") / output_name / "forcings" / "forcings_corrected.nc"
#         if corrected_file_path.exists():
#              print(f"Warning: Unexpected corrected file found: {corrected_file_path}")

#     except Exception as main_e:
#         print(f"\nAn error occurred during the example run: {main_e}")
#         import traceback; traceback.print_exc()
