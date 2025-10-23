import json
import logging
import multiprocessing
import os
import time
import warnings
from functools import partial
from math import ceil
from multiprocessing import shared_memory
from pathlib import Path
from typing import List, Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
import psutil
import xarray as xr
import datetime
from data_processing.dask_utils import no_cluster, use_cluster
from data_processing.dataset_utils import validate_dataset_format
from data_processing.file_paths import FilePaths
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from xarray.core.types import InterpOptions

logger = logging.getLogger(__name__)
# Suppress the specific warning from numpy to keep the cli output clean
warnings.filterwarnings(
    "ignore", message="'DataFrame.swapaxes' is deprecated", category=FutureWarning
)
warnings.filterwarnings(
    "ignore", message="'GeoDataFrame.swapaxes' is deprecated", category=FutureWarning
)


def weighted_sum_of_cells(
    flat_raster: np.ndarray, cell_ids: np.ndarray, factors: np.ndarray
) -> np.ndarray:
    """
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
    """
    # early exit for divide by zero
    if np.all(factors == 0):
        return np.zeros(flat_raster.shape[0])

    selected_cells = flat_raster[:, cell_ids]
    has_nan = np.isnan(selected_cells).any(axis=1)
    result = np.sum(flat_raster[:, cell_ids] * factors, axis=1)
    sum_of_weights = np.sum(factors)
    result /= sum_of_weights
    result[has_nan] = np.nan
    return result


def get_cell_weights(raster: xr.Dataset, gdf: gpd.GeoDataFrame, wkt: str) -> pd.DataFrame:
    """
    Get the cell weights (coverage) for each cell in a divide. Coverage is
    defined as the fraction (a float in [0,1]) of a raster cell that overlaps
    with the polygon in the passed gdf.

    Parameters
    ----------
    raster : xr.Dataset
        One timestep of a gridded forcings dataset.
    gdf : gpd.GeoDataFrame
        A GeoDataFrame with a polygon feature.
    wkt : str
        Well-known text (WKT) representation of gdf's coordinate reference
        system (CRS)

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by divide_id that contains information about coverage
        for each raster cell in gridded forcing file.
    """
    xmin = min(raster.x)
    xmax = max(raster.x)
    ymin = min(raster.y)
    ymax = max(raster.y)
    data_vars = list(raster.data_vars)
    rastersource = NumPyRasterSource(
        raster[data_vars[0]], srs_wkt=wkt, xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax
    )
    output: pd.DataFrame = exact_extract(
        rastersource,
        gdf,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )  # type: ignore
    return output.set_index("divide_id")


def add_APCP_SURFACE_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    """Convert precipitation value to correct units."""
    # precip_rate is mm/s
    # cfe says input atmosphere_water__liquid_equivalent_precipitation_rate is mm/h
    # nom says prcpnonc input is mm/s
    # technically should be kg/m^2/s at 1kg = 1l it equates to mm/s
    # nom says qinsur output is m/s, hopefully qinsur is converted to mm/h by ngen
    dataset["APCP_surface"] = dataset["precip_rate"] * 3600
    dataset["APCP_surface"].attrs["units"] = "mm h^-1"  # ^-1 notation copied from source data
    dataset["APCP_surface"].attrs["source_note"] = (
        "This is just the precip_rate variable converted to mm/h by multiplying by 3600"
    )
    return dataset


def add_precip_rate_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    # the inverse of the function above
    dataset["precip_rate"] = dataset["APCP_surface"] / 3600
    dataset["precip_rate"].attrs["units"] = "mm s^-1"
    dataset["precip_rate"].attrs["source_note"] = (
        "This is just the APCP_surface variable converted to mm/s by dividing by 3600"
    )
    return dataset
  
def add_pet_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    # used for dHBV2
    SOLAR_CONSTANT = 0.0820
    tmp1 = (24.0 * 60.0) / np.pi
    def hargreaves(tmin, tmax, tmean, lat, date):
        #calculate the day of year
        dfdate = date
        tempday = np.array(dfdate.timetuple().tm_yday)
        day_of_year = np.tile(tempday.reshape(-1, 1), [1, tmin.shape[-1]])
        # Loop to reduce memory usage
        pet = np.zeros(tmin.shape, dtype=np.float32) * np.nan

        temp_range = tmax - tmin
        temp_range[temp_range < 0] = 0

        latitude = np.deg2rad(lat)

        sol_dec = 0.409 * np.sin(((2.0 * np.pi / 365.0) * day_of_year - 1.39))
        sha = np.arccos(np.clip(-np.tan(latitude) * np.tan(sol_dec), -1, 1))
        ird = 1 + (0.033 * np.cos((2.0 * np.pi / 365.0) * day_of_year))
        tmp2 = sha * np.sin(latitude) * np.sin(sol_dec)
        tmp3 = np.cos(latitude) * np.cos(sol_dec) * np.sin(sha)
        et_rad = tmp1 * SOLAR_CONSTANT * ird * (tmp2 + tmp3)
        et_rad = et_rad.reshape(-1)
        pet = 0.0023 * (tmean + 17.8) * temp_range ** 0.5 * 0.408 * et_rad

        pet[pet < 0] = 0
        return pet
    
    # read 24 hour chunks at a time to calculate temperature stats
    # if a 24 hr chunk not available, then stats computed for whatever length of timestep is there    
    num_cats = len(dataset['catchment'])
    num_ts = len(dataset['time'])
    day_chunk_start_idx = 0
    pet_array = np.empty((num_cats, num_ts))

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TextColumn("{task.completed}/{task.total}"),
        "•",
        TextColumn(" Elapsed Time:"),
        TimeElapsedColumn(),
        TextColumn(" Remaining Time:"),
        TimeRemainingColumn(),
    )

    int_days = np.ceil(num_ts / 24)

    timer = time.perf_counter()
    day_chunk_task = progress.add_task(
        "[cyan]Calculating PET...", total=int_days, elapsed=0
    )
    progress.start()
    while day_chunk_start_idx <= num_ts - 1:
        progress.update(day_chunk_task, advance=1)
        ts_start = pd.to_datetime(dataset.time.values[day_chunk_start_idx])
        if day_chunk_start_idx + 23 <= num_ts - 1:
            ts_diff = 24
        else: # in case there isn't a full day left in the forcings file
            ts_diff = num_ts-day_chunk_start_idx
        day_chunk = dataset.isel(
            time=slice(day_chunk_start_idx,day_chunk_start_idx+ts_diff))['TMP_2maboveground']

        cat_temps = day_chunk.values
        # calculate stats
        tmin = np.min(cat_temps, axis=1)
        tmax = np.max(cat_temps, axis=1)
        tmean = np.mean(cat_temps, axis=1)
        lat = dataset['lat'].values

        pet = hargreaves(tmin, tmax, tmean, lat, ts_start)
        day_pet = np.repeat(pet[:, np.newaxis], ts_diff, axis=1)
        pet_array[:, day_chunk_start_idx:day_chunk_start_idx+ts_diff] = day_pet

        day_chunk_start_idx += 24

    progress.update(
        day_chunk_task,
        description=f"PET calculated in {time.perf_counter() - timer:2f} seconds",
    )
    progress.stop()
    dataset["PET"] = (("catchment", "time"), pet_array)
    dataset["PET"].attrs["units"] = "mm day^-1"  # ^-1 notation copied from source data
    return dataset
    
def get_index_chunks(data: xr.DataArray) -> list[tuple[int, int]]:
    """
    Take a DataArray and calculate the start and end index for each chunk based
    on the available memory.

    Parameters
    ----------
    data : xr.DataArray
        Large DataArray that can't be loaded into memory all at once.

    Returns
    -------
    list[Tuple[int, int]]
        Each element in the list represents a chunk of data. The tuple within
        the chunk indicates the start index and end index of the chunk.
    """
    array_memory_usage = data.nbytes
    free_memory = psutil.virtual_memory().available * 0.8  # 80% of available memory
    # limit the chunk to 20gb, makes things more stable
    free_memory = min(free_memory, 20 * 1024 * 1024 * 1024)
    num_chunks = ceil(array_memory_usage / free_memory)
    max_index = data.shape[0]
    stride = max_index // num_chunks
    chunk_start = range(0, max_index, stride)
    index_chunks = [(start, start + stride) for start in chunk_start]
    return index_chunks


def create_shared_memory(
    lazy_array: xr.DataArray,
) -> Tuple[shared_memory.SharedMemory, Tuple[int, ...], np.dtype]:
    """
    Create a shared memory object so that multiple processes can access loaded
    data.

    Parameters
    ----------
    lazy_array : xr.Dataset
        A chunk of gridded forcing variable data.

    Returns
    -------
    shared_memory.SharedMemory
        A specific block of memory allocated by the OS of the size of
        lazy_array.
    Tuple[int, ...]
        A shape object with dimensions (# timesteps, # of raster cells) in
        reference to lazy_array.
    np.dtype
        Data type of objects in lazy_array.
    """
    logger.debug(f"Creating shared memory size {lazy_array.nbytes / 10**6} Mb.")
    shm = shared_memory.SharedMemory(create=True, size=lazy_array.nbytes)
    shared_array = np.ndarray(lazy_array.shape, dtype=np.float32, buffer=shm.buf)
    # if your data is not float32, xarray will do an automatic conversion here
    # which consumes a lot more memory, forcings downloaded with this tool will work
    for start, end in get_index_chunks(lazy_array):
        # copy data from lazy to shared memory one chunk at a time
        shared_array[start:end] = lazy_array[start:end]

    time, x, y = shared_array.shape
    shared_array = shared_array.reshape(time, -1)

    return shm, shared_array.shape, shared_array.dtype


def process_chunk_shared(
    variable: str,
    times: np.ndarray,
    shm_name: str,
    shape: Tuple[int, ...],
    dtype: np.dtype,
    chunk: pd.DataFrame,
) -> xr.DataArray:
    """
    Process the gridded forcings chunk loaded into a SharedMemory block.

    Parameters
    ----------
    variable : str
        Name of forcing variable to be processed.
    times : np.ndarray
        Timesteps in gridded forcings chunk.
    shm_name : str
        Unique name that identifies the SharedMemory block.
    shape : np.dtype.shape
        A shape object with dimensions (# timesteps, # of raster cells) in
        reference to the gridded forcings chunk.
    dtype : np.dtype
        Data type of objects in the gridded forcings chunk.
    chunk : gpd.GeoDataFrame
        A chunk of gridded forcings data.

    Returns
    -------
    xr.DataArray
        Averaged forcings data for each timestep for each catchment.
    """
    existing_shm = shared_memory.SharedMemory(name=shm_name)
    raster = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)
    results = []

    for catchment in chunk.index.unique():
        cell_ids = chunk.loc[catchment]["cell_id"]
        weights = chunk.loc[catchment]["coverage"]
        mean_at_timesteps = weighted_sum_of_cells(raster, cell_ids, weights)
        temp_da = xr.DataArray(
            mean_at_timesteps,
            dims=["time"],
            coords={"time": times},
            name=f"{variable}_{catchment}",
        )
        temp_da = temp_da.assign_coords(catchment=catchment)
        results.append(temp_da)
    existing_shm.close()
    return xr.concat(results, dim="catchment")


def get_cell_weights_parallel(
    gdf: gpd.GeoDataFrame, input_forcings: xr.Dataset, num_partitions: int
) -> pd.DataFrame:
    """
    Execute get_cell_weights with multiprocessing, with chunking for the passed
    GeoDataFrame to conserve memory usage.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        A GeoDataFrame with a polygon feature.
    input_forcings : xr.Dataset
        A gridded forcings file.
    num_partitions : int
        Number of chunks to split gdf into.

    Returns
    -------
    pd.DataFrame
        DataFrame indexed by divide_id that contains information about coverage
        for each raster cell and each timestep in gridded forcing file.
    """
    gdf_chunks = np.array_split(gdf, num_partitions)
    wkt = gdf.crs.to_wkt()  # type: ignore
    one_timestep = input_forcings.isel(time=0).compute()
    with multiprocessing.Pool() as pool:
        args = [(one_timestep, gdf_chunk, wkt) for gdf_chunk in gdf_chunks]
        catchments = pool.starmap(get_cell_weights, args)
    return pd.concat(catchments)


def get_units(dataset: xr.Dataset) -> dict:
    """
    Return dictionary of units for each variable in dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset with variables and units.

    Returns
    -------
    dict
        {variable name: unit}
    """
    units = {}
    for var in dataset.data_vars:
        if dataset[var].attrs["units"]:
            units[var] = dataset[var].attrs["units"]
    return units

def get_lats(gdf: gpd.GeoDataFrame) -> dict:
    '''
    return latitudes for each catchment
    '''
    cats = gdf['divide_id']

    # convert to a geographic crs so we get actual degrees for lat/lon
    gdf_geog = gdf.to_crs(4326)
    with warnings.catch_warnings(): # it will complain about it being a geographic CRS, this is to shut it up
        warnings.simplefilter("ignore")
        lats = gdf_geog.centroid.y

    cat_lat = dict(zip(cats, lats))
    return cat_lat


def interpolate_nan_values(
    dataset: xr.Dataset,
    dim: str = "time",
    method: InterpOptions = "linear",
    fill_value: str = "extrapolate",
) -> bool:
    """
    Interpolates NaN values in specified (or all numeric time-dependent)
    variables of an xarray.Dataset. Operates inplace on the dataset.

    Parameters
    ----------
    dataset : xr.Dataset
        The input dataset.
    dim : str, optional
        The dimension along which to interpolate (default is "time").
    method : str, optional
        Interpolation method to use (e.g., "linear", "nearest", "cubic").
        Default is "linear".
    fill_value : str, optional
        Method for filling NaNs at the start/end of the series after interpolation.
        Set to "extrapolate" to fill with the nearest valid value when using 'nearest' or 'linear'.
        Default is "extrapolate".
    """
    for name, var in dataset.data_vars.items():
        # if the variable is non-numeric, skip
        if not np.issubdtype(var.dtype, np.number):
            continue
        # if there are no NANs, skip
        if not var.isnull().any().compute():
            continue
        logger.info("Interpolating NaN values in %s", name)
        var = var.compute()
        dataset[name] = var.interpolate_na(
            dim=dim,
            method=method,
            fill_value=fill_value if method in ["nearest", "linear"] else None,
        )


@no_cluster
def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, gridded_data: xr.Dataset, forcings_dir: Path, dhbv: bool
) -> None:
    """
    Compute zonal statistics in parallel for all timesteps over all desired
    catchments. Create chunks of catchments and within those, chunks of
    timesteps for memory management.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Contains identity and geometry information on desired catchments.
    merged_data : xr.Dataset
        Gridded forcing data that intersects with desired catchments.
    forcings_dir : Path
        Path to directory where outputs are to be stored.
    """
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    num_partitions = multiprocessing.cpu_count() - 1
    if num_partitions > len(gdf):
        num_partitions = len(gdf)

    catchments = get_cell_weights_parallel(gdf, gridded_data, num_partitions)
    units = get_units(gridded_data)
    cat_lat = get_lats(gdf)

    cat_chunks: List[pd.DataFrame] = np.array_split(catchments, num_partitions)  # type: ignore

    progress_file = FilePaths(output_dir=forcings_dir.parent).forcing_progress_file
    ex_var_name = list(gridded_data.data_vars)[0]
    example_time_chunks = get_index_chunks(gridded_data[ex_var_name])

    if dhbv: # cut down on processing time for dhbv
        data_vars = ['APCP_surface', 'TMP_2maboveground']
    else:
        data_vars = gridded_data.data_vars

    all_steps = len(example_time_chunks) * len(data_vars)
    logger.info(
        f"Total steps: {all_steps}, Number of time chunks: {len(example_time_chunks)}, Number of variables: {len(data_vars)}"
    )
    steps_completed = 0
    with open(progress_file, "w") as f:
        json.dump({"total_steps": all_steps, "steps_completed": steps_completed}, f)

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TextColumn("{task.completed}/{task.total}"),
        "•",
        TextColumn(" Elapsed Time:"),
        TimeElapsedColumn(),
        TextColumn(" Remaining Time:"),
        TimeRemainingColumn(),
    )

    timer = time.perf_counter()
    variable_task = progress.add_task(
        "[cyan]Processing variables...", total=len(data_vars), elapsed=0
    )
    progress.start()

    for data_var_name in list(data_vars):
        data_var_name: str
        progress.update(variable_task, advance=1)
        progress.update(variable_task, description=f"Processing {data_var_name}")

        # to make sure this fits in memory, we need to chunk the data
        time_chunks = get_index_chunks(gridded_data[data_var_name])
        chunk_task = progress.add_task("[purple] processing chunks", total=len(time_chunks))
        for i, times in enumerate(time_chunks):
            progress.update(chunk_task, advance=1)
            start, end = times
            # select the chunk of time we want to process
            data_chunk = gridded_data[data_var_name].isel(time=slice(start, end))
            # put it in shared memory
            shm, shape, dtype = create_shared_memory(data_chunk)
            times = data_chunk.time.values
            # create a partial function to pass to the multiprocessing pool
            partial_process_chunk = partial(
                process_chunk_shared, data_var_name, times, shm.name, shape, dtype
            )

            logger.debug(f"Processing variable: {data_var_name}")
            # process the chunks of catchments in parallel
            with multiprocessing.Pool(num_partitions) as pool:
                variable_data = pool.map(partial_process_chunk, cat_chunks)
            del partial_process_chunk
            # clean up the shared memory
            shm.close()
            shm.unlink()
            logger.debug(f"Processed variable: {data_var_name}")
            concatenated_da = xr.concat(variable_data, dim="catchment")
            # delete the data to free up memory
            del variable_data
            logger.debug(f"Concatenated variable: {data_var_name}")
            # write this to disk now to save memory
            # xarray will monitor memory usage, but it doesn't account for the shared memory used to store the raster
            # This reduces memory usage by about 60%
            concatenated_da.to_dataset(name=data_var_name).to_netcdf(
                forcings_dir / "temp" / f"{data_var_name}_timechunk_{i}.nc"
            )
            steps_completed += 1
            with open(progress_file, "w") as f:
                json.dump({"total_steps": all_steps, "steps_completed": steps_completed}, f)
        # Merge the chunks back together
        datasets = [
            xr.open_dataset(forcings_dir / "temp" / f"{data_var_name}_timechunk_{i}.nc")
            for i in range(len(time_chunks))
        ]
        result = xr.concat(datasets, dim="time")
        result.to_netcdf(forcings_dir / "temp" / f"{data_var_name}.nc")
        # close the datasets
        result.close()
        _ = [dataset.close() for dataset in datasets]
        for file in forcings_dir.glob("temp/*_timechunk_*.nc"):
            file.unlink()
        progress.remove_task(chunk_task)
    progress.update(
        variable_task,
        description=f"Forcings processed in {time.perf_counter() - timer:2f} seconds",
    )
    progress.stop()
    logger.info(
        f"Forcing generation complete! Zonal stats computed in {time.time() - timer_start:2f} seconds"
    )
    write_outputs(forcings_dir, units, cat_lat, dhbv)
    time.sleep(1)  # wait for progress bar to update
    progress_file.unlink()


@use_cluster
def write_outputs(forcings_dir: Path, units: dict, cat_lat: dict, dhbv: bool) -> None:
    """
    Write outputs to disk in the form of a NetCDF file, using dask clusters to
    facilitate parallel computing.

    Parameters
    ----------
    forcings_dir : Path
        Path to directory where outputs are to be stored.
    variables : dict
        Preset dictionary where the keys are forcing variable names and the
        values are units.
    units : dict
        Dictionary where the keys are forcing variable names and the values are
        units. Differs from variables, as this dictionary depends on the gridded
        forcing dataset.
    """
    temp_forcings_dir = forcings_dir / "temp"
    # Combine all variables into a single dataset using dask
    results = [xr.open_dataset(file, chunks="auto") for file in temp_forcings_dir.glob("*.nc")]
    final_ds = xr.merge(results)
    for var in final_ds.data_vars:
        if var in units:
            final_ds[var].attrs["units"] = units[var]
        else:
            logger.warning(f"Variable {var} has no units")

    rename_dict = {}

    final_ds = final_ds.rename_vars(rename_dict)
    if "APCP_surface" in final_ds.data_vars:
        final_ds = add_precip_rate_to_dataset(final_ds)
    elif "precip_rate" in final_ds.data_vars:
        final_ds = add_APCP_SURFACE_to_dataset(final_ds)

    # this step halves the storage size of the forcings
    for var in final_ds.data_vars:
        final_ds[var] = final_ds[var].astype(np.float32)

    # The format for the netcdf is to support a legacy format
    # which is why it's a little "unorthodox"
    # There are no coordinates, just dimensions, catchment ids are stored in a 1d data var
    # and time is stored in a 2d data var with the same time array for every catchment
    # time is stored as unix timestamps, units have to be set
    # add the catchment ids as a 1d data var
    final_ds["ids"] = final_ds["catchment"].astype(str)

    # lat 1d var and PET added for dHBV
    if dhbv:
        logger.info("Calculating PET from temperature values...")
        final_ds["lat"] = (("catchment"), [cat_lat[cat] for cat in final_ds["ids"].values])
        final_ds = add_pet_to_dataset(final_ds)

        dhbv_renamedict = {'precip_rate': 'P',
                    'TMP_2maboveground': 'Temp'}
        final_ds = final_ds.drop_vars("APCP_surface")
        final_ds = final_ds.rename_vars(dhbv_renamedict)

    # time needs to be a 2d array of the same time array as unix timestamps for every catchment
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        time_array = (
            final_ds.time.astype("datetime64[s]").astype(np.int64).values // 10**9
        )  ## convert from ns to s
    time_array = time_array.astype(np.int32)  ## convert to int32 to save space
    final_ds = final_ds.drop_vars(
        ["catchment", "time"]
    )  ## drop the original time and catchment vars
    final_ds = final_ds.rename_dims({"catchment": "catchment-id"})  # rename the catchment dimension
    # add the time as a 2d data var, yes this is wasting disk space.
    final_ds["Time"] = (("catchment-id", "time"), [time_array for _ in range(len(final_ds["ids"]))])
    # set the time unit
    final_ds["Time"].attrs["units"] = "s"
    final_ds["Time"].attrs["epoch_start"] = (
        "01/01/1970 00:00:00"  # not needed but suppresses the ngen warning
    )
    interpolate_nan_values(final_ds)

    logger.info("Saving to disk")
    final_ds.to_netcdf(forcings_dir / "forcings.nc", engine="netcdf4")
    # close the datasets
    _ = [result.close() for result in results]
    final_ds.close()

    # clean up the temp files
    for file in temp_forcings_dir.glob("*.*"):
        file.unlink()
    temp_forcings_dir.rmdir()


def setup_directories(cat_id: str) -> FilePaths:
    forcing_paths = FilePaths(cat_id)
    # delete everything in the forcing folder except the cached nc file
    for file in forcing_paths.forcings_dir.glob("*.*"):
        if file != forcing_paths.cached_nc_file:
            file.unlink()

    os.makedirs(forcing_paths.forcings_dir / "temp", exist_ok=True)

    return forcing_paths


def create_forcings(dataset: xr.Dataset, output_folder_name: str, dhbv: bool) -> None:
    validate_dataset_format(dataset)
    forcing_paths = setup_directories(output_folder_name)
    logger.debug(f"forcing path {output_folder_name} {forcing_paths.forcings_dir}")
    gdf = gpd.read_file(forcing_paths.geopackage_path, layer="divides")
    logger.debug(f"gdf  bounds: {gdf.total_bounds}")
    gdf = gdf.to_crs(dataset.crs)
    compute_zonal_stats(gdf, dataset, forcing_paths.forcings_dir, dhbv)
