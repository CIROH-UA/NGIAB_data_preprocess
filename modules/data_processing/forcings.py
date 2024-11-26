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

import geopandas as gpd
import numpy as np
import pandas as pd
import psutil
import xarray as xr
from data_processing.file_paths import file_paths
from data_processing.zarr_utils import get_forcing_data
from exactextract import exact_extract
from exactextract.raster import NumPyRasterSource
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


logger = logging.getLogger(__name__)
# Suppress the specific warning from numpy to keep the cli output clean
warnings.filterwarnings(
    "ignore", message="'DataFrame.swapaxes' is deprecated", category=FutureWarning
)
warnings.filterwarnings(
    "ignore", message="'GeoDataFrame.swapaxes' is deprecated", category=FutureWarning
)


def weighted_sum_of_cells(flat_raster: np.ndarray, cell_ids: np.ndarray , factors: np.ndarray):
    # Create an output array initialized with zeros
    # dimensions are raster[time][x*y]
    result = np.zeros(flat_raster.shape[0])
    result = np.sum(flat_raster[:, cell_ids] * factors, axis=1)
    sum_of_weights = np.sum(factors)
    result /= sum_of_weights
    return result


def get_cell_weights(raster, gdf, wkt):
    # Get the cell weights for each divide
    xmin = raster.x[0]
    xmax = raster.x[-1]
    ymin = raster.y[0]
    ymax = raster.y[-1]
    rastersource = NumPyRasterSource(
        raster["RAINRATE"], srs_wkt=wkt, xmin=xmin, xmax=xmax, ymin=ymin, ymax=ymax
    )
    output = exact_extract(
        rastersource,
        gdf,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )
    return output.set_index("divide_id")


def add_APCP_SURFACE_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    # precip rate is mm/s
    # cfe says input m/h
    dataset["APCP_surface"] = dataset["precip_rate"] * 3600 / 1000
    # technically should be kg/m^2/h, at 1kg = 1l it equates to mm/h
    return dataset


def get_index_chunks(data: xr.DataArray) -> list[tuple[int, int]]:
    # takes a data array and calculates the start and end index for each chunk
    # based on the available memory.
    array_memory_usage = data.nbytes
    free_memory = psutil.virtual_memory().available * 0.8 # 80% of available memory
    num_chunks = ceil(array_memory_usage / free_memory)
    max_index = data.shape[0]
    stride = max_index // num_chunks
    chunk_start = range(0, max_index, stride)
    index_chunks = [(start, start + stride) for start in chunk_start]
    return index_chunks


def create_shared_memory(lazy_array):
    logger.debug(f"Creating shared memory size {lazy_array.nbytes/ 10**6} Mb.")
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


def process_chunk_shared(variable, times, shm_name, shape, dtype, chunk):
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


def get_cell_weights_parallel(gdf, input_forcings, num_partitions):
    gdf_chunks = np.array_split(gdf, num_partitions)
    wkt = gdf.crs.to_wkt()
    one_timestep = input_forcings.isel(time=0).compute()
    with multiprocessing.Pool() as pool:
        args = [(one_timestep, gdf_chunk, wkt) for gdf_chunk in gdf_chunks]
        catchments = pool.starmap(get_cell_weights, args)
    return pd.concat(catchments)


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    num_partitions = multiprocessing.cpu_count() - 1
    if num_partitions > len(gdf):
        num_partitions = len(gdf)

    catchments = get_cell_weights_parallel(gdf, merged_data, num_partitions)

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

    cat_chunks = np.array_split(catchments, num_partitions)

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
        "[cyan]Processing variables...", total=len(variables), elapsed=0
    )
    progress.start()
    for variable in variables.keys():
        progress.update(variable_task, advance=1)
        progress.update(variable_task, description=f"Processing {variable}")

        if variable not in merged_data.data_vars:
            logger.warning(f"Variable {variable} not in forcings, skipping")
            continue

        # to make sure this fits in memory, we need to chunk the data
        time_chunks = get_index_chunks(merged_data[variable])
        chunk_task = progress.add_task("[purple] processing chunks", total=len(time_chunks))
        for i, times in enumerate(time_chunks):
            progress.update(chunk_task, advance=1)
            start, end = times
            # select the chunk of time we want to process
            data_chunk = merged_data[variable].isel(time=slice(start,end))
            # put it in shared memory
            shm, shape, dtype = create_shared_memory(data_chunk)
            times = data_chunk.time.values
            # create a partial function to pass to the multiprocessing pool
            partial_process_chunk = partial(process_chunk_shared,variable,times,shm.name,shape,dtype)

            logger.debug(f"Processing variable: {variable}")
            # process the chunks of catchments in parallel
            with multiprocessing.Pool(num_partitions) as pool:
                variable_data = pool.map(partial_process_chunk, cat_chunks)
            del partial_process_chunk
            # clean up the shared memory
            shm.close()
            shm.unlink()
            logger.debug(f"Processed variable: {variable}")
            concatenated_da = xr.concat(variable_data, dim="catchment")
            # delete the data to free up memory
            del variable_data
            logger.debug(f"Concatenated variable: {variable}")
            # write this to disk now to save memory
            # xarray will monitor memory usage, but it doesn't account for the shared memory used to store the raster
            # This reduces memory usage by about 60%
            concatenated_da.to_dataset(name=variable).to_netcdf(forcings_dir/ "temp" / f"{variable}_{i}.nc")
        # Merge the chunks back together
        datasets = [xr.open_dataset(forcings_dir / "temp" / f"{variable}_{i}.nc") for i in range(len(time_chunks))]
        xr.concat(datasets, dim="time").to_netcdf(forcings_dir / f"{variable}.nc")
        for file in forcings_dir.glob("temp/*.nc"):
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
    write_outputs(forcings_dir, variables)

def write_outputs(forcings_dir, variables):

    # Combine all variables into a single dataset
    results = [xr.open_dataset(file) for file in forcings_dir.glob("*.nc")]
    final_ds = xr.merge(results)

    output_folder = forcings_dir / "by_catchment"

    rename_dict = {}
    for key, value in variables.items():
        if key in final_ds:
            rename_dict[key] = value

    final_ds = final_ds.rename_vars(rename_dict)
    final_ds = add_APCP_SURFACE_to_dataset(final_ds)

    # this step halves the storage size of the forcings
    for var in final_ds.data_vars:
        final_ds[var] = final_ds[var].astype(np.float32)

    logger.info("Saving to disk")
    # The format for the netcdf is to support a legacy format
    # which is why it's a little "unorthodox"
    # There are no coordinates, just dimensions, catchment ids are stored in a 1d data var
    # and time is stored in a 2d data var with the same time array for every catchment
    # time is stored as unix timestamps, units have to be set

    # add the catchment ids as a 1d data var
    final_ds["ids"] = final_ds["catchment"].astype(str)
    # time needs to be a 2d array of the same time array as unix timestamps for every catchment
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        time_array = (
            final_ds.time.astype("datetime64[s]").astype(np.int64).values // 10**9
        )  ## convert from ns to s
    time_array = time_array.astype(np.int32) ## convert to int32 to save space
    final_ds = final_ds.drop_vars(["catchment", "time"]) ## drop the original time and catchment vars
    final_ds = final_ds.rename_dims({"catchment": "catchment-id"}) # rename the catchment dimension
    # add the time as a 2d data var, yes this is wasting disk space.
    final_ds["Time"] = (("catchment-id", "time"), [time_array for _ in range(len(final_ds["ids"]))])
    # set the time unit
    final_ds["Time"].attrs["units"] = "s"
    final_ds["Time"].attrs["epoch_start"] = "01/01/1970 00:00:00" # not needed but suppresses the ngen warning

    final_ds.to_netcdf(output_folder / "forcings.nc", engine="netcdf4")
    # delete the individual variable files
    for file in forcings_dir.glob("*.nc"):
        file.unlink()


def setup_directories(cat_id: str) -> file_paths:
    forcing_paths = file_paths(cat_id)
    for folder in ["by_catchment", "temp"]:
        os.makedirs(forcing_paths.forcings_dir / folder, exist_ok=True)
    return forcing_paths


def create_forcings(start_time: str, end_time: str, output_folder_name: str) -> None:
    forcing_paths = setup_directories(output_folder_name)
    projection = xr.open_dataset(forcing_paths.template_nc, engine="h5netcdf").crs.esri_pe_string
    logger.debug("Got projection from grid file")

    gdf = gpd.read_file(forcing_paths.geopackage_path, layer="divides").to_crs(projection)
    logger.debug(f"gdf  bounds: {gdf.total_bounds}")

    if type(start_time) == datetime:
        start_time = start_time.strftime("%Y-%m-%d %H:%M")
    if type(end_time) == datetime:
        end_time = end_time.strftime("%Y-%m-%d %H:%M")

    merged_data = get_forcing_data(forcing_paths, start_time, end_time, gdf)
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir)


if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    output_folder_name = "cat-1643991"
    # looks in output/cat-1643991/config for the geopackage cat-1643991_subset.gpkg
    # puts forcings in output/cat-1643991/forcings
    logger.basicConfig(level=logging.DEBUG)
    create_forcings(start_time, end_time, output_folder_name)
