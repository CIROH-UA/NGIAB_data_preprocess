import logging
import multiprocessing
import os
import time
import warnings
from pathlib import Path
from datetime import datetime
from functools import partial

from tqdm.rich import tqdm
import numpy as np
import dask
from dask.distributed import Client, LocalCluster, progress
import geopandas as gpd
import pandas as pd
import xarray as xr
from exactextract import exact_extract
from multiprocessing import shared_memory

from data_processing.file_paths import file_paths
from data_processing.zarr_utils import get_forcing_data

logger = logging.getLogger(__name__)
# Suppress the specific warning from numpy
warnings.filterwarnings(
    "ignore", message="'DataFrame.swapaxes' is deprecated", category=FutureWarning
)
warnings.filterwarnings(
    "ignore", message="'GeoDataFrame.swapaxes' is deprecated", category=FutureWarning
)


def weighted_sum_of_cells(flat_tensor, cell_ids, factors):
    # Create an output array initialized with zeros
    result = np.zeros(flat_tensor.shape[0])
    result = np.sum(flat_tensor[:, cell_ids] * factors, axis=1)
    sum_of_weights = np.sum(factors)
    result /= sum_of_weights
    return result

def get_cell_weights(raster, gdf):
    output = exact_extract(
        raster["LWDOWN"],
        gdf,
        ["cell_id", "coverage"],
        include_cols=["divide_id"],
        output="pandas",
    )
    return output.set_index("divide_id")


def add_APCP_SURFACE_to_dataset(dataset: xr.Dataset) -> xr.Dataset:
    dataset["APCP_surface"] = (dataset["precip_rate"] * 3600 * 1000) / 0.9998
    return dataset

def get_index_chunks(data):
    peak_memory_usage = data.nbytes * 1.3 # 30% overhead for safety
    free_memory = psutil.virtual_memory().available
    free_memory = 10000000
    num_chunks = ceil(peak_memory_usage / free_memory)
    max_index = data.shape[0]
    stride = max_index // num_chunks
    chunk_start = range(0, max_index, stride)
    index_chunks = [(start, start + stride) for start in chunk_start]
    return index_chunks

from memory_profiler import profile
#@profile
def create_shared_memory(lazy_array):
    logger.warning(f"Creating shared memory size {lazy_array.nbytes/ 10**6} Mb.")
    shm = shared_memory.SharedMemory(create=True, size=lazy_array.nbytes)
    shared_array = np.ndarray(lazy_array.shape, dtype=np.float32, buffer=shm.buf)
    # if your data is not float32, xarray will do an automatic conversion here
    # which consumes 3x the size of nbytes, otherwise only nbytes is consumed
    #np.copyto(shared_array, data)# = np.asarray(data)
    for start, end in get_index_chunks(lazy_array):
            shared_array[start:end] = lazy_array[start:end]
    #del data
    time, x, y = shared_array.shape
    shared_array = shared_array.reshape(time, -1)
    print(f"Shared array shape {shared_array.shape}")
    print(f"Shared array sample {shared_array[0, 8000]}")
    #shared_array.flags.writeable = False
    return shm, shared_array.shape, shared_array.dtype


def process_chunk_shared(variable, times, shm_name, shape, dtype, chunk):
    existing_shm = shared_memory.SharedMemory(name=shm_name)
    raster = np.ndarray(shape, dtype=dtype, buffer=existing_shm.buf)
    #print(f"Raster shape {raster.shape}")
    results = []
    if "cat-481888" in chunk.index.unique():
        print(f"raster sample {raster[0, 8000]}")

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

import psutil
from math import ceil

def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    num_partitions = multiprocessing.cpu_count() - 1
    if num_partitions > len(gdf):
        num_partitions = len(gdf)
    gdf_chunks = np.array_split(gdf, multiprocessing.cpu_count() - 1)
    one_timestep = merged_data.isel(time=0).compute()
    with multiprocessing.Pool() as pool:
        args = [(one_timestep, gdf_chunk) for gdf_chunk in gdf_chunks]
        catchments = pool.starmap(get_cell_weights, args)

    catchments = pd.concat(catchments)
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

    results = []
    cat_chunks = np.array_split(catchments, num_partitions)
    forcing_times = merged_data.time.values

    for variable in variables.keys():
        peak_memory_usage = merged_data[variable].nbytes * 1.3 # 30% overhead for safety
        free_memory = psutil.virtual_memory().available
        free_memory = 105312678.4
        mem_chunks = 1
        if peak_memory_usage > free_memory:
            logger.warning(f"Variable {variable} requires {peak_memory_usage} bytes, but only {free_memory} bytes are available.")
            mem_chunks = ceil(peak_memory_usage / free_memory)
            logger.warning(f"Splitting variable {variable} into {mem_chunks} chunks.")

        timesteps_per_chunk = len(forcing_times) // mem_chunks
        chunk_start = range(0, len(forcing_times), timesteps_per_chunk)
        time_chunks = [(start, start + timesteps_per_chunk) for start in chunk_start]
        logger.info(f" chunks {time_chunks}")

        for i, times in enumerate(time_chunks):
            start = times[0]
            end = times[1]
            data_chunk = merged_data[variable].isel(time=slice(start,end))
            shm, shape, dtype = create_shared_memory(data_chunk)
            times = data_chunk.time.values
            partial_process_chunk = partial(
                process_chunk_shared,
                variable,
                times,
                shm.name,
                shape,
                dtype,
            )
            logger.debug(f"Processing variable: {variable}")
            with multiprocessing.Pool(num_partitions) as pool:
                variable_data = pool.map(partial_process_chunk, cat_chunks)
            del partial_process_chunk
            # Clean up the shared memory
            shm.close()
            shm.unlink()
            logger.debug(f"Processed variable: {variable}")
            concatenated_da = xr.concat(variable_data, dim="catchment")
            del variable_data
            logger.debug(f"Concatenated variable: {variable}")
            # write this to disk now to save memory
            # xarray will monitor memory usage, but it doesn't account for the shared memory used to store the raster
            # This reduces memory usage by about 60%
            concatenated_da.to_dataset(name=variable).to_netcdf(forcings_dir/ "temp" / f"{variable}_{i}.nc")
            # Merge the chunks back together
        datasets = [xr.open_dataset(forcings_dir / "temp" / f"{variable}_{i}.nc") for i in range(mem_chunks)]
        xr.concat(datasets, dim="time").to_netcdf(forcings_dir / f"{variable}.nc")
        for file in forcings_dir.glob("temp/*.nc"):
            file.unlink()


    # Combine all variables into a single dataset
    results = [xr.open_dataset(forcings_dir / f"{variable}.nc") for variable in variables.keys()]
    final_ds = xr.merge(results)

    output_folder = forcings_dir / "by_catchment"

    final_ds = final_ds.rename_vars(variables)
    print(final_ds)
    for variable in variables.values():
        final_ds[variable] = final_ds[variable].astype(np.float32)

    final_ds = add_APCP_SURFACE_to_dataset(final_ds)

    logger.info("Saving to disk")
    #rename catchment to ids
    # required format ....
    # >>> print(data)
    # <xarray.Dataset> Size: 144kB
    # Dimensions:              (catchment-id: 5, time: 720)
    # Dimensions without coordinates: catchment-id, time
    # Data variables:
    #     ids                  (catchment-id) <U9 180B 'cat-11410' ... 'cat-11224'
    #     Time                 (catchment-id, time) float64 29kB ...
    #     precip_rate          (catchment-id, time) float32 14kB ...
    #     TMP_2maboveground    (catchment-id, time) float32 14kB ...
    #     SPFH_2maboveground   (catchment-id, time) float32 14kB ...
    #     UGRD_10maboveground  (catchment-id, time) float32 14kB ...
    #     VGRD_10maboveground  (catchment-id, time) float32 14kB ...
    #     PRES_surface         (catchment-id, time) float32 14kB ...
    #     DSWRF_surface        (catchment-id, time) float32 14kB ...
    #     DLWRF_surface        (catchment-id, time) float32 14kB ...
    # >>> print(data.ids.values)
    # ['cat-11410' 'cat-11371' 'cat-11509' 'cat-11223' 'cat-11224']
    # >>> print(data['catchment-id'].values)
    # [0 1 2 3 4]
    # >>>
    # to force our data into this format, we need to delete the coordinates and add a data var with the catchment ids

    #
    # TIME IS A 2D ARRAY OF THE SAME TIME FOR EVERY CATCHMENT !!!!!!!!!!!!!!!!!!!!!!
    #drop all coords
    final_ds["ids"] = final_ds["catchment"].astype(str)
    # time needs to be a 2d array of the same time array as unix timestamps for every catchment
    time_array = final_ds.time.astype('datetime64[s]').astype(np.int64).values//10**9
    time_array = time_array.astype(np.int32)
    #print(final_ds.head().time.astype('datetime64[s]').astype(np.int64).values//10**9)
    final_ds = final_ds.drop_vars(["catchment", "time"])
    final_ds = final_ds.rename_dims({"catchment": "catchment-id"})
    final_ds["Time"] = (("catchment-id", "time"), [time_array for _ in range(len(final_ds["ids"]))])
    # time is expected to be a 2d array of catchment id x time
    # since time is the same for every catchment, we can save space by just repeating the time array
    #final_ds["Time"] = ("time",time_array)
    final_ds["Time"].attrs["units"] = "s"
    final_ds["Time"].attrs["epoch_start"] = "01/01/1970 00:00:00" # not needed but suppresses the ngen warning
    # lifted from ngen code good luck figuring out if it's mmddyyyy or ddmmyyyy. why is it not yyyymmdd...
    #add catchment ids as a data var

    final_ds.to_netcdf(output_folder / "forcings.nc", engine="netcdf4")
    print(final_ds)

    logger.info(
        f"Forcing generation complete! Zonal stats computed in {time.time() - timer_start} seconds"
    )

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
    logger.debug(gdf)
    logger.debug("Got gdf")

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
