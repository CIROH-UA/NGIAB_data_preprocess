import logging
import multiprocessing
import os
import time
import warnings
from pathlib import Path
from typing import Tuple
from datetime import datetime
from functools import partial

from tqdm.rich import tqdm
import numpy as np
import dask
from dask.distributed import Client, LocalCluster
import geopandas as gpd
import pandas as pd
import s3fs
import xarray as xr
from exactextract import exact_extract
from multiprocessing import shared_memory

from data_processing.file_paths import file_paths

logger = logging.getLogger(__name__)
# Suppress the specific warning from numpy
warnings.filterwarnings(
    "ignore", message="'DataFrame.swapaxes' is deprecated", category=FutureWarning
)
warnings.filterwarnings(
    "ignore", message="'GeoDataFrame.swapaxes' is deprecated", category=FutureWarning
)


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


def load_geodataframe(geopackage_path: str, projection: str) -> gpd.GeoDataFrame:
    """Load and project a geodataframe from a given path and projection."""
    gdf = gpd.read_file(geopackage_path, layer="divides").to_crs(projection)
    return gdf


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
    stores.to_netcdf(cached_nc_path)
    data = xr.open_mfdataset(cached_nc_path, parallel=True, engine="h5netcdf")
    return data


def create_delayed_save(catchment, output_folder, final_ds):
    csv_path = output_folder / f"{catchment}.csv"
    catchment_ds = final_ds.sel(catchment=catchment)
    return dask.delayed(save_to_csv)(catchment_ds, csv_path)


def create_delayed_saves_pool(final_ds, output_folder):
    all_catchments = final_ds.catchment.values
    create_save = partial(create_delayed_save, output_folder=output_folder, final_ds=final_ds)
    num_workers = multiprocessing.cpu_count()

    with multiprocessing.Pool(num_workers) as pool:
        delayed_saves = pool.map(create_save, all_catchments)

    logger.debug("All delayed saves created")
    return delayed_saves


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


def save_to_csv(catchment_ds, csv_path):
    catchment_df = catchment_ds.to_dataframe().drop(["catchment"], axis=1)
    catchment_df.to_csv(csv_path)
    return csv_path


def create_shared_memory(data):
    shm = shared_memory.SharedMemory(create=True, size=data.nbytes)
    shared_array = np.ndarray(data.shape, dtype=data.dtype, buffer=shm.buf)
    shared_array[:] = data[:]
    return shm, shared_array


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


def compute_zonal_stats(
    gdf: gpd.GeoDataFrame, merged_data: xr.Dataset, forcings_dir: Path
) -> None:
    logger.info("Computing zonal stats in parallel for all timesteps")
    timer_start = time.time()
    gfd_chunks = np.array_split(gdf, multiprocessing.cpu_count() - 1)
    one_timestep = merged_data.isel(time=0).compute()
    with multiprocessing.Pool() as pool:
        args = [(one_timestep, gdf_chunk) for gdf_chunk in gfd_chunks]
        catchments = pool.starmap(get_cell_weights, args)

    catchments = pd.concat(catchments)

    variables = [
        "LWDOWN",
        "PSFC",
        "Q2D",
        "RAINRATE",
        "SWDOWN",
        "T2D",
        "U2D",
        "V2D",
    ]

    results = []

    partitions = multiprocessing.cpu_count()
    # if len(catchments) > 1000:
    #     optimal_partitions = len(catchments) // 1000
    #     partitions = min(optimal_partitions, partitions)

    for variable in variables:
        raster = merged_data[variable].values.reshape(merged_data[variable].shape[0], -1)

        # Create shared memory for the raster
        shm, shared_raster = create_shared_memory(raster)

        cat_chunks = np.array_split(catchments, partitions)
        times = merged_data.time.values

        partial_process_chunk = partial(
            process_chunk_shared,
            variable,
            times,
            shm.name,
            shared_raster.shape,
            shared_raster.dtype,
        )

        logger.debug(f"Processing variable: {variable}")
        with multiprocessing.Pool(partitions) as pool:
            variable_data = pool.map(partial_process_chunk, cat_chunks)

        # Clean up the shared memory
        shm.close()
        shm.unlink()

        logger.debug(f"Processed variable: {variable}")
        concatenated_da = xr.concat(variable_data, dim="catchment")
        logger.debug(f"Concatenated variable: {variable}")
        results.append(concatenated_da.to_dataset(name=variable))

    # Combine all variables into a single dataset
    final_ds = xr.merge(results)

    output_folder = forcings_dir / "by_catchment"
    # Clear out any existing files
    for file in output_folder.glob("*.csv"):
        file.unlink()

    final_ds = final_ds.rename_vars(
        {
            "LWDOWN": "DLWRF_surface",
            "PSFC": "PRES_surface",
            "Q2D": "SPFH_2maboveground",
            "RAINRATE": "precip_rate",
            "SWDOWN": "DSWRF_surface",
            "T2D": "TMP_2maboveground",
            "U2D": "UGRD_10maboveground",
            "V2D": "VGRD_10maboveground",
        }
    )

    final_ds = add_APCP_SURFACE_to_dataset(final_ds)

    logger.info("Saving to disk")
    delayed_saves = create_delayed_saves_pool(final_ds, output_folder)

    if not Client(timeout="2s"):
        cluster = LocalCluster()
    dask.compute(*delayed_saves)

    logger.info(
        f"Forcing generation complete! Zonal stats computed in {time.time() - timer_start} seconds"
    )


def setup_directories(wb_id: str) -> file_paths:
    forcing_paths = file_paths(wb_id)

    for folder in ["by_catchment", "temp"]:
        os.makedirs(forcing_paths.forcings_dir() / folder, exist_ok=True)
    return forcing_paths


def create_forcings(start_time: str, end_time: str, output_folder_name: str) -> None:
    forcing_paths = setup_directories(output_folder_name)
    projection = xr.open_dataset(forcing_paths.template_nc(), engine="h5netcdf").crs.esri_pe_string
    logger.debug("Got projection from grid file")

    gdf = load_geodataframe(forcing_paths.geopackage_path(), projection)
    logger.debug("Got gdf")

    if type(start_time) == datetime:
        start_time = start_time.strftime("%Y-%m-%d %H:%M")
    if type(end_time) == datetime:
        end_time = end_time.strftime("%Y-%m-%d %H:%M")

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
        logger.info("Loading zarr stores, this may take a while.")
        lazy_store = load_zarr_datasets()
        logger.debug("Got zarr stores")

        clipped_store = clip_dataset_to_bounds(lazy_store, gdf.total_bounds, start_time, end_time)
        logger.info("Clipped forcing data to bounds")

        merged_data = compute_store(clipped_store, forcing_paths.cached_nc_file())
        logger.info("Forcing data loaded and cached")

    logger.info("Computing zonal stats")
    compute_zonal_stats(gdf, merged_data, forcing_paths.forcings_dir())


if __name__ == "__main__":
    # Example usage
    start_time = "2010-01-01 00:00"
    end_time = "2010-01-02 00:00"
    output_folder_name = "wb-1643991"
    # looks in output/wb-1643991/config for the geopackage wb-1643991_subset.gpkg
    # puts forcings in output/wb-1643991/forcings
    logger.basicConfig(level=logging.DEBUG)
    create_forcings(start_time, end_time, output_folder_name)
