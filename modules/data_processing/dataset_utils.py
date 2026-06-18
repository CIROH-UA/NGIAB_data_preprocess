import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Literal, Optional, Tuple, Union

import geopandas as gpd
import numpy as np
import xarray as xr
from dask.distributed import Client, Future, progress
from data_processing.dask_utils import no_cluster, temp_cluster

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
    dataset: xr.Dataset,
    bounds: Tuple[float, float, float, float] | np.ndarray[tuple[int], np.dtype[np.float64]],
    start_time: str,
    end_time: str,
) -> xr.Dataset:
    """
    Clip the dataset to specified geographical bounds.

    Parameters
    ----------
    dataset : xr.Dataset
        Dataset to be clipped.
    bounds : tuple[float, float, float, float] | np.ndarray[tuple[int], np.dtype[np.float64]]
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
    samplex = dataset.x.values[:2]
    intervalx = samplex[1] - samplex[0]
    sampley = dataset.y.values[:2]
    intervaly = sampley[1] - sampley[0]
    dataset = dataset.sel(
        x=slice(bounds[0] - intervalx, bounds[2] + intervalx),
        y=slice(bounds[1] - intervaly, bounds[3] + intervaly),
        time=slice(start_time, end_time),
    )
    logger.info("Selected time range and clipped to bounds")
    return dataset


def reproject_bbox(
    bbox: Tuple[float, float, float, float],
    bbox_crs: str,
    dataset_crs: str,
) -> Tuple[float, float, float, float]:
    """
    Reproject a bounding box into the dataset's CRS.

    The box edges are densified before reprojection so that, for curved
    projections (e.g. the NWM Lambert Conformal Conic grid), the returned bounds
    fully enclose the requested area rather than only its four corners. This lets
    a single bbox (typically lon/lat) be used for both the aorc (lon/lat) and nwm
    (LCC, metres) sources.

    Parameters
    ----------
    bbox : tuple[float, float, float, float]
        (xmin, ymin, xmax, ymax) in bbox_crs.
    bbox_crs : str
        CRS the bbox coordinates are given in, e.g. "EPSG:4326".
    dataset_crs : str
        CRS of the gridded dataset to clip (dataset.crs / dataset.attrs["crs"]).

    Returns
    -------
    tuple[float, float, float, float]
        (xmin, ymin, xmax, ymax) in dataset_crs.
    """
    from shapely.geometry import box

    xmin, ymin, xmax, ymax = bbox
    geom = gpd.GeoSeries([box(xmin, ymin, xmax, ymax)], crs=bbox_crs)
    # densify edges so reprojected curved edges are fully contained
    max_seg = max(xmax - xmin, ymax - ymin) / 100
    if max_seg > 0:
        try:
            geom = geom.segmentize(max_seg)
        except AttributeError:
            # older geopandas/shapely without segmentize -> corners only
            pass
    reprojected_bounds = geom.to_crs(dataset_crs).total_bounds
    xmin_r, ymin_r, xmax_r, ymax_r = reprojected_bounds
    return (float(xmin_r), float(ymin_r), float(xmax_r), float(ymax_r))


def bbox_contains(outer, inner) -> bool:
    """Return True if axis-aligned ``outer`` fully contains ``inner``.

    Both are (xmin, ymin, xmax, ymax) in the *same* CRS. Used to check that a
    user-provided --bbox covers the subset catchments before downloading the raw
    grid: a bbox smaller than the catchments is rejected because the
    catchment-averaged forcings.nc would be incomplete (clipping the raw grid
    below the catchment extent is not supported yet).
    """
    return (
        outer[0] <= inner[0]
        and outer[1] <= inner[1]
        and outer[2] >= inner[2]
        and outer[3] >= inner[3]
    )


@temp_cluster
def save_dataset(
    ds_to_save: xr.Dataset,
    target_path: Path,
    engine: Literal["netcdf4", "scipy"] = "netcdf4",
):
    """
    Helper function to compute and save an xarray.Dataset (specifically, the raw
    forcing data) to a NetCDF file.
    Uses a temporary file and rename for atomicity.
    """
    if not target_path.parent.exists():
        target_path.parent.mkdir(parents=True, exist_ok=True)

    temp_file_path = target_path.with_name(target_path.name + ".saving.nc")
    if temp_file_path.exists():
        os.remove(temp_file_path)

    client = Client.current()
    future: Future = client.compute(
        ds_to_save.to_netcdf(temp_file_path, engine=engine, compute=False)
    )  # type: ignore
    logger.debug(
        f"NetCDF write task submitted to Dask. Waiting for completion to {temp_file_path}..."
    )
    logger.info("For more detailed progress, see the Dask dashboard http://localhost:8787/status")
    progress(future)
    future.result()
    os.rename(str(temp_file_path), str(target_path))
    logger.info(f"Successfully saved data to: {target_path}")


@no_cluster
def save_to_cache(stores: xr.Dataset, cached_nc_path: Path) -> xr.Dataset:
    """
    Compute the store and save it to a cached netCDF file. This is not required but will save time and bandwidth.
    """
    logger.debug(f"Processing dataset for caching. Final cache target: {cached_nc_path}")

    # lasily cast all numbers to f32
    for name, var in stores.data_vars.items():
        if np.issubdtype(var.dtype, np.number):
            stores[name] = var.astype("float32", casting="same_kind")

    # save dataset locally before manipulating it
    save_dataset(stores, cached_nc_path)

    stores = xr.open_mfdataset(cached_nc_path, parallel=True, engine="netcdf4")
    return stores


def check_local_cache(
    cached_nc_path: Path,
    start_time: str,
    end_time: str,
    gdf: gpd.GeoDataFrame,
    remote_dataset: xr.Dataset,
    bounds: Optional[
        Tuple[float, float, float, float] | np.ndarray[tuple[int], np.dtype[np.float64]]
    ] = None,
) -> Union[xr.Dataset, None]:
    merged_data = None

    if bounds is None:
        bounds = gdf.total_bounds

    if not os.path.exists(cached_nc_path):
        logger.info("No cache found")
        return

    logger.info("Found cached nc file")
    # open the cached file and check that the time range is correct
    try:
        cached_data = xr.open_mfdataset(cached_nc_path, parallel=True, engine="netcdf4")
    except:
        logger.info("Cache produced with outdated backend, redownloading")
        return

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

    # spatial extent check: make sure the cache actually covers the requested
    # bounds. Without this, a smaller cached extent would be silently reused and
    # re-clipped, returning less data than asked for (e.g. after switching to a
    # larger --bbox). One grid cell of tolerance accounts for the padding added
    # when the cache was written.
    try:
        tol_x = abs(float(cached_data.x.values[1] - cached_data.x.values[0]))
        tol_y = abs(float(cached_data.y.values[1] - cached_data.y.values[0]))
    except (IndexError, ValueError):
        tol_x = tol_y = 0.0
    cx_min, cx_max = float(cached_data.x.min()), float(cached_data.x.max())
    cy_min, cy_max = float(cached_data.y.min()), float(cached_data.y.max())
    if (
        bounds[0] < cx_min - tol_x
        or bounds[2] > cx_max + tol_x
        or bounds[1] < cy_min - tol_y
        or bounds[3] > cy_max + tol_y
    ):
        logger.warning("Cached data does not cover requested bounds, redownloading")
        return

    if range_in_cache:
        logger.info("Time range is within cached data")
        logger.debug(f"Opened cached nc file: [{cached_nc_path}]")
        merged_data = clip_dataset_to_bounds(cached_data, bounds, start_time, end_time)
        logger.debug("Clipped stores")

    return merged_data


def save_and_clip_dataset(
    dataset: xr.Dataset,
    gdf: gpd.GeoDataFrame,
    start_time: datetime,
    end_time: datetime,
    cache_location: Path,
    bounds: Optional[
        Tuple[float, float, float, float] | np.ndarray[tuple[int], np.dtype[np.float64]]
    ] = None,
) -> xr.Dataset:
    """convenience function clip the remote dataset, and either load from cache or save to cache if it's not present

    If ``bounds`` (xmin, ymin, xmax, ymax, in the dataset CRS) is given it is used
    to clip the raw gridded data instead of the catchment ``gdf.total_bounds``.
    This is how a larger / predefined extent is downloaded, e.g. to leave room for
    storm transposition. When ``bounds`` is None the original behaviour (clip to
    the subset catchments) is preserved.
    """
    gdf = gdf.to_crs(dataset.crs)
    if bounds is None:
        bounds = gdf.total_bounds

    cached_data = check_local_cache(
        cache_location,
        start_time,  # type: ignore
        end_time,  # type: ignore
        gdf,
        dataset,
        bounds,
    )

    if not cached_data:
        clipped_data = clip_dataset_to_bounds(
            dataset,
            bounds,
            start_time,  # type: ignore
            end_time,  # type: ignore
        )
        cached_data = save_to_cache(clipped_data, cache_location)
    return cached_data
