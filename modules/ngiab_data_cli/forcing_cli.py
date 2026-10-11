import argparse
import logging
import shutil
import time
from datetime import datetime
from pathlib import Path

import geopandas as gpd
from data_processing.dask_utils import set_n_workers, shutdown_cluster
from data_processing.dataset_utils import (
    bbox_contains,
    check_local_cache,
    clip_dataset_to_bounds,
    reproject_bbox,
    save_to_cache,
)
from data_processing.datasets import load_aorc_zarr, load_v3_retrospective_zarr
from data_processing.forcings import compute_zonal_stats
from data_sources.source_validation import validate_all
from ngiab_data_cli.custom_logging import setup_logging

# Constants
DATE_FORMAT = "%Y-%m-%d"  # used for datetime parsing
DATE_FORMAT_HINT = "YYYY-MM-DD"  # printed in help message


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )
    parser.add_argument(
        "-i",
        "--input_file",
        type=Path,
        help="path to the input hydrofabric geopackage",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_file",
        type=Path,
        help="path to the forcing output file, e.g. /path/to/forcings.nc",
        required=True,
    )
    parser.add_argument(
        "--start_date",
        "--start",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"Start date for forcings/realization (format {DATE_FORMAT_HINT})",
        required=True,
    )
    parser.add_argument(
        "--end_date",
        "--end",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"End date for forcings/realization (format {DATE_FORMAT_HINT})",
        required=True,
    )
    parser.add_argument(
        "--source",
        type=str,
        help="source of the data",
        choices=["aorc", "nwm"],
        default="nwm",
    )
    parser.add_argument(
        "-D",
        "--debug",
        action="store_true",
        help="enable debug logging",
    )
    parser.add_argument(
        "--dask-workers",
        type=int,
        default=None,
        help="Number of Dask workers for forcings/data processing (default: auto)",
    )
    parser.add_argument(
        "--bbox",
        type=float,
        nargs=4,
        metavar=("XMIN", "YMIN", "XMAX", "YMAX"),
        default=None,
        help=(
            "Custom bounding box for the RAW gridded forcing download instead of the "
            "geopackage's catchment bounds. Four numbers: XMIN YMIN XMAX YMAX, given in "
            "--bbox_crs (default EPSG:4326 lon/lat), reprojected internally to the dataset "
            "CRS. Must fully contain the catchments. Intended for storm transposition: pad "
            "the catchment box so displaced storms stay inside the downloaded grid."
        ),
    )
    parser.add_argument(
        "--bbox_crs",
        type=str,
        default="EPSG:4326",
        help="CRS of the --bbox coordinates (default: EPSG:4326). Reprojected to the dataset CRS internally.",
    )

    return parser.parse_args()


def main() -> None:
    time.sleep(0.01)
    setup_logging()
    validate_all()
    args = parse_arguments()

    if args.dask_workers:
        set_n_workers(args.dask_workers)

    gdf = gpd.read_file(args.input_file, layer="divides")
    logging.debug(f"gdf  bounds: {gdf.total_bounds}")

    start_time = args.start_date.strftime("%Y-%m-%d %H:%M")
    end_time = args.end_date.strftime("%Y-%m-%d %H:%M")

    cached_nc_path = args.output_file.parent / (args.input_file.stem + "-raw-gridded-data.nc")
    print(cached_nc_path)
    if args.source == "aorc":
        data = load_aorc_zarr(args.start_date.year, args.end_date.year)
    elif args.source == "nwm":
        data = load_v3_retrospective_zarr()

    gdf = gdf.to_crs(data.crs)

    bounds = gdf.total_bounds
    if args.bbox:
        bounds = reproject_bbox(tuple(args.bbox), args.bbox_crs, data.crs)
        logging.info(
            f"Using predefined bbox for raw forcing download: {tuple(args.bbox)} "
            f"[{args.bbox_crs}] -> {bounds} [{data.crs}]"
        )
        if not bbox_contains(bounds, gdf.total_bounds):
            raise ValueError(
                "The provided --bbox is smaller than the catchments in the geopackage and "
                "does not fully contain them. A bbox smaller than the catchments is not "
                "supported (the catchment-averaged forcings would be incomplete). Enlarge "
                "--bbox so it covers the catchment bounding box.\n"
                f"  catchment bounds [{data.crs}]: "
                f"{tuple(round(float(b), 2) for b in gdf.total_bounds)}\n"
                f"  provided --bbox  [{data.crs}]: "
                f"{tuple(round(float(b), 2) for b in bounds)}"
            )

    cached_data = check_local_cache(cached_nc_path, start_time, end_time, gdf, data, bounds)

    if not cached_data:
        clipped_data = clip_dataset_to_bounds(data, bounds, start_time, end_time)
        cached_data = save_to_cache(clipped_data, cached_nc_path)

    forcing_working_dir = args.output_file.parent / (args.input_file.stem + "-working-dir")
    if not forcing_working_dir.exists():
        forcing_working_dir.mkdir(parents=True, exist_ok=True)

    temp_dir = forcing_working_dir / "temp"
    if not temp_dir.exists():
        temp_dir.mkdir(parents=True, exist_ok=True)

    compute_zonal_stats(gdf, cached_data, forcing_working_dir)

    shutil.copy(forcing_working_dir / "forcings.nc", args.output_file)
    logging.info(f"Created forcings file: {args.output_file}")
    # remove the working directory
    shutil.rmtree(forcing_working_dir)

    shutdown_cluster()


if __name__ == "__main__":
    main()
