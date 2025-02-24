from data_sources.source_validation import validate_all
from ngiab_data_cli.custom_logging import setup_logging
from data_processing.forcings import compute_zonal_stats
from data_processing.dataset_utils import check_local_cache, save_to_cache, clip_dataset_to_bounds
from data_processing.datasets import load_aorc_zarr, load_v3_retrospective_zarr
from data_processing.file_paths import file_paths
import argparse
import logging
import time
import xarray as xr
import geopandas as gpd
from datetime import datetime
from pathlib import Path
import shutil

from dask.distributed import Client, LocalCluster

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

    return parser.parse_args()

def main() -> None:
    time.sleep(0.01)
    setup_logging()
    validate_all()
    args = parse_arguments()

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

    cached_data = check_local_cache(cached_nc_path, start_time, end_time, gdf, data)

    if not cached_data:
        clipped_data = clip_dataset_to_bounds(data, gdf.total_bounds, start_time, end_time)
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

    try:
        client = Client.current()
    except ValueError:
        cluster = LocalCluster()
        client = Client(cluster)
        cluster.close()

    # shut down the client and cluster
    client.close()


if __name__ == "__main__":
    main()
