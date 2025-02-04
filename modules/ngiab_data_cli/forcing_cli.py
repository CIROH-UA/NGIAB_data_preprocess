from data_sources.source_validation import validate_all
from ngiab_data_cli.custom_logging import setup_logging
from data_processing.forcings import compute_zonal_stats
from data_processing.zarr_utils import get_forcing_data
from data_processing.file_paths import file_paths
import argparse
import logging
import time
import xarray as xr
import geopandas as gpd
from datetime import datetime
from pathlib import Path
import shutil

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
    projection = xr.open_dataset(file_paths.template_nc, engine="h5netcdf").crs.esri_pe_string
    logging.debug("Got projection from grid file")

    gdf = gpd.read_file(args.input_file, layer="divides").to_crs(projection)
    logging.debug(f"gdf  bounds: {gdf.total_bounds}")

    start_time = args.start_date.strftime("%Y-%m-%d %H:%M")
    end_time = args.end_date.strftime("%Y-%m-%d %H:%M")

    cached_nc_path = args.output_file.parent / (args.input_file.stem + "-raw-gridded-data.nc")
    print(cached_nc_path)
    merged_data = get_forcing_data(cached_nc_path, start_time, end_time, gdf)
    forcing_working_dir = args.output_file.parent / (args.input_file.stem + "-working-dir")
    if not forcing_working_dir.exists():
        forcing_working_dir.mkdir(parents=True, exist_ok=True)

    temp_dir = forcing_working_dir / "temp"
    if not temp_dir.exists():
        temp_dir.mkdir(parents=True, exist_ok=True)


    compute_zonal_stats(gdf, merged_data, forcing_working_dir)

    shutil.copy(forcing_working_dir / "forcings.nc", args.output_file)
    logging.info(f"Created forcings file: {args.output_file}")
    # remove the working directory
    shutil.rmtree(forcing_working_dir)


if __name__ == "__main__":
    main()
