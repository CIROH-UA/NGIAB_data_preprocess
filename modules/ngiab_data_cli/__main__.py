import argparse
import logging
from typing import List
from datetime import datetime
from pathlib import Path
import pandas as pd

# Import colorama for cross-platform colored terminal text
from colorama import Fore, Style, init

from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import get_wbid_from_point, get_nex_from_gage_id
from data_processing.subset import subset
from data_processing.forcings import create_forcings
from data_processing.create_realization import create_realization
from data_sources.source_validation import validate_all


# Constants
DATE_FORMAT = "%Y-%m-%d"
SUPPORTED_FILE_TYPES = {".csv", ".txt"}
WB_ID_PREFIX = "wb-"

# Initialize colorama
init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        if record.levelno == logging.DEBUG:
            return f"{Fore.BLUE}{message}{Style.RESET_ALL}"
        if record.levelno == logging.WARNING:
            return f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        if record.name == "root":  # Only color info messages from this script green
            return f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        return message


def setup_logging() -> None:
    """Set up logging configuration with green formatting."""
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def set_logging_to_critical_only() -> None:
    """Set logging to CRITICAL level only."""
    logging.getLogger().setLevel(logging.CRITICAL)
    # Explicitly set Dask's logger to CRITICAL level
    logging.getLogger("distributed").setLevel(logging.CRITICAL)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )
    parser.add_argument(
        "-i",
        "--input_file",
        type=str,
        help="Path to a csv or txt file containing a newline separated list of waterbody IDs, when used with -l, the file should contain lat/lon pairs",
    )
    parser.add_argument(
        "-l",
        "--latlon",
        action="store_true",
        help="Use lat lon instead of wbid, expects a csv with columns 'lat' and 'lon' or \
            comma separated via the cli \n e.g. python -m ngiab_data_cli -i 54.33,-69.4 -l -s",
    )
    parser.add_argument(
        "-g",
        "--gage",
        action="store_true",
        help="Use gage ID instead of wbid, expects a csv with a column 'gage' or 'gage_id' or \
            a single gage ID via the cli \n e.g. python -m ngiab_data_cli -i 01646500 -g -s",
    )
    parser.add_argument(
        "-s",
        "--subset",
        action="store_true",
        help="Subset the hydrofabric to the given waterbody IDs",
    )
    parser.add_argument(
        "-f",
        "--forcings",
        action="store_true",
        help="Generate forcings for the given waterbody IDs",
    )
    parser.add_argument(
        "-r",
        "--realization",
        action="store_true",
        help="Create a realization for the given waterbody IDs",
    )
    parser.add_argument(
        "--start_date",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"Start date for forcings/realization (format {DATE_FORMAT})",
    )
    parser.add_argument(
        "--end_date",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"End date for forcings/realization (format {DATE_FORMAT})",
    )
    parser.add_argument(
        "-o",
        "--output_name",
        type=str,
        help="Name of the subset to be created (default is the first waterbody ID in the input file)",
    )
    parser.add_argument(
        "-D",
        "--debug",
        action="store_true",
        help="enable debug logging",
    )
    return parser.parse_args()


def validate_input(args: argparse.Namespace) -> None:
    """Validate input arguments."""
    if not any([args.subset, args.forcings, args.realization]):
        raise ValueError("At least one of --subset, --forcings, or --realization must be set.")

    if not args.input_file:
        raise ValueError(
            "Input file or single wb-id/gage-id is required. e.g. -i wb_ids.txt or -i wb-5173 or -i 01646500 -g"
        )

    if (args.forcings or args.realization) and not (args.start_date and args.end_date):
        raise ValueError(
            "Both --start_date and --end_date are required for forcings generation or realization creation."
        )

    if args.latlon and args.gage:
        raise ValueError("Cannot use both --latlon and --gage options at the same time.")

    input_file = Path(args.input_file)
    if args.latlon:
        waterbody_ids = get_wb_ids_from_lat_lon(input_file)
    elif args.gage:
        waterbody_ids = get_wb_ids_from_gage_ids(input_file)
    else:
        waterbody_ids = read_waterbody_ids(input_file)
    logging.info(f"Read {len(waterbody_ids)} waterbody IDs from {input_file}")

    wb_id_for_name = args.output_name or (waterbody_ids[0] if waterbody_ids else None)
    if not wb_id_for_name:
        raise ValueError("No waterbody input file or output folder provided.")

    if not args.subset and (args.forcings or args.realization):
        if not file_paths(wb_id_for_name).subset_dir().exists():
            logging.warning(
                "Forcings and realization creation require subsetting at least once. Automatically enabling subset for this run."
            )
            args.subset = True
    return wb_id_for_name, waterbody_ids


def read_csv(input_file: Path) -> List[str]:
    """Read waterbody IDs from a CSV file."""
    # read the first line of the csv file, if it contains a item starting wb_ then that's the column to use
    # if not then look for a column named 'wb_id' or 'waterbody_id' or divide_id
    # if not, then use the first column
    df = pd.read_csv(input_file)
    wb_id_col = None
    for col in df.columns:
        if col.startswith("wb-") and col.lower() != "wb-id":
            wb_id_col = col
            df = df.read_csv(input_file, header=None)
            break
    if wb_id_col is None:
        for col in df.columns:
            if col.lower() in ["wb_id", "waterbody_id", "divide_id"]:
                wb_id_col = col
                break
    if wb_id_col is None:
        raise ValueError(
            "No waterbody IDs column found in the input file: \n\
                         csv expects a single column of waterbody IDs  \n\
                         or a column named 'wb_id' or 'waterbody_id' or 'divide_id'"
        )

    entries = df[wb_id_col].astype(str).tolist()

    if len(entries) == 0:
        raise ValueError("No waterbody IDs found in the input file")

    return df[wb_id_col].astype(str).tolist()


def read_lat_lon_csv(input_file: Path) -> List[str]:
    # read the csv, see if the first line contains lat and lon, if not, check if it's a pair of numeric values
    # if not, raise an error
    df = pd.read_csv(input_file)
    lat_col = None
    lon_col = None
    for col in df.columns:
        if col.lower() == "lat":
            lat_col = col
        if col.lower() == "lon":
            lon_col = col
    if len(df.columns) == 2 and lat_col is None and lon_col is None:
        lat_col = 0
        lon_col = 1
        df = pd.read_csv(input_file, header=None)
    if lat_col is None or lon_col is None:
        raise ValueError(
            "No lat/lon columns found in the input file: \n\
                         csv expects columns named 'lat' and 'lon' or exactly two unnamed columns of lat and lon"
        )
    return df[[lat_col, lon_col]].astype(float).values.tolist()


def read_waterbody_ids(input_file: Path) -> List[str]:
    """Read waterbody IDs from input file or return single ID."""
    if input_file.stem.startswith(WB_ID_PREFIX):
        return [input_file.stem]

    if not input_file.exists():
        raise FileNotFoundError(f"The file {input_file} does not exist")

    if input_file.suffix not in SUPPORTED_FILE_TYPES:
        raise ValueError(f"Unsupported file type: {input_file.suffix}")

    if input_file.suffix == ".csv":
        return read_csv(input_file)

    with input_file.open("r") as f:
        return f.read().splitlines()


def get_wb_ids_from_lat_lon(input_file: Path) -> List[str]:
    """Read waterbody IDs from input file or return single ID."""
    lat_lon_list = []
    if "," in input_file.name:
        coords = input_file.name.split(",")
        lat_lon_list.append(
            get_wbid_from_point({"lat": float(coords[0]), "lng": float(coords[1])})
        )
        return lat_lon_list

    if not input_file.exists():
        raise FileNotFoundError(f"The file {input_file} does not exist")

    if input_file.suffix not in SUPPORTED_FILE_TYPES:
        raise ValueError(f"Unsupported file type: {input_file.suffix}")

    if input_file.suffix == ".csv":
        lat_lon_list = read_lat_lon_csv(input_file)

    converted_coords = []
    for ll in lat_lon_list:
        converted_coords.append(get_wbid_from_point({"lat": ll[0], "lng": ll[1]}))

    return converted_coords


def read_gage_ids(input_file: Path) -> List[str]:
    """Read gage IDs from input file or return single ID."""
    if input_file.stem.isdigit():
        return [input_file.stem]

    if not input_file.exists():
        raise FileNotFoundError(f"The file {input_file} does not exist")

    if input_file.suffix not in SUPPORTED_FILE_TYPES:
        raise ValueError(f"Unsupported file type: {input_file.suffix}")

    if input_file.suffix == ".csv":
        df = pd.read_csv(input_file)
        gage_col = None
        for col in df.columns:
            if col.lower() in ["gage", "gage_id"]:
                gage_col = col
                break
        if gage_col is None:
            raise ValueError("No gage ID column found in the input file")
        return df[gage_col].astype(str).tolist()

    with input_file.open("r") as f:
        return f.read().splitlines()


def get_wb_ids_from_gage_ids(input_file: Path) -> List[str]:
    """Convert gage IDs to waterbody IDs."""
    gage_ids = read_gage_ids(input_file)
    wb_ids = []
    for gage_id in gage_ids:
        nex_id = get_nex_from_gage_id(gage_id)
        wb_id = f"wb-{nex_id[4:]}"  # Replace 'nex-' with 'wb-'
        wb_ids.append(wb_id)
    return wb_ids


def main() -> None:
    setup_logging()

    try:
        args = parse_arguments()
        wb_id_for_name, waterbody_ids = validate_input(args)
        paths = file_paths(wb_id_for_name)
        output_folder = paths.subset_dir()
        output_folder.mkdir(parents=True, exist_ok=True)
        logging.info(f"Using output folder: {output_folder}")

        if args.debug:
            logging.getLogger("data_processing").setLevel(logging.DEBUG)

        if args.subset:
            logging.info(f"Subsetting hydrofabric for {len(waterbody_ids)} waterbody IDs...")
            subset(waterbody_ids, subset_name=wb_id_for_name)
            logging.info("Subsetting complete.")

        if args.forcings:
            logging.info(f"Generating forcings from {args.start_date} to {args.end_date}...")
            create_forcings(
                start_time=args.start_date,
                end_time=args.end_date,
                output_folder_name=wb_id_for_name,
            )
            logging.info("Forcings generation complete.")

        if args.realization:
            logging.info(f"Creating realization from {args.start_date} to {args.end_date}...")
            create_realization(wb_id_for_name, start_time=args.start_date, end_time=args.end_date)
            logging.info("Realization creation complete.")

        logging.info("All requested operations completed successfully.")
        # set logging to ERROR level only as dask distributed can clutter the terminal with INFO messages
        # that look like errors

        set_logging_to_critical_only()

    except Exception as e:
        logging.error(f"{Fore.RED}An error occurred: {str(e)}{Style.RESET_ALL}")
        raise


if __name__ == "__main__":
    validate_all()
    main()
