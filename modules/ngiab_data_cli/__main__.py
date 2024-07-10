import argparse
import logging
from typing import List, Optional
from datetime import datetime
from pathlib import Path

from data_processing.file_paths import file_paths
from data_processing.subset import subset
from data_processing.forcings import create_forcings
from data_processing.create_realization import create_realization
from data_sources.source_validation import validate_all


# Constants
DATE_FORMAT = "%Y-%m-%d"
SUPPORTED_FILE_TYPES = {".csv", ".txt"}
WB_ID_PREFIX = "wb-"


def setup_logging() -> None:
    """Set up logging configuration."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )
    parser.add_argument(
        "-i",
        "--input_file",
        type=str,
        help="Path to a csv or txt file containing a list of waterbody IDs",
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

    return parser.parse_args()


def validate_input(args: argparse.Namespace) -> None:
    """Validate input arguments."""
    if not any([args.subset, args.forcings, args.realization]):
        raise ValueError("At least one of --subset, --forcings, or --realization must be set.")

    if args.subset and not args.input_file:
        raise ValueError(
            "Input file or single wb-id is required for subsetting. e.g. -i wb_ids.txt or -i wb-5173"
        )

    if (args.forcings or args.realization) and not (args.start_date and args.end_date):
        raise ValueError(
            "Both --start_date and --end_date are required for forcings generation or realization creation."
        )


def read_waterbody_ids(input_file: Path) -> List[str]:
    """Read waterbody IDs from input file or return single ID."""
    if input_file.stem.startswith(WB_ID_PREFIX):
        return [input_file.stem]

    if not input_file.exists():
        raise FileNotFoundError(f"The file {input_file} does not exist")

    if input_file.suffix not in SUPPORTED_FILE_TYPES:
        raise ValueError(f"Unsupported file type: {input_file.suffix}")

    with input_file.open("r") as f:
        return f.read().splitlines()


def main() -> None:
    setup_logging()

    try:
        args = parse_arguments()
        validate_input(args)

        if args.input_file:
            input_file = Path(args.input_file)
            waterbody_ids = read_waterbody_ids(input_file)
            logging.info(f"Read {len(waterbody_ids)} waterbody IDs from {input_file}")
        else:
            waterbody_ids = []

        wb_id_for_name = args.output_name or (waterbody_ids[0] if waterbody_ids else None)
        if not wb_id_for_name:
            raise ValueError("No waterbody input file or output folder provided.")

        paths = file_paths(wb_id_for_name)
        output_folder = paths.subset_dir()
        output_folder.mkdir(parents=True, exist_ok=True)
        logging.info(f"Using output folder: {output_folder}")

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

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise


if __name__ == "__main__":
    validate_all()
    main()
