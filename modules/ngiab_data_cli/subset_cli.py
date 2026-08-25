"""Command line interface for subsetting geopackages by catchment ID."""

import argparse
import logging
import time
from pathlib import Path

from data_processing.subset import subset

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Subsetting hydrofabrics")
    parser.add_argument(
        "--cat_id",
        type=str,
        help="ID of the catchment to subset",
        required=True,
    )
    parser.add_argument(
        "--gpkg_path",
        type=Path,
        help="path to the geopackage to subset",
        required=True,
    )
    parser.add_argument(
        "--output_path",
        type=Path,
        help="path to the output geopackage",
        required=True,
    )

    return parser.parse_args()


def main() -> None:
    """
    Main function to run the subsetting process.
    """
    time.sleep(0.01)
    args = parse_arguments()

    subset(
        cat_ids=args.cat_id,
        hydrofabric=args.gpkg_path,
        output_gpkg_path=args.output_path,
        include_outlet=True,
        override_gpkg=True,
    )

    logger.info(
        "Subset complete for catchment %s from %s. Output saved to %s",
        args.cat_id,
        args.gpkg_path,
        args.output_path,
    )


if __name__ == "__main__":
    main()
