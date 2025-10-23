import argparse
from datetime import datetime

# Constants
DATE_FORMAT = "%Y-%m-%d"  # used for datetime parsing
DATE_FORMAT_HINT = "YYYY-MM-DD"  # printed in help message


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )
    group = parser.add_mutually_exclusive_group(required=False)

    parser.add_argument(
        "--output_root",
        type=str,
        help="Path to new default directory where outputs in the future will be stored",
    )

    group.add_argument(
        "-i",
        "--input_feature",
        "--input_file",
        type=str,
        help="ID of feature to subset, providing a prefix will automatically convert to catid, \n e.g. cat-5173 or gage-01646500 or wb-1234",
    )
    group.add_argument(
        "--vpu",
        type=str,
        help="The VPU to subset, e.g. 01",
        choices=[
            "01",
            "02",
            "03",
            "03N",
            "03S",
            "03W",
            "04",
            "05",
            "06",
            "07",
            "08",
            "09",
            "10",
            "10L",
            "10U",
            "11",
            "12",
            "13",
            "14",
            "15",
            "16",
            "17",
            "18",
        ],
    )

    parser.add_argument(
        "-l",
        "--latlon",
        action="store_true",
        help="Use lat lon instead of catid, expects \
            comma separated via the cli \n e.g. python -m ngiab_data_cli -i 54.33,-69.4 -l -s",
    )
    parser.add_argument(
        "-g",
        "--gage",
        action="store_true",
        help="Use gage ID instead of catid, expects a single gage ID via the cli \n e.g. python -m ngiab_data_cli -i 01646500 -g -s",
    )
    parser.add_argument(
        "-s",
        "--subset",
        action="store_true",
        help="Subset the hydrofabric to the given feature",
    )
    parser.add_argument(
        "-f",
        "--forcings",
        action="store_true",
        help="Generate forcings for the given feature",
    )
    parser.add_argument(
        "-r",
        "--realization",
        action="store_true",
        help="Create a realization for the given feature",
    )
    parser.add_argument(
        "--start_date",
        "--start",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"Start date for forcings/realization (format {DATE_FORMAT_HINT})",
    )
    parser.add_argument(
        "--end_date",
        "--end",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"End date for forcings/realization (format {DATE_FORMAT_HINT})",
    )
    parser.add_argument(
        "-o",
        "--output_name",
        type=str,
        help="Custom data output folder name in lieu of the default, which is the ID of the input feature",
    )
    parser.add_argument(
        "-D",
        "--debug",
        action="store_true",
        help="enable debug logging",
    )
    lstm_group = parser.add_mutually_exclusive_group(required=False)
    lstm_group.add_argument(
        "--lstm",
        action="store_true",
        help="enable LSTM model realization and forcings",
    )
    lstm_group.add_argument(
        "--lstm_rust",
        action="store_true",
        help="enable experimental high speed Rust bindings of LSTM model realization and forcings",
    )

    dhbv2_group = parser.add_mutually_exclusive_group(required=False)
    dhbv2_group.add_argument(
        "--dhbv2",
        action="store_true",
        help="enable dHBV2 model realization and forcings",
    )
    
    parser.add_argument(
        "--nwm_gw",
        action="store_true",
        help="use NWM retrospective output groundwater level for CFE initial gw state",
    )
    parser.add_argument(
        "--run", action="store_true", help="Automatically run Next Gen against the output folder"
    )
    parser.add_argument(
        "--validate", action="store_true", help="Run every missing step required to run ngiab"
    )
    parser.add_argument(
        "--eval", action="store_true", help="Evaluate perforance of the model after running"
    )
    parser.add_argument(
        "--vis", "--visualise", action="store_true", help="Visualize the model output"
    )
    parser.add_argument(
        "--source",
        type=str,
        help="source of the data",
        choices=["aorc", "nwm"],
        default="nwm",
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Run all operations: subset, forcings, realization, and run Next Gen",
    )

    args = parser.parse_args()

    if args.all:
        args.subset = True
        args.forcings = True
        args.realization = True
        args.run = True

    if args.vis:
        args.eval = True

    if args.run:
        args.validate = True

    return args
