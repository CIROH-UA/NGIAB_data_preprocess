from typing import Tuple

import rich.status

# add a status bar for these imports so the cli feels more responsive
with rich.status.Status("loading") as status:
    import argparse
    import logging
    import subprocess
    import time
    from multiprocessing import cpu_count
    from pathlib import Path

    import geopandas as gpd
    from data_processing.create_realization import create_lstm_realization, create_realization, create_dhbv2_realization
    from data_processing.dask_utils import shutdown_cluster
    from data_processing.dataset_utils import save_and_clip_dataset
    from data_processing.datasets import load_aorc_zarr, load_v3_retrospective_zarr
    from data_processing.file_paths import FilePaths
    from data_processing.forcings import create_forcings
    from data_processing.gpkg_utils import get_cat_from_gage_id, get_catid_from_point
    from data_processing.graph_utils import get_upstream_cats
    from data_processing.subset import subset, subset_vpu
    from data_sources.source_validation import validate_hydrofabric, validate_output_dir
    from ngiab_data_cli.arguments import parse_arguments
    from ngiab_data_cli.custom_logging import set_logging_to_critical_only, setup_logging


def validate_input(args: argparse.Namespace) -> Tuple[str, str]:
    """Validate input arguments."""

    feature_name = None
    output_folder = None

    if args.vpu:
        if not args.output_name:
            args.output_name = f"vpu-{args.vpu}"
            validate_output_dir()
        return args.vpu, args.output_name

    if args.input_feature:
        input_feature = args.input_feature.replace("_", "-")

        if args.gage and not input_feature.startswith("gage-"):
            input_feature = "gage-" + input_feature

        # look at the prefix for autodetection, if -g or -l is used then there is no prefix
        if len(input_feature.split("-")) > 1:
            prefix = input_feature.split("-")[0]
            if prefix.lower() == "gage":
                args.gage = True
            elif prefix.lower() == "wb":
                logging.warning("Waterbody IDs are no longer supported!")
                logging.warning(f"Automatically converting {input_feature} to catid")
                time.sleep(2)

        # always add or replace the prefix with cat if it is not a lat lon or gage
        if not args.latlon and not args.gage:
            input_feature = "cat-" + input_feature.split("-")[-1]

        if args.latlon and args.gage:
            raise ValueError("Cannot use both --latlon and --gage options at the same time.")

        if args.latlon:
            validate_hydrofabric()
            feature_name = get_cat_id_from_lat_lon(input_feature)
            logging.info(f"Found {feature_name} from {input_feature}")
        elif args.gage:
            validate_hydrofabric()
            feature_name = get_cat_from_gage_id(input_feature)
            logging.info(f"Found {feature_name} from {input_feature}")
        else:
            feature_name = input_feature

        if args.output_name:
            output_folder = args.output_name
            validate_output_dir()
        elif args.gage:
            output_folder = input_feature
            validate_output_dir()
        else:
            output_folder = feature_name
            validate_output_dir()

    return feature_name, output_folder


def get_cat_id_from_lat_lon(input_feature: str) -> str:
    """Read catchment IDs from input file or return single ID."""
    if "," in input_feature:
        coords = input_feature.split(",")
        return get_catid_from_point({"lat": float(coords[0]), "lng": float(coords[1])})
    else:
        raise ValueError("Lat Lon input must be comma separated e.g. -l 54.33,-69.4")


def set_dependent_flags(args, paths: FilePaths):
    # if validate is set, run everything that is missing
    if args.validate:
        logging.info("Running all missing steps required to run ngiab.")
        args = validate_run_directory(args, paths)

    # realization and forcings require subset to have been run at least once
    if args.realization or args.forcings:
        if not paths.subset_dir.exists() and not args.subset:
            logging.warning(
                "Subset required for forcings and realization generation, enabling subset."
            )
            args.subset = True

    if (args.forcings or args.realization) and not (args.start_date and args.end_date):
        raise ValueError(
            "Both --start and --end are required for forcings generation or realization creation. YYYY-MM-DD"
        )

    return args


def validate_run_directory(args, paths: FilePaths):
    # checks the folder that is going to be run, enables steps that are needed to populate the folder
    if not paths.subset_dir.exists():
        logging.info("Subset folder does not exist, enabling subset, forcings, and realization.")
        args.subset = True
        args.forcings = True
        args.realization = True
        return args
    if not paths.forcings_dir.exists():
        logging.info("Forcings folder does not exist, enabling forcings.")
        args.forcings = True
    # this folder only exists if realization generation has run
    cat_config_dir = paths.config_dir / "cat_config"
    if not cat_config_dir.exists():
        logging.info("Realization folder does not exist, enabling realization.")
        args.realization = True
    return args


def main() -> None:
    setup_logging()
    try:
        args = parse_arguments()
        if args.debug:
            logging.getLogger("data_processing").setLevel(logging.DEBUG)

        if args.output_root:
            with open(FilePaths.config_file, "w") as config_file:
                config_file.write(str(Path(args.output_root).expanduser().absolute()))
            logging.info(
                f"Changed default directory where outputs are stored to {args.output_root}"
            )

        feature_to_subset, output_folder = validate_input(args)

        if (feature_to_subset, output_folder) == (
            None,
            None,
        ):  # in case someone just passes an argument to change default output dir
            return

        paths = FilePaths(output_folder)
        args = set_dependent_flags(args, paths)  # --validate
        if feature_to_subset:
            logging.info(f"Processing {feature_to_subset} in {paths.output_dir}")
            if not args.vpu:
                upstream_count = len(get_upstream_cats(feature_to_subset))
                logging.info(f"Upstream catchments: {upstream_count}")
                if upstream_count == 0:
                    # if there are no upstreams, exit
                    logging.error("No upstream catchments found.")
                    return

        if args.subset:
            if args.vpu:
                logging.info(f"Subsetting VPU {args.vpu}")
                subset_vpu(args.vpu, output_gpkg_path=paths.geopackage_path)
                logging.info("Subsetting complete.")
            else:
                logging.info("Subsetting hydrofabric")
                include_outlet = True
                if args.gage:
                    include_outlet = False
                subset(
                    feature_to_subset,
                    output_gpkg_path=paths.geopackage_path,
                    include_outlet=include_outlet,
                )
                logging.info("Subsetting complete.")

        if args.forcings:
            logging.info(f"Generating forcings from {args.start_date} to {args.end_date}...")
            if args.source == "aorc":
                data = load_aorc_zarr(args.start_date.year, args.end_date.year)
            elif args.source == "nwm":
                data = load_v3_retrospective_zarr()
            gdf = gpd.read_file(paths.geopackage_path, layer="divides")
            cached_data = save_and_clip_dataset(
                data, gdf, args.start_date, args.end_date, paths.cached_nc_file
            )

            create_forcings(
                cached_data,
                output_folder_name=output_folder,
                dhbv = args.dhbv2 # need this argument to determine if we do the PET calculation or not
            )
            logging.info("Forcings generation complete.")

        if args.realization:
            logging.info(f"Creating realization from {args.start_date} to {args.end_date}...")
            gage_id = None
            if args.gage:
                gage_id = args.input_feature
                if not gage_id.startswith("gage-"):
                    gage_id = "gage-" + gage_id
            if args.lstm or args.lstm_rust:
                create_lstm_realization(
                    output_folder,
                    start_time=args.start_date,
                    end_time=args.end_date,
                    use_rust=args.lstm_rust,
                )
            if args.dhbv2:
                create_dhbv2_realization(
                    output_folder,
                    start_time = args.start_date,
                    end_time = args.end_date,
                )
            else:
                create_realization(
                    output_folder,
                    start_time=args.start_date,
                    end_time=args.end_date,
                    use_nwm_gw=args.nwm_gw,
                    gage_id=gage_id,
                )
            logging.info("Realization creation complete.")

        if args.run:
            logging.info("Running Next Gen using NGIAB...")

            try:
                subprocess.run("docker pull awiciroh/ciroh-ngen-image:latest", shell=True)
            except:
                logging.error("Docker is not running, please start Docker and try again.")
            try:
                command = f'docker run --rm -it -v "{str(paths.subset_dir)}:/ngen/ngen/data" awiciroh/ciroh-ngen-image:latest /ngen/ngen/data/ auto {cpu_count()} local'
                subprocess.run(command, shell=True)
                logging.info("Next Gen run complete.")
            except:
                logging.error("Next Gen run failed.")

        if args.eval:
            plot = False
            try:
                import matplotlib
                import seaborn

                plot = True
            except ImportError:
                # silently fail as plotting isn't publicly supported
                pass

            try:
                from ngiab_eval import evaluate_folder

                if plot:
                    logging.info("Plotting enabled")
                logging.info("Evaluating model performance...")
                evaluate_folder(paths.subset_dir, plot=plot, debug=args.debug)
            except ImportError:
                logging.error(
                    "Evaluation module not found. Please install the ngiab_eval package to evaluate model performance."
                )
                args.vis = False

        if args.vis:
            try:
                command = f'docker run --rm -it -p 3000:3000 -v "{str(paths.subset_dir)}:/ngen/ngen/data/" joshcu/ngiab_grafana:v0.2.1'
                subprocess.run(command, shell=True)
            except:
                logging.error("Failed to launch docker container.")

        logging.info("All operations completed successfully.")
        logging.info(f"Output folder: file:///{paths.subset_dir}")
        # set logging to ERROR level only as dask distributed can clutter the terminal with INFO messages
        # that look like errors
        set_logging_to_critical_only()

    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise
    shutdown_cluster()


if __name__ == "__main__":
    main()
