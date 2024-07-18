#!/usr/bin/env python3

import json
import typing
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import multiprocessing
import pandas
import yaml
from collections import defaultdict
from math import ceil

from data_processing.file_paths import file_paths


def parse_cfe_parameters(cfe_noahowp_attributes: pandas.DataFrame) -> typing.Dict[str, dict]:
    """Parses parameters from NOAHOWP_CFE DataFrame and returns a dictionary of catchment configurations."""

    catchment_configs = {}
    for _, row in cfe_noahowp_attributes.iterrows():
        d = OrderedDict()
        # static parameters
        d["forcing_file"] = "BMI"
        d["surface_partitioning_scheme"] = "Schaake"

        # ----------------
        # State Parameters
        # ----------------
        d["soil_params.depth"] = "2.0[m]"
        # beta exponent on Clapp-Hornberger (1978) soil water relations
        d["soil_params.b"] = f'{row["bexp_soil_layers_stag=2"]}[]'
        # saturated hydraulic conductivity
        d["soil_params.satdk"] = f'{row["dksat_soil_layers_stag=2"]}[m s-1]'
        # saturated capillary head
        d["soil_params.satpsi"] = f'{row["psisat_soil_layers_stag=2"]}[m]'
        # this factor (0-1) modifies the gradient of the hydraulic head at the soil bottom. 0=no-flow.
        d["soil_params.slop"] = f'{row["slope"]}[m/m]'
        # saturated soil moisture content
        d["soil_params.smcmax"] = f'{row["smcmax_soil_layers_stag=2"]}[m/m]'
        # wilting point soil moisture content
        d["soil_params.wltsmc"] = f'{row["smcwlt_soil_layers_stag=2"]}[m/m]'

        # ---------------------
        # Adjustable Parameters
        # ---------------------
        # optional; defaults to 1.0
        d["soil_params.expon"] = f'{row["gw_Expon"]}[]' if row["gw_Expon"] is not None else "1.0[]"
        # not sure if this is the correct key
        d["soil_params.expon_secondary"] = (
            f'{row["gw_Coeff"]}[]' if row["gw_Coeff"] is not None else "1.0[]"
        )
        # maximum storage in the conceptual reservoir
        d["max_gw_storage"] = f'{row["gw_Zmax"]}[m]' if row["gw_Zmax"] is not None else "0.011[m]"
        # primary outlet coefficient
        d["Cgw"] = "0.0018[m h-1]"
        # exponent parameter (1.0 for linear reservoir)
        d["expon"] = "6.0[]"
        # initial condition for groundwater reservoir - it is the ground water as a
        # decimal fraction of the maximum groundwater storage (max_gw_storage) for the initial timestep
        d["gw_storage"] = "0.05[m/m]"
        # field capacity
        d["alpha_fc"] = "0.33"
        # initial condition for soil reservoir - it is the water in the soil as a
        # decimal fraction of maximum soil water storage (smcmax * depth) for the initial timestep
        d["soil_storage"] = "0.05[m/m]"
        # number of Nash lf reservoirs (optional, defaults to 2, ignored if storage values present)
        d["K_nash"] = "0.03[]"
        # Nash Config param - primary reservoir
        d["K_lf"] = "0.01[]"
        # Nash Config param - secondary reservoir
        d["nash_storage"] = "0.0,0.0"
        # Giuh ordinates in dt time steps
        d["giuh_ordinates"] = "1.00,0.00"

        # ---------------------
        # Time Info
        # ---------------------
        # set to 1 if forcing_file=BMI
        d["num_timesteps"] = "1"
        # prints various debug and bmi info
        d["verbosity"] = "1"
        d["DEBUG"] = "0"
        # Parameter in the surface runoff parameterization
        # (https://mikejohnson51.github.io/hyAggregate/#Routing_Attributes)
        d["refkdt"] = f'{row["refkdt"]}'
        catchment_configs[row["divide_id"]] = d

    return catchment_configs


def make_catchment_configs(base_dir: Path, catchment_configs: pandas.DataFrame) -> None:
    cat_config_dir = base_dir / "cat_config" / "CFE"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for name, conf in catchment_configs.items():
        with open(f"{cat_config_dir}/{name}.ini", "w") as f:
            for k, v in conf.items():
                f.write(f"{k}={v}\n")


def make_noahowp_config(
    base_dir: Path, cfe_atts_path: Path, start_time: datetime, end_time: datetime
) -> None:
    divide_conf_df = pandas.read_csv(cfe_atts_path)
    divide_conf_df.set_index("divide_id", inplace=True)
    start_datetime = start_time.strftime("%Y%m%d%H%M")
    end_datetime = end_time.strftime("%Y%m%d%H%M")
    with open(file_paths.template_noahowp_config(), "r") as file:
        template = file.read()

    cat_config_dir = base_dir / "cat_config" / "NOAH-OWP-M"
    cat_config_dir.mkdir(parents=True, exist_ok=True)

    for divide in divide_conf_df.index:
        with open(cat_config_dir / f"{divide}.input", "w") as file:
            file.write(
                template.format(
                    start_datetime=start_datetime,
                    end_datetime=end_datetime,
                    lat=divide_conf_df.loc[divide, "Y"],
                    lon=divide_conf_df.loc[divide, "X"],
                    terrain_slope=divide_conf_df.loc[divide, "slope_mean"],
                    azimuth=divide_conf_df.loc[divide, "aspect_c_mean"],
                    ISLTYP=divide_conf_df.loc[divide, "ISLTYP"],
                    IVGTYP=divide_conf_df.loc[divide, "IVGTYP"],

                )
            )


def configure_troute(
    wb_id: str, config_dir: Path, start_time: datetime, end_time: datetime
) -> int:
    with open(file_paths.template_troute_config(), "r") as file:
        troute = yaml.safe_load(file)  # Use safe_load for loading

    time_step_size = troute["compute_parameters"]["forcing_parameters"]["dt"]

    network_topology = troute["network_topology_parameters"]
    supernetwork_params = network_topology["supernetwork_parameters"]

    geo_file_path = f"/ngen/ngen/data/config/{wb_id}_subset.gpkg"
    supernetwork_params["geo_file_path"] = geo_file_path

    troute["compute_parameters"]["restart_parameters"]["start_datetime"] = start_time.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    # TODO figure out what ngens doing with the timesteps.
    nts = (end_time - start_time).total_seconds() / time_step_size
    troute["compute_parameters"]["forcing_parameters"]["nts"] = nts
    troute["compute_parameters"]["forcing_parameters"]["max_loop_size"] = nts
    time_step_size = int(troute["compute_parameters"]["forcing_parameters"]["dt"])  # seconds

    number_of_hourly_steps = nts * time_step_size / 3600
    # not setting this will cause troute to output one netcdf file per timestep
    troute["output_parameters"]["stream_output"]["stream_output_time"] = number_of_hourly_steps

    with open(config_dir / "ngen.yaml", "w") as file:
        yaml.dump(troute, file)

    return nts


def make_ngen_realization_json(
    config_dir: Path, start_time: datetime, end_time: datetime, nts: int
) -> None:
    with open(file_paths.template_realization_config(), "r") as file:
        realization = json.load(file)

    realization["time"]["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
    realization["time"]["output_interval"] = 3600
    realization["time"]["nts"] = nts

    with open(config_dir / "realization.json", "w") as file:
        json.dump(realization, file)


def create_realization(wb_id: str, start_time: datetime, end_time: datetime):
    # quick wrapper to get the cfe realization working
    # without having to refactor this whole thing
    paths = file_paths(wb_id)

    # make cfe init config files
    cfe_atts_path = paths.config_dir() / "cfe_noahowp_attributes.csv"
    catchment_configs = parse_cfe_parameters(pandas.read_csv(cfe_atts_path))
    make_catchment_configs(paths.config_dir(), catchment_configs)

    # make NOAH-OWP-Modular config files
    make_noahowp_config(paths.config_dir(), cfe_atts_path, start_time, end_time)

    # make troute config files
    num_timesteps = configure_troute(wb_id, paths.config_dir(), start_time, end_time)

    # create the realization
    make_ngen_realization_json(paths.config_dir(), start_time, end_time, num_timesteps)

    paths.setup_run_folders()


if __name__ == "__main__":
    wb_id = "wb-1643991"
    start_time = datetime(2010, 1, 1, 0, 0, 0)
    end_time = datetime(2010, 1, 2, 0, 0, 0)
    # output_interval = 3600
    # nts = 2592
    create_realization(wb_id, start_time, end_time)
