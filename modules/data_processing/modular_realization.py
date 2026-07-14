"""Placeholder module to generate modular realizations."""

import copy
import json
from datetime import datetime
import subprocess
from rich.prompt import Prompt

from data_processing.file_paths import FilePaths
from data_processing.create_realization import (
    get_model_attributes,
    make_cfe_config,
    make_noahowp_config,
    make_snow17_config,
    make_sacsma_config,
    make_lstm_config,
    make_dhbv2_config,
    make_casam_config,
    configure_troute,
)

# ---------------------------------------------------------------------------
# ACCEPTED MODELS
# ---------------------------------------------------------------------------

ACCEPTED_MODELS = [
    "cfe",
    "casam",
    "sft",
    "smp",
    "topmodel",
    "nom",
    "pet",
    "snow17",
    "sac-sma",
    "lstm",
    "lstm_rust",
    "dhbv2",
    "dhbv2_daily",
    "summa",
    "sloth",
]

MAIN_OUTPUT_VARIABLES = {
    # --- Streamflow → T-Route ---
    "cfe": "Q_OUT",  # GIUH + Nash cascade + baseflow (m/timestep)
    "casam": "total_discharge",  # Green-Ampt infiltration excess (m/timestep)
    "topmodel": "Qout",  # Accumulated discharge (m/timestep)
    "sac-sma": "tci",  # Total channel inflow (m)
    "summa": "land_surface_water__runoff_volume_flux",  # Total runoff flux
    "lstm": "land_surface_water__runoff_volume_flux",  # ML streamflow
    "lstm_rust": "land_surface_water__runoff_volume_flux",  # ML streamflow
    "dhbv2": "streamflow",  # ML streamflow (hourly)
    "dhbv2_daily": "streamflow",  # ML streamflow (daily)
    # --- Intermediate → downstream BMI models ---
    "pet": "water_potential_evaporation_flux",  # → CFE/CASAM/TOPMODEL/SAC-SMA
    "sft": "num_cells",  # Soil column layer count; ice fractions via get_value()
    "smp": "soil_storage",  # Scalar (CFE/TOPMODEL) or wetting fronts (CASAM)
    "nom": "EVAPOTRANS",  # ET → CFE/CASAM/TOPMODEL; also outputs QINSUR
    "snow17": "raim",  # Rain + snowmelt (mm/s) → CFE/CASAM/SAC-SMA
}

# ---------------------------------------------------------------------------
# ALL_VARIABLE_NAMES_MAPS
# Default variables_names_map per model assuming NO coupling.
# Left:  model's internal BMI input name (from get_input_var_names())
# Right: forcing column name or sloth_* prefixed default
# Only models that appear in a bmi_multi variables_names_map are included.
# When coupled, MODEL_VARIABLE_OVERRIDES supersede relevant entries.
# ---------------------------------------------------------------------------

ALL_VARIABLES_NAMES_MAPS = {
    "cfe": {
        "atmosphere_water__liquid_equivalent_precipitation_rate": "APCP_surface",
        "water_potential_evaporation_flux": "sloth_pet",
        "ice_fraction_schaake": "sloth_ice_fraction_schaake",
        "ice_fraction_xinanjiang": "sloth_ice_fraction_xinanjiang",
        "soil_moisture_profile": "sloth_soil_moisture_profile",
    },
    "casam": {
        "precipitation_rate": "precip_rate",
        "potential_evapotranspiration_rate": "sloth_pet",
        "soil_temperature_profile": "sloth_soil_temperature_profile",
    },
    "nom": {
        "PRCPNONC": "precip_rate",
        "Q2": "SPFH_2maboveground",
        "SFCTMP": "TMP_2maboveground",
        "UU": "UGRD_10maboveground",
        "VV": "VGRD_10maboveground",
        "LWDN": "DLWRF_surface",
        "SOLDN": "DSWRF_surface",
        "SFCPRS": "PRES_surface",
    },
    "pet": {"water_potential_evaporation_flux": "potential_evapotranspiration"},
    "sft": {
        "ground_temperature": "sloth_ground_temperature",
        "soil_moisture_profile": "sloth_soil_moisture_profile",
    },
    "smp": {
        # CFE/TOPMODEL mode — scalar storage
        "soil_storage": "sloth_soil_storage",
        "soil_storage_change": "sloth_soil_storage_change",
        # CASAM/layered mode — wetting fronts
        "num_wetting_fronts": "sloth_num_wetting_fronts",
        "soil_moisture_wetting_fronts": "sloth_soil_moisture_wetting_fronts",
        "soil_depth_wetting_fronts": "sloth_soil_depth_wetting_fronts",
        # TOPMODEL mode
        "Qb_topmodel": "sloth_Qb_topmodel",
        "Qv_topmodel": "sloth_Qv_topmodel",
        "global_deficit": "sloth_global_deficit",
    },
    "topmodel": {
        "atmosphere_water__liquid_equivalent_precipitation_rate": "APCP_surface",
        "water_potential_evaporation_flux": "sloth_pet",
    },
    "sac-sma": {"tair": "TMP_2maboveground", "precip": "precip_rate", "pet": "sloth_pet"},
}

# ---------------------------------------------------------------------------
# ALL_SLOTH_MODEL_PARAMS
# SLoTH model_params used to bootstrap variables at t=0 when upstream
# models are absent. Format: { param_name: "(size,type,unit,location)" }
# ---------------------------------------------------------------------------

ALL_SLOTH_MODEL_PARAMS = {
    "sloth_ice_fraction_schaake": "(1,double,m,node)",
    "sloth_ice_fraction_xinanjiang": "(1,double,1,node)",
    "sloth_soil_moisture_profile": "(1,double,1,node)",
    "sloth_soil_temperature_profile": "(1,double,K,node)",
    "sloth_soil_storage": "(1,double,m,node)",
    "sloth_soil_storage_change": "(1,double,m,node)",
    "sloth_num_wetting_fronts": "(1,double,1,node)",
    "sloth_soil_moisture_wetting_fronts": "(1,double,1,node)",
    "sloth_soil_depth_wetting_fronts": "(1,double,m,node)",
    "sloth_Qb_topmodel": "(1,double,m h^-1,node)",
    "sloth_Qv_topmodel": "(1,double,m h^-1,node)",
    "sloth_global_deficit": "(1,double,m,node)",
    "sloth_pet": "(1,double,m s-1,node)",
    "sloth_ground_temperature": "(1,double,K,node)",
}

# ---------------------------------------------------------------------------
# MODEL DEPENDENCY RULES
# Format: (model, violation_lambda, message)
# Lambda receives the full module list and returns True when VIOLATED.
#
# Read as: ("target_model", lambda models: <condition that means rule is broken>, "warning")
# ---------------------------------------------------------------------------

MODEL_DEPENDENCY_RULES = (
    # SLoTH required — bootstraps defaults for any model needing inter-model vars at t=0
    ("cfe", lambda models: "sloth" not in models, "CFE requires SLoTH"),
    ("casam", lambda models: "sloth" not in models, "CASAM requires SLoTH"),
    ("sft", lambda models: "sloth" not in models, "SFT requires SLoTH"),
    ("smp", lambda models: "sloth" not in models, "SMP requires SLoTH"),
    (
        "smp",
        lambda models: len([m for m in models if m != "sloth"]) > 1
        and not any(m in models for m in ("casam", "cfe", "topmodel")),
        "SMP requires one of: CASAM, CFE, or TOPMODEL when coupled",
    ),
    (
        "topmodel",
        lambda models: "sloth" not in models and "pet" not in models and "nom" not in models,
        "TOPMODEL requires SLoTH, NOM, or PET",
    ),
    (
        "sac-sma",
        lambda models: "sloth" not in models and "pet" not in models and "nom" not in models,
        "SAC-SMA requires SLoTH, NOM, or PET",
    ),
    # Snow17 must have a downstream runoff model (unless standalone with only SLoTH)
    (
        "snow17",
        lambda models: len([m for m in models if m != "sloth"]) > 1
        and not any(m in models for m in ("cfe", "casam", "topmodel", "sac-sma")),
        "Snow17 requires a downstream runoff model (CFE, CASAM, TOPMODEL, or SAC-SMA)",
    ),
    # CFE: NOM and Snow17 are mutually exclusive precip sources
    (
        "cfe",
        lambda models: "nom" in models and "snow17" in models,
        "CFE cannot have both NOM and Snow17 as precip sources",
    ),
    # NOM with no runoff model is likely misconfigured (unless standalone)
    (
        "nom",
        lambda models: len([m for m in models if m != "sloth"]) > 1
        and not any(m in models for m in ("cfe", "casam", "topmodel", "sac-sma")),
        "NOM is present but no runoff model (CFE, CASAM, TOPMODEL, SAC-SMA) found",
    ),
    # SFT: flag if coupled but no downstream consumer and no SMP
    (
        "sft",
        lambda models: len([m for m in models if m != "sloth"]) > 1
        and not any(m in models for m in ("cfe", "casam", "smp")),
        "SFT has no downstream consumer (CFE or CASAM) and no SMP",
    ),
    # Standalone models should not couple with physics models
    (
        "lstm",
        lambda models: any(
            m in models for m in ("cfe", "casam", "sft", "smp", "sac-sma", "topmodel")
        ),
        "LSTM is standalone — unexpected coupling with physics models",
    ),
    (
        "lstm_rust",
        lambda models: any(
            m in models for m in ("cfe", "casam", "sft", "smp", "sac-sma", "topmodel")
        ),
        "LSTM-rust is standalone — unexpected coupling with physics models",
    ),
    (
        "dhbv2",
        lambda models: any(
            m in models for m in ("cfe", "casam", "sft", "smp", "sac-sma", "topmodel")
        ),
        "dHBV2 is standalone — unexpected coupling with physics models",
    ),
    (
        "dhbv2_daily",
        lambda models: any(
            m in models for m in ("cfe", "casam", "sft", "smp", "sac-sma", "topmodel")
        ),
        "dHBV2-daily is standalone — unexpected coupling with physics models",
    ),
    (
        "summa",
        lambda models: any(
            m in models for m in ("cfe", "casam", "sft", "smp", "snow17", "pet", "sac-sma")
        ),
        "SUMMA is a full land surface model — unexpected coupling with physics models",
    ),
)

# ---------------------------------------------------------------------------
# MODEL VARIABLE OVERRIDES
# When a model is coupled to an upstream provider, these entries override
# the corresponding entries in ALL_VARIABLE_NAMES_MAPS.
#
# Format: { receiving_model: [ (providing_model, {input_name: source_name}) ] }
# Left:  receiving model's internal BMI input name
# Right: providing model's output variable name
# ---------------------------------------------------------------------------

MODEL_VARIABLE_OVERRIDES = {
    "cfe": [
        (
            "nom",
            {
                "atmosphere_water__liquid_equivalent_precipitation_rate": "QINSUR",
                "water_potential_evaporation_flux": "EVAPOTRANS",
            },
        ),
        ("snow17", {"atmosphere_water__liquid_equivalent_precipitation_rate": "raim"}),
        ("pet", {"water_potential_evaporation_flux": "water_potential_evaporation_flux"}),
        (
            "sft",
            {
                "ice_fraction_schaake": "ice_fraction_schaake",
                "ice_fraction_xinanjiang": "ice_fraction_xinanjiang",
            },
        ),
        ("smp", {"soil_moisture_profile": "soil_moisture_profile"}),
    ],
    "casam": [
        ("nom", {"potential_evapotranspiration_rate": "EVAPOTRANS"}),
        ("snow17", {"precipitation_rate": "raim"}),
        ("pet", {"potential_evapotranspiration_rate": "water_potential_evaporation_flux"}),
        ("sft", {"soil_temperature_profile": "soil_temperature_profile"}),
    ],
    "sft": [
        ("nom", {"ground_temperature": "TGS"}),
        ("smp", {"soil_moisture_profile": "soil_moisture_profile"}),
    ],
    "smp": [
        (
            "casam",
            {
                "num_wetting_fronts": "soil_num_wetting_fronts",
                "soil_moisture_wetting_fronts": "soil_moisture_wetting_fronts",
                "soil_depth_wetting_fronts": "soil_depth_wetting_fronts",
                "soil_storage": "soil_storage",
            },
        ),
        (
            "cfe",
            {
                "soil_storage": "SOIL_STORAGE",
                "soil_storage_change": "SOIL_STORAGE_CHANGE",
            },
        ),
        (
            "topmodel",
            {
                "Qb_topmodel": "land_surface_water__baseflow_volume_flux",
                "Qv_topmodel": "soil_water_root-zone_unsat-zone_top__recharge_volume_flux",
                "global_deficit": "soil_water__domain_volume_deficit",
            },
        ),
    ],
    "topmodel": [
        (
            "nom",
            {
                "atmosphere_water__liquid_equivalent_precipitation_rate": "QINSUR",
                "water_potential_evaporation_flux": "EVAPOTRANS",
            },
        ),
        ("snow17", {"atmosphere_water__liquid_equivalent_precipitation_rate": "raim"}),
        ("pet", {"water_potential_evaporation_flux": "water_potential_evaporation_flux"}),
    ],
    "sac-sma": [
        ("nom", {"pet": "EVAPOTRANS"}),
        ("snow17", {"precip": "raim"}),
        ("pet", {"pet": "water_potential_evaporation_flux"}),
    ],
}

# placeholder dictionary for currently non-existent modularized realization configs
MODEL_PATHS = {
    "cfe": FilePaths.cfe_modular_config,
    "casam": FilePaths.casam_modular_config,
    # "sft": FilePaths.sft_modular_config,
    # "smp": FilePaths.smp_modular_config,
    # "topmodel": FilePaths.topmodel_modular_config,
    "nom": FilePaths.nom_modular_config,
    # "pet": FilePaths.pet_modular_config,
    "sloth": FilePaths.sloth_modular_config,
}


# This function would get called to use the above rules to validate a passed list of models
def validate_models(models: list[str], routing: bool):
    """Check that the specified models are valid and that any dependencies are met. If there are any
    issues, print a warning message and ask the user if they want to proceed anyway.

    Args:
        models (list[str]): List of models to use, in the order they will be executed
        routing (bool): Whether routing is enabled

    Raises:
        ValueError: models is empty
        ValueError: models contains invalid model names
        ValueError: Model dependencies are not met and user chooses not to proceed
    """
    if len(models) == 0:
        raise ValueError("No models specified")

    if any(model not in ACCEPTED_MODELS for model in models):
        invalid_models = [model for model in models if model not in ACCEPTED_MODELS]
        raise ValueError(
            f"Invalid models specified: {invalid_models}. Accepted models are: {ACCEPTED_MODELS}"
        )

    # checks model dependencies
    warnings = []
    warnings.extend(
        message
        for model_name, predicate, message in MODEL_DEPENDENCY_RULES
        if model_name in models and predicate(models)
    )

    # Check that a rainfall-runoff model is used when routing is on
    if routing and not any(
        model in models
        for model in [
            "cfe",
            "casam",
            "topmodel",
            "sac-sma",
            "lstm",
            "lstm_rust",
            "dhbv2",
            "dhbv2_daily",
            "summa",
        ]
    ):
        warnings.append("Routing is on but no rainfall-runoff model is used")

    if len(warnings) > 0:
        warning_message = "Model configuration warnings:\n" + "\n".join(warnings)
        print(warning_message)

        response = Prompt.ask(
            "Run anyway? (y/n)",
            default="n",
            choices=["y", "n"],
        )
        if response == "n":
            raise ValueError("Model configuration invalid: " + warning_message)
        print("Proceeding with data preprocessing despite warnings: " + warning_message)


def _append_model_realization(
    model: str,
    target_variable_names: dict[str, dict[str, str]],
    modules: list[dict],
) -> None:
    if model == "cfe":
        with open(MODEL_PATHS["cfe"], "r", encoding="utf-8") as f:
            realization = json.load(f)
        realization["params"]["variables_names_map"] = target_variable_names["cfe"]
        modules.append(realization)
    elif model == "nom":
        with open(MODEL_PATHS["nom"], "r", encoding="utf-8") as f:
            realization = json.load(f)
        modules.append(realization)
    elif model == "casam":
        with open(MODEL_PATHS["casam"], "r", encoding="utf-8") as f:
            realization = json.load(f)
        realization["params"]["variables_names_map"] = target_variable_names["casam"]
        modules.append(realization)


def _insert_sloth_module(
    models: list[str], target_variable_names: dict[str, dict[str, str]], modules: list[dict]
) -> None:
    params: dict[str, float] = {}
    for model_vars in target_variable_names.values():
        for varname in model_vars.values():
            if varname in ALL_SLOTH_MODEL_PARAMS:
                params[varname + ALL_SLOTH_MODEL_PARAMS[varname]] = 0.0
    sloth_position = models.index("sloth")
    with open(MODEL_PATHS["sloth"], "r", encoding="utf-8") as f:
        sloth_realization = json.load(f)
    sloth_realization["params"]["model_params"] = params
    modules.insert(sloth_position, sloth_realization)


def create_modular_realization(
    output_folder: str,
    start_time: datetime,
    end_time: datetime,
    models: list[str],
    routing: bool = False,
):
    """Creates a realization file based on the specified models.

    Args:
        output_folder (str): Name of the output folder, usually the cat-id
        start_time (str): Start time of simulation in YYYY-MM-DD HH:MM:SS
        end_time (str): End time of simulation in YYYY-MM-DD HH:MM:SS
        models (list[str]): List of models to be coupled together
        routing (bool, optional): True if t-route is coupled. Defaults to False.
    """

    paths = FilePaths(output_folder)
    main_output_variable = MAIN_OUTPUT_VARIABLES[models[-1]]

    target_variable_names = {}
    for model in models:
        if model in ALL_VARIABLES_NAMES_MAPS:
            target_variable_names[model] = copy.deepcopy(ALL_VARIABLES_NAMES_MAPS[model])

    modules: list[dict] = []
    seen_models: list[str] = []

    for model in list(target_variable_names.keys()):
        # Implicitly this means that if we have something like ["nom", "pet"], then the PET value
        # from the evapotranspiration module will override the PET value from Noah-OWP-M
        for dependency, overrides in MODEL_VARIABLE_OVERRIDES.get(model, []):
            if dependency in seen_models:
                target_variable_names[model].update(overrides)

        _append_model_realization(model, target_variable_names, modules)
        seen_models.append(model)

    if "sloth" in models:
        _insert_sloth_module(models, target_variable_names, modules)

    with open(FilePaths.modular_template, "r", encoding="utf-8") as f:
        realization = json.load(f)
    realization["global"]["formulations"][0]["params"]["main_output_variable"] = (
        main_output_variable
    )
    realization["global"]["formulations"][0]["params"]["modules"] = modules
    realization["time"]["start_time"] = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
    realization["time"]["end_time"] = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")

    if routing:
        realization["routing"] = {"t_route_config_file_with_path": "./config/troute.yaml"}

    with open(paths.config_dir / "realization.json", "w", encoding="utf-8") as f:
        json.dump(realization, f, indent=4)


def create_modular_configs(  # pylint: disable=too-many-arguments, too-many-branches
    output_folder: str,
    start_time: datetime,
    end_time: datetime,
    models: list[str],
    *,
    routing: bool = False,
):
    """Creates a BMI configuration files based on the specified models.

    Args:
        output_folder (str): Name of the output folder, usually the cat-id
        start_time (str): Start time of simulation in YYYY-MM-DD HH:MM:SS
        end_time (str): End time of simulation in YYYY-MM-DD HH:MM:SS
        models (list[str]): List of models to be coupled together
        routing (bool, optional): True if t-route is coupled. Defaults to False.

    Raises:
        NotImplementedError: Raised when user tries to generate a configuration for a model not
            supported by this module yet
    """
    paths = FilePaths(output_folder)
    conf_df = get_model_attributes(paths.geopackage_path)

    for model in models:
        if model == "cfe":
            # currently does not support pulling GW from NWM
            # pretty sure that upstream implementation is broken right now
            make_cfe_config(conf_df, paths, {})
        elif model == "nom":
            make_noahowp_config(paths.config_dir, conf_df, start_time, end_time)
        elif model == "snow17":
            make_snow17_config(paths.config_dir, conf_df, start_time, end_time)
        elif model == "sac-sma":
            make_sacsma_config(paths.config_dir, conf_df, start_time, end_time)
        elif model in ("lstm", "lstm_rust"):
            make_lstm_config(paths.geopackage_path, paths.config_dir)
        elif model == "dhbv2":
            make_dhbv2_config(paths.geopackage_path, paths.config_dir, start_time, end_time)
        elif model == "dhbv2_daily":
            make_dhbv2_config(
                paths.geopackage_path,
                paths.config_dir,
                start_time,
                end_time,
                template_path=FilePaths.template_dhbv2_daily_config,
            )
        elif model == "sloth":
            pass  # no config file needed for SLoTH
        elif model == "casam":
            if "sft" in models:
                sft_coupled = True
            else:
                sft_coupled = False
            make_casam_config(paths.config_dir, conf_df, start_time, end_time, sft_coupled)
        else:
            # config generation not supported for PET, SFT, SMP, TOPMODEL yet
            # SUMMA also needs forcings for config generation, this will get added as a separate
            # PR
            raise NotImplementedError(f"Config generation not yet supported for '{model}'")

    if routing:
        configure_troute(output_folder, paths.config_dir, start_time, end_time)


def main():
    """
    Main function to test the modular realization creation process for CASAM.
    """

    # this section processes forcings and sets up the ngen-run dir
    # it does produce CFE+NOM stuff but that gets ignored because the realization file gets
    # overwritten
    subprocess.run(
        [
            "uv",
            "run",
            "cli",
            "-i",
            "cat-1555522",
            "-sfr",
            "--start",
            "2020-01-01",
            "--end",
            "2020-01-02",
            "--source",
            "aorc",
            "-o",
            "casam_test_output",
        ],
        check=True,
    )

    # set up CASAM modular realization with SLoTH and NOM, and routing enabled
    models = ["sloth", "nom", "casam"]
    routing = True
    start_time = datetime(2020, 1, 1, 0, 0, 0)
    end_time = datetime(2020, 1, 2, 0, 0, 0)
    output_folder = "casam_test_output"

    validate_models(models, routing)
    print(f"Validated models: {models} with routing={routing}")

    create_modular_configs(
        output_folder=output_folder,
        start_time=start_time,
        end_time=end_time,
        models=models,
        routing=routing,
    )
    print(f"Created modular configs for models: {models} with routing={routing}")

    create_modular_realization(
        output_folder=output_folder,
        start_time=start_time,
        end_time=end_time,
        models=models,
        routing=routing,
    )
    print(f"Created modular realization for models: {models} with routing={routing}")


if __name__ == "__main__":
    main()
