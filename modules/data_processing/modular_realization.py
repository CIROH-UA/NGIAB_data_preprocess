import json
import copy
from pathlib import Path
from rich.prompt import Prompt
from datetime import datetime

from data_processing.file_paths import FilePaths

accepted_models = [
    "cfe",
    "casam",
    "sft",
    "smp",
    "topmodel",
    "nom",
    "pet",
    "snow17",
    "sacsma",
    "lstm",
    "lstm_rust",
    "dhbv2",
    "dhbv2_daily",
    "summa",
    "sloth",
]

MODEL_DEPENDENCY_RULES = (
    ("cfe", lambda models: "sloth" not in models, "CFE requires SLoTH"),
    ("casam", lambda models: "sloth" not in models, "CASAM requires SLoTH"),
    ("sft", lambda models: "sloth" not in models, "SFT requires SLoTH"),
    (
        "smp",
        lambda models: "sloth" not in models
        and ("casam" not in models or "cfe" not in models or "topmodel" not in models),
        "SMP requires SLoTH or CASAM, CFE, and TOPMODEL",
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
)

all_variable_names_maps = {
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
    "sft": {
        "ground_temperature": "sloth_ground_temperature",
        "soil_moisture_profile": "sloth_soil_moisture_profile",
    },
    "smp": {
        "soil_storage": "sloth_soil_storage",
        "soil_storage_change": "sloth_soil_storage_change",
        "num_wetting_fronts": "sloth_num_wetting_fronts",
        "soil_moisture_wetting_fronts": "sloth_soil_moisture_wetting_fronts",
        "soil_depth_wetting_fronts": "sloth_soil_depth_wetting_fronts",
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

all_sloth_model_params = {
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
}

model_paths = {
    "cfe": FilePaths.cfe_modular_config,
    # "casam": FilePaths.casam_modular_config,
    # "sft": FilePaths.sft_modular_config,
    # "smp": FilePaths.smp_modular_config,
    # "topmodel": FilePaths.topmodel_modular_config,
    "nom": FilePaths.nom_modular_config,
    # "pet": FilePaths.pet_modular_config,
    "sloth": FilePaths.sloth_modular_config,
}

MAIN_OUTPUT_VARIABLES = {
    "cfe": "Q_OUT",
    "pet": "water_potential_evaporation_flux",
    "sft": "num_cells",
    "smp": "soil_storage",
    "topmodel": "Qout",
    "nom": "EVAPOTRANS",
    "snow17": "raim",
    "sac-sma": "tci",
}

MODEL_VARIABLE_OVERRIDES = {
    "cfe": [
        (
            "nom",
            {
                "atmosphere_water__liquid_equivalent_precipitation_rate": "QINSUR",
                "water_potential_evaporation_flux": "EVAPOTRANS",
            },
        ),
        (
            "pet",
            {"water_potential_evaporation_flux": "water_potential_evaporation_flux"},
        ),
        (
            "sft",
            {
                "ice_fraction_schaake": "ice_fraction_schaake",
                "ice_fraction_xinanjiang": "ice_fraction_xinanjiang",
            },
        ),
        (
            "smp",
            {"soil_moisture_profile": "soil_moisture_profile"},
        ),
    ],
    "casam": [
        (
            "nom",
            {"potential_evapotranspiration_rate": "EVAPOTRANS"},
        ),
        (
            "pet",
            {"potential_evapotranspiration_rate": "water_potential_evaporation_flux"},
        ),
        (
            "sft",
            {"soil_temperature_profile": "soil_temperature_profile"},
        ),
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
        (
            "pet",
            {"water_potential_evaporation_flux": "water_potential_evaporation_flux"},
        ),
    ],
    "sac-sma": [
        ("nom", {"pet": "EVAPOTRANS"}),
        ("pet", {"pet": "water_potential_evaporation_flux"}),
    ],
}


def _main_output_variable(main_model: str, routing: bool) -> str | None:
    if main_model == "casam":
        return "surface_runoff" if routing else "total_discharge"
    return MAIN_OUTPUT_VARIABLES.get(main_model)


def _filtered_variable_names(models: list[str]) -> dict[str, dict[str, str]]:
    return {
        model: copy.deepcopy(all_variable_names_maps[model])
        for model in models
        if model in all_variable_names_maps
    }


def _apply_model_overrides(
    model: str,
    target_variable_names: dict[str, dict[str, str]],
    seen_models: list[str],
) -> None:
    for dependency, overrides in MODEL_VARIABLE_OVERRIDES.get(model, []):
        if dependency in seen_models:
            target_variable_names[model].update(overrides)


def _load_json_realization(path: Path | str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _build_sloth_model_params(
    target_variable_names: dict[str, dict[str, str]]
) -> dict[str, float]:
    params: dict[str, float] = {}
    for model_vars in target_variable_names.values():
        for varname in model_vars.values():
            if varname in all_sloth_model_params:
                params[varname + all_sloth_model_params[varname]] = 0.0
    return params


def _append_model_realization(
    model: str,
    target_variable_names: dict[str, dict[str, str]],
    modules: list[dict],
) -> None:
    if model == "cfe":
        realization = _load_json_realization(model_paths["cfe"])
        realization["params"]["variable_names_map"] = target_variable_names["cfe"]
        modules.append(realization)
    elif model == "nom":
        modules.append(_load_json_realization(model_paths["nom"]))


def _insert_sloth_module(
    models: list[str], target_variable_names: dict[str, dict[str, str]], modules: list[dict]
) -> None:
    params = _build_sloth_model_params(target_variable_names)
    sloth_position = models.index("sloth")
    sloth_realization = _load_json_realization(model_paths["sloth"])
    sloth_realization["params"]["model_params"] = params
    modules.insert(sloth_position, sloth_realization)


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

    if any(model not in accepted_models for model in models):
        invalid_models = [model for model in models if model not in accepted_models]
        raise ValueError(
            f"Invalid models specified: {invalid_models}. Accepted models are: {accepted_models}"
        )

    main_model = models[-1]

    # checks model dependencies
    warnings = []
    warnings.extend(
        message
        for model_name, predicate, message in MODEL_DEPENDENCY_RULES
        if model_name == main_model and predicate(models)
    )

    # Check that a rainfall-runoff model is used when routing is on
    if routing and not any(
        model in models
        for model in [
            "cfe",
            "casam",
            "topmodel",
            "sacsma",
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


# Build configs using existing code
# Build realization

def create_modular_configs(
    output_folder: str,
    models: list[str],
    gage_id: str | None = None,
    use_nwm_gw: bool = False,
):
    pass

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
    main_output_variable = _main_output_variable(models[-1], routing)
    target_variable_names = _filtered_variable_names(models)
    modules: list[dict] = []
    seen_models: list[str] = []

    for model in models:
        if model in target_variable_names:
            _apply_model_overrides(model, target_variable_names, seen_models)
        _append_model_realization(model, target_variable_names, modules)
        seen_models.append(model)

    if "sloth" in models:
        _insert_sloth_module(models, target_variable_names, modules)

    realization = _load_json_realization(FilePaths.modular_template)
    realization["global"]["formulations"][0]["params"]["main_output_variable"] = (
        main_output_variable
    )
    realization["global"]["formulations"][0]["params"]["modules"] = modules
    realization["time"]["start_time"] = start_time
    realization["time"]["end_time"] = end_time

    if routing:
        realization["routing"] = {"t_route_config_file_with_path": "./config/troute.yaml"}

    with open(paths.config_dir / "realization.json", "w", encoding="utf-8") as f:
        json.dump(realization, f, indent=4)
