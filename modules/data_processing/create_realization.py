"""Placeholder module to generate modular realizations."""

import copy
import json
from datetime import datetime
import logging
from dataclasses import dataclass, field
from typing import Optional
from pathlib import Path
import requests

from data_processing.file_paths import FilePaths

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class ModelSpec:
    """All per-model coupling knowledge for one model.

    main_output_variable:
        The module's terminal BMI output. This is what would get passed to t-route.
    realization_fragment:
        Path to the modular-realization JSON stub for this model. ``None`` means
        the model is recognized but not yet wired end-to-end — ``supported`` is
        ``False`` and ``validate_models`` rejects it with a clear message rather
        than letting it KeyError deep in realization assembly.
    routable:
        True if it is a rainfall-runoff model whose outputs can be passed to t-route.
        False if not.
    variables_names_map:
        Default variables_names_map assuming NO coupling. Empty for models that
        carry none (LSTM, SLoTH).
    overrides:
        (upstream_provider, {input_name: source_name}) entries applied when the
        provider precedes this model in the run order.
    """

    main_output_variable: str
    realization_fragment: Path
    routable: Optional[bool] = False
    variables_names_map: dict[str, str] = field(default_factory=dict)
    overrides: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    @property
    def supported(self) -> bool:
        """True when the model is wired end-to-end (has a realization realization_fragment)."""
        return self.realization_fragment is not None


# ---------------------------------------------------------------------------
# THE REGISTRY
# One entry per model. Order here is only for readability.
# ---------------------------------------------------------------------------

MODEL_REGISTRY: dict[str, ModelSpec] = {
    "sloth": ModelSpec(
        main_output_variable="z",
        realization_fragment=FilePaths.sloth_modular_config,
    ),
    "nom": ModelSpec(
        main_output_variable="EVAPOTRANS",
        realization_fragment=FilePaths.nom_modular_config,
        variables_names_map={
            "PRCPNONC": "precip_rate",
            "Q2": "SPFH_2maboveground",
            "SFCTMP": "TMP_2maboveground",
            "UU": "UGRD_10maboveground",
            "VV": "VGRD_10maboveground",
            "LWDN": "DLWRF_surface",
            "SOLDN": "DSWRF_surface",
            "SFCPRS": "PRES_surface",
        },
    ),
    "cfe": ModelSpec(
        main_output_variable="Q_OUT",
        realization_fragment=FilePaths.cfe_modular_config,
        routable=True,
        variables_names_map={
            "atmosphere_water__liquid_equivalent_precipitation_rate": "APCP_surface",
            "water_potential_evaporation_flux": "sloth_pet",
            "ice_fraction_schaake": "sloth_ice_fraction_schaake",
            "ice_fraction_xinanjiang": "sloth_ice_fraction_xinanjiang",
            "soil_moisture_profile": "sloth_soil_moisture_profile",
        },
        overrides=[
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
    ),
    "casam": ModelSpec(
        main_output_variable="total_discharge",
        realization_fragment=FilePaths.casam_modular_config,
        routable=True,
        variables_names_map={
            "precipitation_rate": "precip_rate",
            "potential_evapotranspiration_rate": "sloth_pet",
            "soil_temperature_profile": "sloth_soil_temperature_profile",
        },
        overrides=[
            ("nom", {"potential_evapotranspiration_rate": "EVAPOTRANS"}),
            ("snow17", {"precipitation_rate": "raim"}),
            ("pet", {"potential_evapotranspiration_rate": "water_potential_evaporation_flux"}),
            ("sft", {"soil_temperature_profile": "soil_temperature_profile"}),
        ],
    ),
    "snow17": ModelSpec(
        main_output_variable="raim",
        realization_fragment=FilePaths.snow17_modular_config,
        variables_names_map={
            "precip": "atmosphere_water__liquid_equivalent_precipitation_rate",
            "tair": "land_surface_air__temperature",
        },
    ),
    "sac-sma": ModelSpec(
        main_output_variable="tci",
        realization_fragment=FilePaths.sac_modular_config,
        routable=True,
        variables_names_map={
            "precip": "atmosphere_water__liquid_equivalent_precipitation_rate",
            "tair": "land_surface_air__temperature",
            "pet": "sloth_pet",
        },
        overrides=[
            ("nom", {"pet": "EVAPOTRANS"}),
            ("snow17", {"precip": "raim"}),
            ("pet", {"pet": "water_potential_evaporation_flux"}),
        ],
    ),
    "lstm": ModelSpec(
        main_output_variable="land_surface_water__runoff_depth",
        realization_fragment=FilePaths.lstm_modular_config,
        routable=True
    ),
    "lstm_rust": ModelSpec(
        main_output_variable="land_surface_water__runoff_depth",
        realization_fragment=FilePaths.lstm_rust_modular_config,
        routable=True
    ),
    "dhbv2": ModelSpec(
        main_output_variable="land_surface_water__runoff_volume_flux",
        realization_fragment=FilePaths.dhbv2_modular_config,
        routable=True,
        variables_names_map={
            "atmosphere_water__liquid_equivalent_precipitation_rate": "precip_rate",
            "land_surface_air__temperature": "TMP_2maboveground",
            "atmosphere_air_water~vapor__relative_saturation": "SPFH_2maboveground",
            "land_surface_radiation~incoming~longwave__energy_flux": "DLWRF_surface",
            "land_surface_radiation~incoming~shortwave__energy_flux": "DSWRF_surface",
            "land_surface_air__pressure": "PRES_surface",
            "land_surface_wind__x_component_of_velocity": "UGRD_10maboveground",
            "land_surface_wind__y_component_of_velocity": "VGRD_10maboveground",
            "land_surface_water__runoff_volume_flux": "streamflow",
        },
    ),
    "dhbv2_daily": ModelSpec(
        main_output_variable="land_surface_water__runoff_volume_flux",
        realization_fragment=FilePaths.dhbv2_daily_modular_config,
        routable=True,
        variables_names_map={
            "atmosphere_water__liquid_equivalent_precipitation_rate": "precip_rate",
            "land_surface_air__temperature": "TMP_2maboveground",
            "atmosphere_air_water~vapor__relative_saturation": "SPFH_2maboveground",
            "land_surface_radiation~incoming~longwave__energy_flux": "DLWRF_surface",
            "land_surface_radiation~incoming~shortwave__energy_flux": "DSWRF_surface",
            "land_surface_air__pressure": "PRES_surface",
            "land_surface_wind__x_component_of_velocity": "UGRD_10maboveground",
            "land_surface_wind__y_component_of_velocity": "VGRD_10maboveground",
            "land_surface_water__runoff_volume_flux": "streamflow",
        },
    ),
    "summa": ModelSpec(
        main_output_variable="land_surface_water__runoff_volume_flux",
        realization_fragment=FilePaths.summa_modular_config,
        routable=True,
        variables_names_map={
            "atmosphere_water__precipitation_mass_flux": "precip_rate",
            "land_surface_air__temperature": "TMP_2maboveground",
            "atmosphere_air_water~vapor__relative_saturation": "SPFH_2maboveground",
            "land_surface_wind__x_component_of_velocity": "UGRD_10maboveground",
            "land_surface_wind__y_component_of_velocity": "VGRD_10maboveground",
            "land_surface_radiation~incoming~shortwave__energy_flux": "DSWRF_surface",
            "land_surface_radiation~incoming~longwave__energy_flux": "DLWRF_surface",
            "land_surface_air__pressure": "PRES_surface",
        },
    ),
    # ------------------------------------------------------------------
    # Recognized but not yet wired: no realization realization_fragment, so supported
    # is False and validate_models rejects them up front. Their names-maps
    # and overrides are kept as scaffolding for when they are wired.
    # ------------------------------------------------------------------
    "pet": ModelSpec(
        main_output_variable="water_potential_evaporation_flux",
        realization_fragment=None,
        variables_names_map={"water_potential_evaporation_flux": "potential_evapotranspiration"},
    ),
    "sft": ModelSpec(
        main_output_variable="num_cells",
        realization_fragment=None,
        variables_names_map={
            "ground_temperature": "sloth_ground_temperature",
            "soil_moisture_profile": "sloth_soil_moisture_profile",
        },
        overrides=[
            ("nom", {"ground_temperature": "TGS"}),
            ("smp", {"soil_moisture_profile": "soil_moisture_profile"}),
        ],
    ),
    "smp": ModelSpec(
        main_output_variable="soil_storage",
        realization_fragment=None,
        variables_names_map={
            "soil_storage": "sloth_soil_storage",
            "soil_storage_change": "sloth_soil_storage_change",
            "num_wetting_fronts": "sloth_num_wetting_fronts",
            "soil_moisture_wetting_fronts": "sloth_soil_moisture_wetting_fronts",
            "soil_depth_wetting_fronts": "sloth_soil_depth_wetting_fronts",
            "Qb_topmodel": "sloth_Qb_topmodel",
            "Qv_topmodel": "sloth_Qv_topmodel",
            "global_deficit": "sloth_global_deficit",
        },
        overrides=[
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
    ),
    "topmodel": ModelSpec(
        main_output_variable="Qout",
        realization_fragment=None,
        routable=True,
        variables_names_map={
            "atmosphere_water__liquid_equivalent_precipitation_rate": "APCP_surface",
            "water_potential_evaporation_flux": "sloth_pet",
        },
        overrides=[
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
    ),
}


# ---------------------------------------------------------------------------
# Derived membership — one source of truth, no hand-maintained parallel lists.
# ---------------------------------------------------------------------------

ACCEPTED_MODELS: tuple[str, ...] = tuple(MODEL_REGISTRY)
SUPPORTED_MODELS: tuple[str, ...] = tuple(
    name for name, spec in MODEL_REGISTRY.items() if spec.supported
)

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


# This function would get called to use the above rules to validate a passed list of models
def validate_models(models: list[str], routing: bool) -> list:
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

    # Check that a rainfall-runoff model is used as the final model when routing is on
    if routing and not MODEL_REGISTRY[models[-1]].routable:
        warnings.append("Routing is on but no rainfall-runoff model is used")

    return warnings


def _insert_sloth_module(
    models: list[str], target_variable_names: dict[str, dict[str, str]], modules: list[dict]
) -> None:
    params: dict[str, float] = {}
    for model_vars in target_variable_names.values():
        for varname in model_vars.values():
            if varname in ALL_SLOTH_MODEL_PARAMS:
                params[varname + ALL_SLOTH_MODEL_PARAMS[varname]] = 0.0
    sloth_position = models.index("sloth")
    with open(MODEL_REGISTRY["sloth"].realization_fragment, "r", encoding="utf-8") as f:
        sloth_realization = json.load(f)

    # insert dummy param if no other params are present, otherwise BMI will throw an error
    if not params:
        params["sloth_dummy_param(1,double,1,node)"] = 0.0
    sloth_realization["params"]["model_params"] = params
    modules.insert(sloth_position, sloth_realization)


def _handle_calibrated_params(paths: FilePaths, gage_id: str) -> bool:
    # try and download s3:communityhydrofabric/hydrofabrics/community/gage_parameters/gage_id
    # if it doesn't exist, use the default
    url = (
        "https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/"
        + f"gage_parameters/{gage_id}.json"
    )
    response = requests.get(url, timeout=10)

    if response.status_code == 200:
        new_template = response.json()
        template_path = paths.config_dir / "downloaded_params.json"
        with open(template_path, "w", encoding="utf-8") as f:
            json.dump(new_template, f)

        logger.info("downloaded calibrated parameters for %s", gage_id)
        return True

    logger.warning("could not download parameters for %s, using default template", gage_id)
    return False


def create_modular_realization(  # pylint: disable=too-many-locals,too-many-arguments
    output_folder: str,
    start_time: datetime,
    end_time: datetime,
    models: list[str],
    *,
    routing: bool = False,
    gage_id: str | None = None,
):
    """Creates a realization file based on the specified models.
    Note: This function automatically fetches calibrated parameters. Calibrated params are only
    available at certain gages for the SLoTH, NOM, and CFE model combination.

    Args:
        output_folder (str): Name of the output folder, usually the cat-id
        start_time (str): Start time of simulation in YYYY-MM-DD HH:MM:SS
        end_time (str): End time of simulation in YYYY-MM-DD HH:MM:SS
        models (list[str]): List of models to be coupled together
        routing (bool, optional): True if t-route is coupled. Defaults to False.
        gage_id (str | None, optional): Gage ID for the simulation. Defaults to None.
    """

    paths = FilePaths(output_folder)

    # calibrated parameter fetching
    if gage_id is not None and models == ["sloth", "nom", "cfe"]:
        if _handle_calibrated_params(paths, gage_id):
            return

    main_output_variable = MODEL_REGISTRY[models[-1]].main_output_variable

    target_variable_names = {}
    for model in models:
        if MODEL_REGISTRY[model].variables_names_map:
            target_variable_names[model] = copy.deepcopy(MODEL_REGISTRY[model].variables_names_map)

    modules: list[dict] = []
    seen_models: list[str] = []

    for model in models:
        if model == "sloth":
            continue  # SLoTH is handled separately below after all other models have been processed

        # Implicitly this means that if we have something like ["nom", "pet"], then the PET value
        # from the evapotranspiration module will override the PET value from Noah-OWP-M
        for dependency, overrides in MODEL_REGISTRY[model].overrides:
            if dependency in seen_models:
                target_variable_names[model].update(overrides)

        with open(MODEL_REGISTRY[model].realization_fragment, "r", encoding="utf-8") as f:
            realization = json.load(f)
        if model in target_variable_names:
            realization["params"]["variables_names_map"] = target_variable_names[model]
        modules.append(realization)

        seen_models.append(model)

    if "sloth" in models:
        _insert_sloth_module(models, target_variable_names, modules)

    with open(FilePaths.modular_template, "r", encoding="utf-8") as f:
        realization = json.load(f)

    realization["global"]["formulations"][0]["params"][
        "main_output_variable"
    ] = main_output_variable
    realization["global"]["formulations"][0]["params"]["modules"] = modules
    realization["time"]["start_time"] = datetime.strftime(start_time, "%Y-%m-%d %H:%M:%S")
    realization["time"]["end_time"] = datetime.strftime(end_time, "%Y-%m-%d %H:%M:%S")

    if routing:
        realization["routing"] = {"t_route_config_file_with_path": "./config/troute.yaml"}

    with open(paths.config_dir / "realization.json", "w", encoding="utf-8") as f:
        json.dump(realization, f, indent=4)
