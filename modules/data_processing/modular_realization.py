#import json
from rich.prompt import Prompt

# all_variable_names_maps = {
#     "cfe": {
#         "atmosphere_water__liquid_equivalent_precipitation_rate": "APCP_surface",
#         "water_potential_evaporation_flux": "sloth_pet",
#         "ice_fraction_schaake": "sloth_ice_fraction_schaake",
#         "ice_fraction_xinanjiang": "sloth_ice_fraction_xinanjiang",
#         "soil_moisture_profile": "sloth_soil_moisture_profile",
#     },
#     "casam": {
#         "precipitation_rate": "precip_rate",
#         "potential_evapotranspiration_rate": "sloth_pet",
#         "soil_temperature_profile": "sloth_soil_temperature_profile",
#     },
#     "sft": {
#         "ground_temperature": "sloth_ground_temperature",
#         "soil_moisture_profile": "sloth_soil_moisture_profile",
#     },
#     "smp": {
#         "soil_storage": "sloth_soil_storage",
#         "soil_storage_change": "sloth_soil_storage_change",
#         "num_wetting_fronts": "sloth_num_wetting_fronts",
#         "soil_moisture_wetting_fronts": "sloth_soil_moisture_wetting_fronts",
#         "soil_depth_wetting_fronts": "sloth_soil_depth_wetting_fronts",
#         "Qb_topmodel": "sloth_Qb_topmodel",
#         "Qv_topmodel": "sloth_Qv_topmodel",
#         "global_deficit": "sloth_global_deficit",
#     },
#     "topmodel": {
#         "atmosphere_water__liquid_equivalent_precipitation_rate": "APCP_surface",
#         "water_potential_evaporation_flux": "sloth_pet",
#     },
#     "sac-sma": {"tair": "TMP_2maboveground", "precip": "precip_rate", "pet": "sloth_pet"},
# }

# all_sloth_model_params = {
#     "sloth_ice_fraction_schaake": "(1,double,m,node)",
#     "sloth_ice_fraction_xinanjiang": "(1,double,1,node)",
#     "sloth_soil_moisture_profile": "(1,double,1,node)",
#     "sloth_soil_temperature_profile": "(1,double,K,node)",
#     "sloth_soil_storage": "(1,double,m,node)",
#     "sloth_soil_storage_change": "(1,double,m,node)",
#     "sloth_num_wetting_fronts": "(1,double,1,node)",
#     "sloth_soil_moisture_wetting_fronts": "(1,double,1,node)",
#     "sloth_soil_depth_wetting_fronts": "(1,double,m,node)",
#     "sloth_Qb_topmodel": "(1,double,m h^-1,node)",
#     "sloth_Qv_topmodel": "(1,double,m h^-1,node)",
#     "sloth_global_deficit": "(1,double,m,node)",
#     "sloth_pet": "(1,double,m s-1,node)",
# }

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

def validate_models(models: list[str], routing: bool):
    """Check that the specified models are valid and that any dependencies are met. If there are any
    issues, print a warning message and ask the user if they want to proceed anyway.

    Args:
        models (list[str]): List of models to use, in the order they will be executed
        routing (bool): Whether routing is enabled

    Raises:
        ValueError: _models is empty
        ValueError: _models contains invalid model names
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

# Main output variable
# if main_model == "cfe":
#     MAIN_OUTPUT_VARIABLE = "Q_OUT"
# elif main_model == "casam":
#     if ROUTING:
#         MAIN_OUTPUT_VARIABLE = "surface_runoff"
#     else:
#         MAIN_OUTPUT_VARIABLE = "total_discharge"
# elif main_model == "pet":
#     MAIN_OUTPUT_VARIABLE = "water_potential_evaporation_flux"
# elif main_model == "sft":
#     MAIN_OUTPUT_VARIABLE = "num_cells"
# elif main_model == "smp":
#     MAIN_OUTPUT_VARIABLE = "soil_storage"
# elif main_model == "topmodel":
#     MAIN_OUTPUT_VARIABLE = "Qout"
# elif main_model == "nom":
#     MAIN_OUTPUT_VARIABLE = "EVAPOTRANS"
# elif main_model == "snow17":
#     MAIN_OUTPUT_VARIABLE = "raim"
# elif main_model == "sac-sma":
#     MAIN_OUTPUT_VARIABLE = "tci"
# else:
#     MAIN_OUTPUT_VARIABLE = None

# # Get input variable names for each model based on configuration
# target_variable_names = copy.deepcopy(all_variable_names_maps)
# for model in target_variable_names:
#     if model not in models:
#         target_variable_names.pop(model)

# seen_models = []

# for model in models:
#     if model == "cfe":
#         if "nom" in seen_models:
#             target_variable_names[model][
#                 "atmosphere_water__liquid_equivalent_precipitation_rate"
#             ] = "QINSUR"
#             target_variable_names[model]["water_potential_evaporation_flux"] = "EVAPOTRANS"
#         if "pet" in seen_models:
#             target_variable_names[model][
#                 "water_potential_evaporation_flux"
#             ] = "water_potential_evaporation_flux"
#         if "sft" in seen_models:
#             target_variable_names[model]["ice_fraction_schaake"] = "ice_fraction_schaake"
#             target_variable_names[model]["ice_fraction_xinanjiang"] = "ice_fraction_xinanjiang"
#         if "smp" in seen_models:
#             target_variable_names[model]["soil_moisture_profile"] = "soil_moisture_profile"
#     elif model == "casam":
#         if "nom" in seen_models:
#             target_variable_names[model]["potential_evapotranspiration_rate"] = "EVAPOTRANS"
#         if "pet" in seen_models:
#             target_variable_names[model][
#                 "potential_evapotranspiration_rate"
#             ] = "water_potential_evaporation_flux"
#         if "sft" in seen_models:
#             target_variable_names[model]["soil_temperature_profile"] = "soil_temperature_profile"
#     elif model == "sft":
#         if "nom" in seen_models:
#             target_variable_names[model]["ground_temperature"] = "TGS"
#         if "smp" in seen_models:
#             target_variable_names[model]["soil_moisture_profile"] = "soil_moisture_profile"
#     elif model == "smp":
#         if "casam" in seen_models:
#             target_variable_names[model]["num_wetting_fronts"] = "soil_num_wetting_fronts"
#             target_variable_names[model][
#                 "soil_moisture_wetting_fronts"
#             ] = "soil_moisture_wetting_fronts"
#             target_variable_names[model]["soil_depth_wetting_fronts"] = "soil_depth_wetting_fronts"
#             target_variable_names[model]["soil_storage"] = "soil_storage"
#         if "cfe" in seen_models:
#             target_variable_names[model]["soil_storage"] = "SOIL_STORAGE"
#             target_variable_names[model]["soil_storage_change"] = "SOIL_STORAGE_CHANGE"
#         if "topmodel" in seen_models:
#             target_variable_names[model]["Qb_topmodel"] = "land_surface_water__baseflow_volume_flux"
#             target_variable_names[model][
#                 "Qv_topmodel"
#             ] = "soil_water_root-zone_unsat-zone_top__recharge_volume_flux"
#             target_variable_names[model]["global_deficit"] = "soil_water__domain_volume_deficit"
#     elif model == "topmodel":
#         if "nom" in seen_models:
#             target_variable_names[model][
#                 "atmosphere_water__liquid_equivalent_precipitation_rate"
#             ] = "QINSUR"
#             target_variable_names[model]["water_potential_evaporation_flux"] = "EVAPOTRANS"
#         if "pet" in seen_models:
#             target_variable_names[model][
#                 "water_potential_evaporation_flux"
#             ] = "water_potential_evaporation_flux"
#     elif model == "sac-sma":
#         if "nom" in seen_models:
#             target_variable_names[model]["pet"] = "EVAPOTRANS"
#         if "pet" in seen_models:
#             target_variable_names[model]["pet"] = "water_potential_evaporation_flux"
#     else:
#         pass

#     seen_models.append(model)

# sloth_model_params = {}
# for model in target_variable_names:
#     for var in target_variable_names[model]:
#         varname = target_variable_names[model][var]
#         if varname in all_sloth_model_params:
#             sloth_param_name = varname + all_sloth_model_params[varname]
#             sloth_model_params[sloth_param_name] = 0.0
