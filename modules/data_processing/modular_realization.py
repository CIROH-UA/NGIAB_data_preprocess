"""Placeholder module to generate modular realizations. Currently only contains rules and model
dependencies."""

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
    "cfe": "Q_OUT",
    "pet": "water_potential_evaporation_flux",
    "sft": "num_cells",
    "smp": "soil_storage",
    "topmodel": "Qout",
    "nom": "EVAPOTRANS",
    "snow17": "raim",
    "sac-sma": "tci",
}

# these go into the variable_names_map section of the realization. This dictionary is a template
# until a copy is edited later
ALL_VARIABLE_NAMES_MAPS = {
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

# Some models cannot run without other models running first.
# if models is a list of models that a user wants to run in the order that is passed,
# we would read these rules like this:
# ("target model", lambda models: models that need to run before the target model, warning message
# that is shown if the model list is invalid)
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

# SLoTH is used to fill in parameters when prerequisite models aren't used
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
    "sloth_ground_temperature": "(1,double,K,node)"
}

# read like this:
# "target model"("model run prior to target model", {lines in realization template that would get
# overwritten}
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