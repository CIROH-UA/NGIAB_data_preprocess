"""Tests for ``data_processing.create_realization``.
"""

import copy
import difflib
import json
from datetime import datetime
from pathlib import Path

import pytest

from data_processing.file_paths import FilePaths
import data_processing.create_realization as mr
from data_processing.create_realization import (
    ACCEPTED_MODELS,
    ALL_SLOTH_MODEL_PARAMS,
    ALL_VARIABLES_NAMES_MAPS,
    MAIN_OUTPUT_VARIABLES,
    _insert_sloth_module,
    create_modular_realization,
    validate_models,
)

START = datetime(2020, 1, 1, 0, 0, 0)
END = datetime(2020, 1, 2, 0, 0, 0)

GOLDEN_DIR = Path(__file__).parent / "golden" / "realization"
CFE_NOM_GOLDEN = GOLDEN_DIR / "cfe-nom.json"


# ---------------------------------------------------------------------------
# fixtures / helpers
# ---------------------------------------------------------------------------
@pytest.fixture(name="answer_prompt")
def answer_prompt_fixture(monkeypatch):
    """Stub ``Prompt.ask``; record whether it was shown and what it returned."""

    class Recorder:  # pylint: disable=too-few-public-methods
        """Simple prompt-answer recorder used to stub Prompt.ask in tests."""

        called = False
        response = "n"

        def __call__(self, response):
            self.response = response
            return self

    rec = Recorder()

    def fake_ask(*args, **kwargs):  # pylint: disable=unused-argument
        rec.called = True
        return rec.response

    monkeypatch.setattr(mr.Prompt, "ask", staticmethod(fake_ask))
    return rec


@pytest.fixture(name="make_realization")
def make_realization_fixture(tmp_path, monkeypatch, answer_prompt):
    """Return a runner for ``create_modular_realization`` rooted at ``tmp_path``.

    Pre-creates ``config/`` because the function writes ``realization.json`` into
    it but does not create it (the real workflow makes it during subsetting).
    """
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: Path(tmp_path)))
    answer_prompt("y")

    def _run(  # pylint: disable=too-many-arguments
        models, *, folder="cat-test", start=START, end=END, routing=False, make_config=True
    ):
        paths = FilePaths(folder)
        if make_config:
            paths.config_dir.mkdir(parents=True, exist_ok=True)
        create_modular_realization(folder, start, end, models, routing=routing)
        return json.loads((paths.config_dir / "realization.json").read_text())

    return _run


def _modules_of(realization):
    return realization["global"]["formulations"][0]["params"]["modules"]


def _model_type_names(realization):
    return [m["params"]["model_type_name"] for m in _modules_of(realization)]


def _cfe_module(realization):
    return next(m for m in _modules_of(realization) if m["params"]["model_type_name"] == "CFE")


# ===========================================================================
# Headline: the modular build must equal the respective golden.
# ===========================================================================

# One entry per committed golden: (models in execution order, golden filename, label).
# routing is on for all of them (the goldens were stamped with routing enabled).
GOLDEN_CASES = [
    (["sloth", "nom", "cfe"], "cfe-nom.json", "sloth->nom->cfe"),
    (["dhbv2"], "dhbv2.json", "dhbv2"),
    (["dhbv2_daily"], "dhbv2-daily.json", "dhbv2_daily"),
    (["lstm"], "lstm-py.json", "lstm"),
    (["lstm_rust"], "lstm-rs.json", "lstm_rust"),
    (["nom", "sac-sma"], "sacsma-nom.json", "nom->sac-sma"),
    (["sloth", "snow17", "nom", "cfe"], "snow17-nom-cfe.json", "sloth->snow17->nom->cfe"),
    (["summa"], "summa.json", "summa"),
]


@pytest.mark.parametrize("models, golden_name, label", GOLDEN_CASES)
def test_modular_realization_matches_golden(models, golden_name, label, make_realization):
    """Each model combination (routing on) must reproduce its committed golden.

    Mirrors test_sloth_nom_cfe_matches_cfe_nom_golden: build with the same fixed
    inputs the golden was stamped with, then compare the parsed dicts (key order
    is irrelevant -- only structure and values matter).
    """
    produced = make_realization(models, routing=True)
    golden = json.loads((GOLDEN_DIR / golden_name).read_text())
    if produced != golden:
        diff = "\n".join(
            difflib.unified_diff(
                json.dumps(golden, indent=2, sort_keys=True).splitlines(),
                json.dumps(produced, indent=2, sort_keys=True).splitlines(),
                fromfile=f"golden/{golden_name}",
                tofile=f"produced ({label})",
                lineterm="",
            )
        )
        pytest.fail(f"modular {label} realization does not match {golden_name}.\n\n" + diff)


# ---------------------------------------------------------------------------
# validate_models -- input validation
# ---------------------------------------------------------------------------
class TestValidateModelsInputs:
    """Validate input handling for the model-selection parser."""

    def test_empty_list_raises(self, answer_prompt):
        """Ensure an empty model list raises a clear validation error."""
        with pytest.raises(ValueError, match="No models specified"):
            validate_models([], routing=False)
        assert not answer_prompt.called

    def test_unknown_model_raises_and_names_offender(self, answer_prompt):
        """Unknown model names are rejected before any prompt, and the error
        message names the offending model."""
        with pytest.raises(ValueError) as exc:
            validate_models(["cfe", "not_a_model"], routing=False)
        message = str(exc.value)
        assert "Invalid models specified" in message
        assert "not_a_model" in message
        assert not answer_prompt.called

    def test_every_accepted_model_is_a_valid_name(self, answer_prompt):
        """[model] alone must never trip the 'invalid name' guard (it may still
        warn about dependencies -- we answer 'y')."""
        answer_prompt("y")
        for model in ACCEPTED_MODELS:
            try:
                validate_models([model], routing=False)
            except ValueError as e:
                assert "Invalid models specified" not in str(e), model


# ---------------------------------------------------------------------------
# validate_models -- dependency rules
# ---------------------------------------------------------------------------
class TestValidateModelsDependencies:
    """Validate dependency-driven prompts and rule enforcement for model lists."""

    WARNING_CASES = [
        (["cfe"], "CFE requires SLoTH"),
        (["casam"], "CASAM requires SLoTH"),
        (["sft"], "SFT requires SLoTH"),
        (["smp"], "SMP requires SLoTH"),
        (["topmodel"], "TOPMODEL requires SLoTH, NOM, or PET"),
        (["sac-sma"], "SAC-SMA requires SLoTH, NOM, or PET"),
    ]

    @pytest.mark.parametrize("models, substring", WARNING_CASES)
    def test_unmet_dependency_warns_and_aborts_on_no(self, models, substring, answer_prompt):
        """Ensure a dependency warning aborts when the user declines to proceed."""
        answer_prompt("n")
        with pytest.raises(ValueError) as exc:
            validate_models(models, routing=False)
        assert answer_prompt.called
        assert substring in str(exc.value)

    @pytest.mark.parametrize("models", [case[0] for case in WARNING_CASES])
    def test_unmet_dependency_proceeds_on_yes(self, models, answer_prompt):
        """Ensure the user can proceed past a dependency warning by answering yes."""
        answer_prompt("y")
        validate_models(models, routing=False)  # must not raise
        assert answer_prompt.called

    @pytest.mark.parametrize(
        "models",
        [
            ["sloth", "cfe"],
            ["sloth", "casam"],
            ["sloth", "sft"],
            ["sloth", "smp"],
            ["nom", "topmodel"],
            ["pet", "topmodel"],
            ["nom", "sac-sma"],
            ["pet", "sac-sma"],
        ],
    )
    def test_met_dependency_does_not_prompt(self, models, answer_prompt):
        """Ensure satisfied dependencies do not trigger a confirmation prompt."""
        validate_models(models, routing=False)
        assert not answer_prompt.called

    def test_multiple_unmet_dependencies_accumulate(self, answer_prompt):
        """Each failing model contributes its own warning line to the message."""
        answer_prompt("n")
        with pytest.raises(ValueError) as exc:
            validate_models(["cfe", "casam"], routing=False)
        message = str(exc.value)
        assert "CFE requires SLoTH" in message
        assert "CASAM requires SLoTH" in message


# ---------------------------------------------------------------------------
# validate_models -- routing rule
# ---------------------------------------------------------------------------
class TestValidateModelsRouting:
    """Validate routing-related model selection rules."""

    def test_routing_without_rainfall_runoff_warns(self, answer_prompt):
        """Ensure routing requires a rainfall-runoff model and prompts otherwise."""
        answer_prompt("n")
        with pytest.raises(ValueError, match="Routing is on but no rainfall-runoff"):
            validate_models(["nom"], routing=True)
        assert answer_prompt.called

    def test_routing_with_rainfall_runoff_is_quiet(self, answer_prompt):
        """Ensure routing succeeds without prompting when a runoff model is present."""
        validate_models(["sloth", "cfe"], routing=True)
        assert not answer_prompt.called

    def test_routing_off_never_adds_routing_warning(self, answer_prompt):
        """Ensure routing is ignored when routing is disabled."""
        validate_models(["nom"], routing=False)
        assert not answer_prompt.called


# ---------------------------------------------------------------------------
# _insert_sloth_module
# ---------------------------------------------------------------------------
class TestInsertSlothModule:
    """Validate the helper that inserts the SLOTH module into the realization."""

    def test_inserts_at_sloth_index(self):
        """Ensure the SLOTH module is inserted at the expected position."""
        modules = [
            {"params": {"model_type_name": "A"}},
            {"params": {"model_type_name": "B"}},
        ]
        _insert_sloth_module(["A", "sloth", "B"], {"cfe": {"x": "sloth_pet"}}, modules)
        assert [m["params"]["model_type_name"] for m in modules] == ["A", "SLOTH", "B"]

    def test_builds_model_params_from_sloth_prefixed_vars(self):
        """Ensure SLOTH parameters are derived from sloth-prefixed variable names."""
        modules = []
        target = {"cfe": copy.deepcopy(ALL_VARIABLES_NAMES_MAPS["cfe"])}
        _insert_sloth_module(["sloth"], target, modules)
        params = modules[0]["params"]["model_params"]
        assert set(params.keys()) == {
            "sloth_pet" + ALL_SLOTH_MODEL_PARAMS["sloth_pet"],
            "sloth_ice_fraction_schaake" + ALL_SLOTH_MODEL_PARAMS["sloth_ice_fraction_schaake"],
            "sloth_ice_fraction_xinanjiang"
            + ALL_SLOTH_MODEL_PARAMS["sloth_ice_fraction_xinanjiang"],
            "sloth_soil_moisture_profile" + ALL_SLOTH_MODEL_PARAMS["sloth_soil_moisture_profile"],
        }
        assert all(v == 0.0 for v in params.values())

    def test_non_sloth_vars_are_ignored(self):
        """Ensure non-SLOTH variables do not contribute model parameters."""
        modules = []
        _insert_sloth_module(["sloth"], {"cfe": {"precip": "APCP_surface"}}, modules)
        assert modules[0]["params"]["model_params"] == {"sloth_dummy_param(1,double,1,node)": 0.0}

    def test_sloth_dummy_variable_added(self):
        """Ensure SLoTH dummy variable is added when no other parameters are present."""
        modules = []
        _insert_sloth_module(["sloth"], {}, modules)
        assert modules[0]["params"]["model_params"] == {"sloth_dummy_param(1,double,1,node)": 0.0}


# ---------------------------------------------------------------------------
# create_modular_realization -- integration
# ---------------------------------------------------------------------------
class TestCreateModularRealization:
    """Validate the end-to-end modular realization creation workflow."""

    def test_writes_realization_json(self, make_realization, tmp_path):
        """Ensure the realization JSON is written to the expected config path."""
        make_realization(["sloth", "cfe"])
        assert (tmp_path / "cat-test" / "config" / "realization.json").exists()

    def test_top_level_shape(self, make_realization):
        """Ensure the generated realization contains the expected top-level keys."""
        r = make_realization(["sloth", "cfe"])
        assert {"global", "time", "output_root"} <= set(r)

    def test_main_output_variable_comes_from_last_model(self, make_realization):
        """Ensure the main output variable follows the last model in the chain."""
        r = make_realization(["sloth", "cfe"])
        params = r["global"]["formulations"][0]["params"]
        assert params["main_output_variable"] == MAIN_OUTPUT_VARIABLES["cfe"] == "Q_OUT"

    def test_sloth_inserted_before_cfe_by_position(self, make_realization):
        """Ensure the SLOTH module appears before the CFE module in the chain."""
        r = make_realization(["sloth", "cfe"])
        names = _model_type_names(r)
        assert names.index("SLOTH") < names.index("CFE")

    def test_nom_override_rewrites_cfe_sources(self, make_realization):
        """nom seen before cfe -> cfe's precip/PET sources switch to the
        Noah-OWP outputs."""
        r = make_realization(["sloth", "nom", "cfe"])
        vmap = _cfe_module(r)["params"]["variables_names_map"]
        assert vmap["water_potential_evaporation_flux"] == "EVAPOTRANS"
        assert vmap["atmosphere_water__liquid_equivalent_precipitation_rate"] == "QINSUR"

    def test_casam_can_be_main_model(self, make_realization):
        """casam has a MAIN_OUTPUT_VARIABLES entry, so it can be the terminal
        model, and its module appears in the coupled chain."""
        r = make_realization(["sloth", "nom", "casam"])
        assert "CASAM" in _model_type_names(r)
        params = r["global"]["formulations"][0]["params"]
        assert params["main_output_variable"] == MAIN_OUTPUT_VARIABLES["casam"] == "total_discharge"

    def test_nom_override_rewrites_casam_pet_source(self, make_realization):
        """nom seen before casam -> casam's PET source switches to the Noah-OWP
        output (confirms casam applies its computed map, like cfe)."""
        r = make_realization(["sloth", "nom", "casam"])
        casam = next(m for m in _modules_of(r) if m["params"]["model_type_name"] == "CASAM")
        assert casam["params"]["variables_names_map"]["potential_evapotranspiration_rate"] == (
            "EVAPOTRANS"
        )

    def test_override_does_not_fire_when_dependency_seen_later(self, make_realization):
        """cfe before nom -> cfe keeps its default (sloth/forcing) sources."""
        r = make_realization(["sloth", "cfe", "nom"])
        vmap = _cfe_module(r)["params"]["variables_names_map"]
        assert (
            vmap["water_potential_evaporation_flux"]
            == (ALL_VARIABLES_NAMES_MAPS["cfe"]["water_potential_evaporation_flux"])
        )

    def test_routing_on_adds_troute_block(self, make_realization):
        """Ensure routing adds the expected TRoute configuration block."""
        r = make_realization(["sloth", "cfe"], routing=True)
        assert r["routing"] == {"t_route_config_file_with_path": "./config/troute.yaml"}

    def test_routing_off_omits_routing_block(self, make_realization):
        """Ensure routing is omitted when routing is disabled."""
        r = make_realization(["sloth", "cfe"], routing=False)
        assert "routing" not in r

    def test_missing_config_dir_raises(self, make_realization):
        """config/ must already exist; the function does not create it."""
        with pytest.raises(FileNotFoundError):
            make_realization(["sloth", "cfe"], make_config=False)

    def test_string_time_inputs_now_raise(self, make_realization):
        """The builder stamps time via datetime.strftime, so it now requires
        datetime objects -- passing pre-formatted strings raises TypeError.
        (Matches the datetime contract of make_ngen_realization_json.)"""
        with pytest.raises(TypeError):
            make_realization(
                ["sloth", "cfe"], start="2020-01-01 00:00:00", end="2020-01-02 00:00:00"
            )
