"""Tests for ``data_processing.modular_realization``.
Currently only tests validation logic.
"""

from datetime import datetime
from pathlib import Path

import pytest

import data_processing.modular_realization as mr
from data_processing.modular_realization import (
    ACCEPTED_MODELS,
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
    return rec  # pylint: disable=too-few-public-methods


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

    def test_unknown_model_raises_and_names_it(self, answer_prompt):
        """Ensure unknown model names are rejected with a helpful message."""
        with pytest.raises(ValueError, match="Invalid models specified"):
            validate_models(["cfe", "not_a_model"], routing=False)
        assert not answer_prompt.called

    def test_unknown_model_message_lists_the_offender(self):
        """Ensure the validation error reports the offending model name."""
        with pytest.raises(ValueError) as exc:
            validate_models(["bogus"], routing=False)
        assert "bogus" in str(exc.value)

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

    @pytest.mark.parametrize("models, substring", WARNING_CASES)
    def test_unmet_dependency_proceeds_on_yes(self, models, substring, answer_prompt):  # pylint: disable=unused-argument
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
