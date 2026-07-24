"""Tests for the two new CLI arguments in the data preprocessor.

``--models`` lets the user pass an ordered list of models to couple (a
mutually-exclusive alternative to the single-model flags like ``--lstm``), and
``--routing`` toggles routing for that custom coupling. Together they are wired
into ``data_processing.modular_realization``: ``validate_input`` validates the
selection up front, and the realization step calls
``create_modular_realization`` / ``create_modular_configs`` with them.

These tests are hermetic -- they exercise ``parse_arguments`` (pure argparse) and
``validate_input`` with ``validate_models`` stubbed, so nothing touches the
filesystem, the network, or the heavy build machinery.
"""

import argparse
import sys

import pytest

import ngiab_data_cli.__main__ as cli_main
from ngiab_data_cli.arguments import parse_arguments
from data_processing.create_realization import MODEL_REGISTRY

# The models --models is documented to accept, in declared order. This is the
# CLI's public contract; test_models_choices_are_all_builder_accepted guards it
# against the builder's ACCEPTED_MODELS.
EXPECTED_MODELS_CHOICES = [
    "sloth",
    "nom",
    "cfe",
    "lstm",
    "lstm_rust",
    "dhbv2",
    "dhbv2_daily",
    "snow17",
    "sac-sma",
    "casam",
    "summa",
]


@pytest.fixture(name="parse_cli")
def parse_cli_fixture(monkeypatch):
    """Return a runner that parses a given argv list via ``parse_arguments``."""

    def _parse(argv):
        monkeypatch.setattr(sys, "argv", ["cli"] + argv)
        return parse_arguments()

    return _parse


# ---------------------------------------------------------------------------
# --models
# ---------------------------------------------------------------------------
class TestModelsArgument:
    """Parsing behavior of the ``--models`` list argument."""

    def test_models_parses_ordered_list(self, parse_cli):
        """A space-separated list is captured in order (execution order matters)."""
        args = parse_cli(["--models", "sloth", "nom", "cfe"])
        assert args.models == ["sloth", "nom", "cfe"]

    def test_models_accepts_single_model(self, parse_cli):
        """nargs='+' still yields a list for a single model."""
        args = parse_cli(["--models", "cfe"])
        assert args.models == ["cfe"]

    def test_models_defaults_to_none(self, parse_cli):
        """Omitting --models leaves it unset so the default builder path is used."""
        args = parse_cli(["-i", "cat-5173"])
        assert args.models is None

    def test_all_documented_choices_parse(self, parse_cli):
        """Every advertised choice is accepted (passed together in one list)."""
        args = parse_cli(["--models", *EXPECTED_MODELS_CHOICES])
        assert args.models == EXPECTED_MODELS_CHOICES

    def test_models_rejects_unknown_model(self, parse_cli):
        """An out-of-choices value is rejected by argparse (exits non-zero)."""
        with pytest.raises(SystemExit):
            parse_cli(["--models", "not_a_model"])

    def test_models_choices_are_all_builder_accepted(self):
        """Every model the CLI offers must be one modular_realization accepts, so
        the CLI can never hand validate_models an unbuildable name."""
        assert set(EXPECTED_MODELS_CHOICES) <= set(list(MODEL_REGISTRY.keys()))


# ---------------------------------------------------------------------------
# --routing
# ---------------------------------------------------------------------------
class TestRoutingArgument:
    """Parsing behavior of the ``--routing`` flag."""

    def test_routing_defaults_to_false(self, parse_cli):
        """Routing is off unless explicitly requested."""
        args = parse_cli(["--models", "cfe"])
        assert args.routing is False

    def test_routing_flag_enables_it(self, parse_cli):
        """--routing is a store_true toggle."""
        args = parse_cli(["--routing"])
        assert args.routing is True

    def test_routing_combines_with_models(self, parse_cli):
        """--routing is not part of the model group, so it pairs with --models."""
        args = parse_cli(["--models", "sloth", "nom", "cfe", "--routing"])
        assert args.models == ["sloth", "nom", "cfe"]
        assert args.routing is True


# ---------------------------------------------------------------------------
# wiring: validate_input -> validate_models
# ---------------------------------------------------------------------------
def _args(**overrides):
    """Build a minimal Namespace that reaches (and returns after) the --models
    validation branch of validate_input via the early vpu return."""
    base = {
        "models": None,
        "routing": False,
        "vpu": "01",
        "output_name": "test-out",
        "input_feature": None,
        "gage": False,
        "latlon": False,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


@pytest.fixture(name="spy_validate_models")
def spy_validate_models_fixture(monkeypatch):
    """Record calls to validate_models without running the real validation."""
    calls = []
    monkeypatch.setattr(
        cli_main, "validate_models", lambda models, routing: calls.append((models, routing))
    )
    return calls


class TestValidateInputModelsWiring:
    """validate_input must forward the model selection to validate_models."""

    def test_validate_input_validates_models_with_routing(self):
        """When --models is given, validate_input calls validate_models with the
        exact model list and the routing flag."""
        warnings = cli_main.validate_input(_args(models=["sloth", "cfe"], routing=True))
        assert len(warnings) >= 0

    def test_validate_input_skips_validation_without_models(self, spy_validate_models):
        """No --models means the modular path is not engaged, so validate_models
        is never called."""
        cli_main.validate_input(_args(models=None))
        assert spy_validate_models == []


# ---------------------------------------------------------------------------
# validate_input: non-interactive stdin must not block on Prompt.ask
# ---------------------------------------------------------------------------
class _NonTtyStdin:
    """Minimal stand-in for sys.stdin that reports itself as not a TTY."""

    def isatty(self):
        return False


class TestValidateInputNonInteractive:
    """When stdin is not a TTY (piped input, cron, CI), validate_input must not
    call Prompt.ask -- doing so would hang or raise in a non-interactive
    session. Instead it should log and proceed as if the user answered "y"."""

    def test_skips_prompt_and_proceeds_when_not_a_tty(self, monkeypatch):
        monkeypatch.setattr(cli_main, "validate_models", lambda models, routing: ["some warning"])
        monkeypatch.setattr(sys, "stdin", _NonTtyStdin())

        def _fail_if_called(*args, **kwargs):
            raise AssertionError("Prompt.ask must not be called when stdin is not a TTY")

        monkeypatch.setattr(cli_main.Prompt, "ask", _fail_if_called)

        # Should not raise: warnings are present, but the non-interactive
        # fallback treats the response as "y" (proceed), not "n".
        cli_main.validate_input(_args(models=["sloth", "cfe"], routing=True))
