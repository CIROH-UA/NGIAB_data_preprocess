"""Unit tests for grafting calibrated gage parameters onto the default template.

Hermetic: both inputs are built in memory, nothing is downloaded.
"""

import copy
import json
import logging

import pytest

from data_processing.create_realization import (
    CALIBRATED_MODULES,
    graft_calibrated_model_params,
)
from data_processing.file_paths import FilePaths


def _modules(realization):
    return realization["global"]["formulations"][0]["params"]["modules"]


def _module(realization, model_type_name):
    return next(
        m for m in _modules(realization) if m["params"]["model_type_name"] == model_type_name
    )


@pytest.fixture
def default_template():
    """The template the preprocessor ships."""
    return json.loads(FilePaths.template_cfe_nowpm_realization_config.read_text())


@pytest.fixture
def calibrated(default_template):
    """A published gage file: calibrated values plus the stale scaffolding.

    Mirrors what is actually in the bucket -- see gage-10109001.json.
    """
    doc = copy.deepcopy(default_template)

    sloth = _module(doc, "SLOTH")["params"]
    sloth["library_file"] = "/dmod/shared_libs/libslothmodel.so.1.0.0"
    sloth["model_params"] = {k: str(v) for k, v in sloth["model_params"].items()}

    _module(doc, "NoahOWP")["params"]["model_params"] = {"CWP": 0.162, "MFSNO": 3.644}
    _module(doc, "CFE")["params"]["model_params"] = {"b": 7.05, "satdk": 0.00072}

    for module in _modules(doc):
        module["params"].pop("forcing_file", None)
        module["params"]["fixed_time_step"] = False

    return doc


def test_calibrated_values_are_grafted(default_template, calibrated):
    merged = graft_calibrated_model_params(default_template, calibrated)

    assert _module(merged, "NoahOWP")["params"]["model_params"] == {"CWP": 0.162, "MFSNO": 3.644}
    assert _module(merged, "CFE")["params"]["model_params"] == {"b": 7.05, "satdk": 0.00072}


def test_sloth_model_params_are_left_alone(default_template, calibrated):
    """SLOTH's published values are the defaults stringified, so they are not copied."""
    merged = graft_calibrated_model_params(default_template, calibrated)

    assert (
        _module(merged, "SLOTH")["params"]["model_params"]
        == _module(default_template, "SLOTH")["params"]["model_params"]
    )
    assert all(
        isinstance(v, float) for v in _module(merged, "SLOTH")["params"]["model_params"].values()
    )


def test_stale_scaffolding_is_not_carried_over(default_template, calibrated):
    """Everything outside model_params comes from the default template."""
    merged = graft_calibrated_model_params(default_template, calibrated)

    assert (
        _module(merged, "SLOTH")["params"]["library_file"] == "/dmod/shared_libs/libslothmodel.so"
    )

    # keys the published file adds must not appear, and keys it drops must survive
    assert "fixed_time_step" not in _module(merged, "NoahOWP")["params"]
    assert "forcing_file" in _module(merged, "NoahOWP")["params"]


def test_only_model_params_differ_from_the_default(default_template, calibrated):
    """Guards against anything else leaking in as the published files change."""
    merged = graft_calibrated_model_params(default_template, calibrated)

    for module in _modules(merged):
        module["params"].pop("model_params", None)
    for module in _modules(default_template):
        module["params"].pop("model_params", None)

    assert merged == default_template


def test_inputs_are_not_mutated(default_template, calibrated):
    before_default = copy.deepcopy(default_template)
    before_calibrated = copy.deepcopy(calibrated)

    graft_calibrated_model_params(default_template, calibrated)

    assert default_template == before_default
    assert calibrated == before_calibrated


def test_template_without_calibrated_params_falls_back_and_warns(default_template, caplog):
    """gage-10171000 publishes no model_params for either calibrated module."""
    calibrated = copy.deepcopy(default_template)
    for name in CALIBRATED_MODULES:
        _module(calibrated, name)["params"].pop("model_params", None)

    with caplog.at_level(logging.WARNING):
        merged = graft_calibrated_model_params(default_template, calibrated)

    assert merged == default_template
    assert "no model_params" in caplog.text


def test_unrecognised_template_shape_falls_back(default_template, caplog):
    """A malformed download must not take the realization down with it."""
    with caplog.at_level(logging.WARNING):
        merged = graft_calibrated_model_params(default_template, {"not": "a realization"})

    assert merged == default_template
    assert "no model_params" in caplog.text
