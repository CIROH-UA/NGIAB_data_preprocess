"""Regression (characterization) tests for the generated ngen realization files.
These tests snapshot that output for each model against a committed golden file.
Updating goldens (only when a change is intentional):
    UPDATE_GOLDEN=1 uv run pytest tests/test_realization_templates.py
then eyeball the diff in ``tests/golden/realization/`` and commit it.
"""

import difflib
import json
import os
from datetime import datetime
from pathlib import Path

import pytest

from data_processing.create_realization import make_ngen_realization_json
from data_processing.file_paths import FilePaths

# Fixed inputs so the output is fully deterministic. These MUST match the values
# the committed goldens were generated with.
START = datetime(2020, 1, 1, 0, 0, 0)
END = datetime(2020, 1, 2, 0, 0, 0)
DEFAULT_OUTPUT_INTERVAL = 3600

GOLDEN_DIR = Path(__file__).parent / "golden" / "realization"

# model id (and golden filename stem) -> the FilePaths template attribute used by
# the corresponding create_*_realization builder.
MODELS = {
    "cfe-nom": "template_cfe_nowpm_realization_config",  # create_realization (default NOAH-OWP/CFE)
    "lstm-py": "template_lstm_realization_config",  # create_lstm_realization(use_rust=False)
    "lstm-rs": "template_lstm_rust_realization_config",  # create_lstm_realization(use_rust=True)
    "dhbv2": "template_dhbv2_realization_config",  # create_dhbv2_realization(daily=False)
    "dhbv2-daily": "template_dhbv2_daily_realization_config", # create_dhbv2_realization(daily=True)
    "summa": "template_summa_realization_config",  # create_summa_realization
    "snow17-nom-cfe": "template_snow17_realization_config",  # create_snow17_realization
    "sacsma-nom": "template_sac_realization_config",  # create_sacsma_realization
}


def _build_realization(template_attr, config_dir, output_interval=DEFAULT_OUTPUT_INTERVAL):
    """Run the real builder transform and return the parsed realization.json."""
    template_path = getattr(FilePaths, template_attr)
    assert Path(template_path).exists(), f"template not found on disk: {template_path}"
    make_ngen_realization_json(config_dir, template_path, START, END, output_interval)
    return json.loads((Path(config_dir) / "realization.json").read_text())


def _pretty(obj):
    # sort_keys only for a stable, readable diff; equality is checked on the dicts.
    return json.dumps(obj, indent=2, sort_keys=True).splitlines()


@pytest.mark.parametrize("model_id", list(MODELS))
def test_realization_matches_golden(model_id, tmp_path):
    """Check that generated realization files match golden files."""
    produced = _build_realization(MODELS[model_id], tmp_path)
    golden_file = GOLDEN_DIR / f"{model_id}.json"

    if os.environ.get("UPDATE_GOLDEN"):
        golden_file.parent.mkdir(parents=True, exist_ok=True)
        golden_file.write_text(json.dumps(produced, indent=2) + "\n")
        pytest.skip(f"UPDATE_GOLDEN set -- wrote {golden_file.name}; re-run without it to verify")

    assert golden_file.exists(), (
        f"missing golden {golden_file}. Generate it with: "
        f"UPDATE_GOLDEN=1 uv run pytest tests/test_realization_templates.py"
    )
    golden = json.loads(golden_file.read_text())

    if produced != golden:
        diff = "\n".join(
            difflib.unified_diff(
                _pretty(golden),
                _pretty(produced),
                fromfile=f"golden/{model_id}.json",
                tofile="produced",
                lineterm="",
            )
        )
        pytest.fail(
            f"realization.json for '{model_id}' changed.\n"
            f"If this is intentional, regenerate with UPDATE_GOLDEN=1 and commit.\n\n{diff}"
        )


@pytest.mark.parametrize("model_id", list(MODELS))
def test_realization_has_expected_top_level_shape(model_id, tmp_path):
    """Cheap structural guard: every realization keeps its core blocks."""
    produced = _build_realization(MODELS[model_id], tmp_path)
    for key in ("global", "time", "routing"):
        assert key in produced, f"'{model_id}' realization is missing top-level key '{key}'"


def test_time_block_is_stamped_correctly(tmp_path):
    """Guards the make_ngen_realization_json logic itself (not just the templates)."""
    produced = _build_realization(MODELS["cfe-nom"], tmp_path, output_interval=300)
    assert produced["time"]["start_time"] == "2020-01-01 00:00:00"
    assert produced["time"]["end_time"] == "2020-01-02 00:00:00"
    assert produced["time"]["output_interval"] == 300


def test_default_output_interval_is_hourly(tmp_path):
    """Check that output_interval is 3600s."""
    produced = _build_realization(MODELS["cfe-nom"], tmp_path)  # uses the default
    assert produced["time"]["output_interval"] == 3600
