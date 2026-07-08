"""Config-generation tests for ``create_modular_configs`` (the orchestration wrapper).

``create_modular_configs`` dispatches each requested model to the same
``make_*_config`` builders that ``test_config_generation`` exercises directly, so a
run over the full supported model set must reproduce the committed
``tests/golden/config/{cat_id}.json`` goldens. That equivalence is the headline
test. The rest cover the wrapper's own orchestration logic that the legacy suite
does not: per-model dispatch, the ``routing`` flag, the SLoTH no-op, and the
``NotImplementedError`` path for not-yet-supported models.

Marked ``integration`` to match ``test_config_generation``: these spin up the full
config-generation machinery (dask/duckdb/geopandas) and the dHBV2 path reads its
attributes parquet from S3 (FilePaths.dhbv_attributes), so they are non-hermetic.
Fixtures, the golden directory, the fixed START/END, and the tolerant comparison
helpers are imported from ``test_config_generation`` so the goldens and comparison
logic stay single-sourced.
"""

import difflib
import json
import shutil
from pathlib import Path

import pytest

from data_processing.file_paths import FilePaths
from data_processing.modular_realization import create_modular_configs

from test_config_generation import (
    GEOPACKAGE_FIXTURES,
    GOLDEN_CONFIG_DIR,
    START,
    END,
    _config_text_equivalent,
    _normalize,
)

pytestmark = pytest.mark.integration

# A small, fast fixture used by the single-catchment orchestration tests.
CAT_ID = "cat-1555522"

# The models create_modular_configs can build a config for today; this is exactly
# the set baked into the golden {cat_id}.json files (sloth produces no file, and
# casam/pet/sft/smp/topmodel/summa are not supported yet).
ALL_CONFIG_MODELS = ["cfe", "nom", "snow17", "sac-sma", "lstm", "dhbv2", "dhbv2_daily"]


def _generate_modular_config(cat_id, tmp_root, monkeypatch, *, models, routing=False):
    """Run ``create_modular_configs`` and return ``{relative_path: normalized_text}``.

    Mirrors ``test_config_generation._generate_config`` (patches get_working_dir,
    seeds the geopackage, normalizes machine-specific bits) but drives the build
    through the modular wrapper instead of the individual makers.
    """
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: Path(tmp_root)))

    paths = FilePaths(cat_id)
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPACKAGE_FIXTURES[cat_id], paths.geopackage_path)

    create_modular_configs(cat_id, START, END, models, routing=routing)

    produced = {}
    for f in sorted(paths.config_dir.rglob("*")):
        if f.is_file() and f.suffix != ".gpkg" and f.name != "realization.json":
            rel = str(f.relative_to(paths.config_dir))
            text = f.read_text(errors="replace")
            produced[rel] = _normalize(text, paths.output_dir)  # type: ignore
    return produced


@pytest.fixture(name="require")
def require_fixture():
    """Skip a test if its required geopackage fixture is not committed."""

    def _check(cat_id):
        if not GEOPACKAGE_FIXTURES[cat_id].exists():
            pytest.skip(
                f"missing geopackage fixture {GEOPACKAGE_FIXTURES[cat_id]}. "
                "Save it there or edit GEOPACKAGE_FIXTURES."
            )

    return _check


# ===========================================================================
# Headline: the wrapper reproduces the legacy config golden.
# ===========================================================================
@pytest.mark.parametrize("cat_id", list(GEOPACKAGE_FIXTURES))
def test_modular_configs_match_legacy_golden(cat_id, tmp_path, monkeypatch, require):
    """The full supported model set (routing on) must reproduce {cat_id}.json --
    the same golden the legacy builder is checked against -- proving the wrapper
    delegates to the makers with the correct arguments.

    Note: this shares tests/golden/config/{cat_id}.json with
    test_config_generation, so regenerate via that suite's UPDATE_GOLDEN flow.
    """
    require(cat_id)
    produced = _generate_modular_config(
        cat_id, tmp_path, monkeypatch, models=ALL_CONFIG_MODELS, routing=True
    )

    golden_file = GOLDEN_CONFIG_DIR / f"{cat_id}.json"
    assert golden_file.exists(), (
        f"missing golden {golden_file}. Generate it with: "
        f"UPDATE_GOLDEN=1 uv run pytest tests/test_config_generation.py"
    )
    golden = json.loads(golden_file.read_text())

    # 1) the set of generated files must match the legacy set exactly
    missing = sorted(set(golden) - set(produced))
    extra = sorted(set(produced) - set(golden))
    assert not missing and not extra, (
        f"config file set differs for {cat_id}.\n  missing: {missing}\n  extra: {extra}"
    )

    # 2) each file's (normalized) content must match, with numeric tolerance
    changed = [p for p in sorted(golden) if not _config_text_equivalent(golden[p], produced[p])]
    if changed:
        first = changed[0]
        diff = "\n".join(
            difflib.unified_diff(
                golden[first].splitlines(),
                produced[first].splitlines(),
                fromfile=f"golden/{first}",
                tofile=f"produced/{first}",
                lineterm="",
            )
        )
        pytest.fail(
            f"{len(changed)} config file(s) differ for {cat_id}: {changed}\n"
            f"first diff ({first}):\n{diff}"
        )


# ===========================================================================
# Orchestration logic specific to create_modular_configs.
# ===========================================================================
class TestOrchestration:
    """Behaviors the wrapper owns, beyond delegating to the makers."""

    def test_routing_true_emits_troute(self, tmp_path, monkeypatch, require):
        """routing=True must invoke configure_troute (troute.yaml appears)."""
        require(CAT_ID)
        produced = _generate_modular_config(
            CAT_ID, tmp_path, monkeypatch, models=["cfe"], routing=True
        )
        assert "troute.yaml" in produced

    def test_routing_false_omits_troute(self, tmp_path, monkeypatch, require):
        """routing=False must not emit a troute.yaml."""
        require(CAT_ID)
        produced = _generate_modular_config(
            CAT_ID, tmp_path, monkeypatch, models=["cfe"], routing=False
        )
        assert "troute.yaml" not in produced

    def test_only_requested_models_are_generated(self, tmp_path, monkeypatch, require):
        """Asking for cfe alone produces CFE configs and nothing for other models."""
        require(CAT_ID)
        produced = _generate_modular_config(
            CAT_ID, tmp_path, monkeypatch, models=["cfe"], routing=False
        )
        keys = list(produced)
        assert any(k.startswith("cat_config/CFE/") and k.endswith(".ini") for k in keys)
        assert not any(k.startswith("cat_config/NOAH-OWP-M/") for k in keys)
        assert not any(k.startswith("cat_config/SNOW17/") for k in keys)
        assert not any(k.startswith("cat_config/dhbv2") for k in keys)

    def test_sloth_only_generates_no_config_files(self, tmp_path, monkeypatch, require):
        """SLoTH needs no config file, so a sloth-only run writes nothing and
        does not error."""
        require(CAT_ID)
        produced = _generate_modular_config(
            CAT_ID, tmp_path, monkeypatch, models=["sloth"], routing=False
        )
        assert not produced

    def test_unsupported_model_raises_not_implemented(self, tmp_path, monkeypatch, require):
        """Models without a config builder yet raise NotImplementedError naming
        the offending model."""
        require(CAT_ID)
        with pytest.raises(NotImplementedError, match="casam"):
            _generate_modular_config(CAT_ID, tmp_path, monkeypatch, models=["casam"], routing=False)
