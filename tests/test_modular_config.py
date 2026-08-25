"""Config-generation tests for ``create_configs`` (the orchestration wrapper)."""

import difflib
import json
import re
import shutil
from pathlib import Path

import pytest

from data_processing.file_paths import FilePaths
from data_processing.create_configs import create_modular_configs

from golden_utils import (
    GEOPACKAGE_FIXTURES,
    GOLDEN_CONFIG_DIR,
    START,
    END,
    config_text_equivalent,
    normalize,
    write_golden_json,
)

pytestmark = pytest.mark.integration


def test_write_golden_json_writes_sorted_json(tmp_path):
    """The helper should serialize dict goldens with stable formatting."""
    golden_path = tmp_path / "sample.json"
    payload = {"b": 2, "a": {"d": 4, "c": 3}}

    write_golden_json(golden_path, payload)

    assert golden_path.read_text() == '{\n  "a": {\n    "c": 3,\n    "d": 4\n  },\n  "b": 2\n}\n'


# A small, fast fixture used by the single-catchment orchestration tests.
CAT_ID = "cat-1555522"

# The models whose output is baked into the golden {cat_id}.json files. sloth
# produces no file; summa IS now supported by create_modular_configs (it dispatches
# to the SUMMA config suite) but is deliberately excluded here: the goldens predate
# summa support and summa needs forcings.nc + a hydrofabric fixture, so it has its
# own dedicated suite in test_summa_config_generation.py. pet/sft/smp/topmodel are
# not supported yet.

ALL_CONFIG_MODELS = ["cfe", "nom", "snow17", "sac-sma", "lstm", "dhbv2", "dhbv2_daily", "casam"]


def _generate_modular_config(cat_id, tmp_root, monkeypatch, *, models, routing=False):
    """Run ``create_modular_configs`` and return ``{relative_path: normalized_text}``.

    Patches get_working_dir, seeds the geopackage, and normalizes machine-specific
    bits, then drives the build through the public modular wrapper.
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
            produced[rel] = normalize(
                f.read_text(errors="replace"),
                paths.output_dir,  # type: ignore
            )

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
# Headline: the wrapper reproduces the config golden.
# ===========================================================================
@pytest.mark.parametrize("cat_id", list(GEOPACKAGE_FIXTURES))
def test_modular_configs_match_golden(cat_id, tmp_path, monkeypatch, require):
    """The full supported model set (routing on) must reproduce {cat_id}.json,
    proving the wrapper delegates to the makers with the correct arguments.

    Regenerate with ``uv run python tests/golden/regen_realization_goldens.py``
    (needs S3 + the CONUS hydrofabric), then eyeball and commit the diff under
    tests/golden/config/.
    """
    require(cat_id)
    produced = _generate_modular_config(
        cat_id, tmp_path, monkeypatch, models=ALL_CONFIG_MODELS, routing=True
    )

    golden_file = GOLDEN_CONFIG_DIR / f"{cat_id}.json"
    assert golden_file.exists(), f"missing golden {golden_file}."

    golden = json.loads(golden_file.read_text())

    # 1) the set of generated files must match exactly
    missing = sorted(set(golden) - set(produced))
    extra = sorted(set(produced) - set(golden))
    assert not missing and not extra, (
        f"config file set differs for {cat_id}.\n  missing: {missing}\n  extra: {extra}"
    )

    # 2) each file's (normalized) content must match, with numeric tolerance
    changed = [p for p in sorted(golden) if not config_text_equivalent(golden[p], produced[p])]
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

    def test_troute_nested_output_folder_uses_leaf_name(self, tmp_path, monkeypatch, require):
        """A nested output_folder (containing a path separator) must not leak into
        the on-disk gpkg lookup or the troute.yaml geo_file_path -- both must
        resolve to the bare leaf name, matching how FilePaths.geopackage_path
        always builds the real gpkg filename from the leaf-only folder_name.

        Regression test for the path-handling bug fixed on main in #228
        ("Fix output_name path handling"), which was silently reintroduced when
        config generation was split out into create_configs.py.
        """
        require(CAT_ID)
        monkeypatch.chdir(tmp_path)
        nested_id = f"nested/{CAT_ID}"

        # FilePaths resolves a multi-segment folder_name relative to cwd (not
        # get_working_dir()), so chdir into tmp_path keeps this hermetic. The gpkg
        # itself still only ever lives at paths.geopackage_path (leaf-name only).
        paths = FilePaths(nested_id)
        paths.config_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(GEOPACKAGE_FIXTURES[CAT_ID], paths.geopackage_path)

        create_modular_configs(nested_id, START, END, ["cfe"], routing=True)

        troute_yaml = (paths.config_dir / "troute.yaml").read_text()
        match = re.search(r"geo_file_path:\s*(\S+)", troute_yaml)
        assert match, "troute.yaml missing geo_file_path"
        geo_file_path = match.group(1)
        assert geo_file_path == f"./config/{CAT_ID}_subset.gpkg"
        assert "nested" not in geo_file_path

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
        with pytest.raises(NotImplementedError, match="sft"):
            _generate_modular_config(CAT_ID, tmp_path, monkeypatch, models=["sft"], routing=False)
