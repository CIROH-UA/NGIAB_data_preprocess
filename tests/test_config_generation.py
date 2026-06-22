"""Config-generation regression tests.

Runs the default ``create_realization`` (NOAH-OWP + CFE) against the two committed
subset geopackages and snapshots every generated config file (per-catchment CFE
``.ini`` and NOAH-OWP ``.input``, ``troute.yaml``, ``realization.json``) against a
golden.

Hermetic: the default builder reads catchment attributes straight from the
geopackage (``divides`` + ``divide-attributes`` via SQL) and touches no network,
so this runs in the fast unit tier. (snow17 / sacsma / dhbv2 are intentionally
NOT covered here -- each calls ``download_*_attributes()`` and pulls extra parquet
files from S3, which would make them integration tests. Add them once those
attribute files are also committed as fixtures.)

Fixture geopackages:
    tests/golden/geopackage/cat-1555522_subset.gpkg
    tests/golden/geopackage/gage-10109001_subset.gpkg
"""

import difflib
import json
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from data_processing.create_realization import create_realization
from data_processing.file_paths import FilePaths

GOLDEN_GPKG_DIR = Path(__file__).parent / "golden" / "geopackage"
GOLDEN_CONFIG_DIR = Path(__file__).parent / "golden" / "config"

# input id -> committed subset geopackage
GEOPACKAGE_FIXTURES = {
    "cat-1555522": GOLDEN_GPKG_DIR / "cat-1555522_subset.gpkg",
    "gage-10109001": GOLDEN_GPKG_DIR / "gage-10109001_subset.gpkg",
}

# Fixed so config output (NOAH dates, troute nts) is deterministic.
START = datetime(2020, 1, 1, 0, 0, 0)
END = datetime(2020, 1, 2, 0, 0, 0)


def _normalize(text: str, output_dir: Path) -> str:
    """Strip the few non-deterministic / machine-specific bits.

    - troute.yaml embeds multiprocessing.cpu_count() in `cpu_pool`.
    - any absolute path under the temp output dir (defensive; the templates use
    relative paths, but normalize in case that changes).
    """
    text = text.replace(str(output_dir), "<OUTPUT_DIR>")
    text = re.sub(r"cpu_pool:\s*\d+", "cpu_pool: <CPU>", text)
    return text


def _generate_config(cat_id: str, tmp_root: Path, monkeypatch) -> dict:
    """Run the default builder offline and return {relative_path: normalized_text}."""
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: Path(tmp_root)))

    paths = FilePaths(cat_id)
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPACKAGE_FIXTURES[cat_id], paths.geopackage_path)

    # Default model engine: NOAH-OWP + CFE. Offline (no gw download, no gage params).
    create_realization(cat_id, START, END, use_nwm_gw=False, gage_id=None)

    produced = {}
    for f in sorted(paths.config_dir.rglob("*")):
        if f.is_file() and f.suffix != ".gpkg":
            rel = str(f.relative_to(paths.config_dir))
            produced[rel] = _normalize(
                f.read_text(errors="replace"),
                paths.output_dir,  # type: ignore
            )
    return produced


@pytest.fixture(name="require")
def require_fixture():
    """Checks if required geopackages are available."""

    def _check(cat_id):
        if not GEOPACKAGE_FIXTURES[cat_id].exists():
            pytest.skip(
                f"missing geopackage fixture {GEOPACKAGE_FIXTURES[cat_id]}. "
                "Save it there or edit GEOPACKAGE_FIXTURES."
            )

    return _check


@pytest.mark.parametrize("cat_id", list(GEOPACKAGE_FIXTURES))
def test_config_generation_matches_golden(cat_id, tmp_path, monkeypatch, require):
    """Checks generated configs against golden files."""

    require(cat_id)
    produced = _generate_config(cat_id, tmp_path, monkeypatch)
    golden_file = GOLDEN_CONFIG_DIR / f"{cat_id}.json"

    if os.environ.get("UPDATE_GOLDEN"):
        golden_file.parent.mkdir(parents=True, exist_ok=True)
        golden_file.write_text(json.dumps(produced, indent=2, sort_keys=True) + "\n")
        pytest.skip(f"UPDATE_GOLDEN set -- wrote {golden_file.name} ({len(produced)} files)")

    assert golden_file.exists(), (
        f"missing golden {golden_file}. Generate it with: "
        f"UPDATE_GOLDEN=1 uv run pytest tests/test_config_generation.py"
    )
    golden = json.loads(golden_file.read_text())

    # 1) the set of generated files must match
    missing = sorted(set(golden) - set(produced))
    extra = sorted(set(produced) - set(golden))
    assert not missing and not extra, (
        f"config file set changed for {cat_id}.\n  missing: {missing}\n  extra: {extra}"
    )

    # 2) each file's (normalized) content must match
    changed = [p for p in sorted(golden) if produced[p] != golden[p]]
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
            f"{len(changed)} config file(s) changed for {cat_id}: {changed}\n"
            f"If intentional, regenerate with UPDATE_GOLDEN=1 and commit.\n\n"
            f"first diff ({first}):\n{diff}"
        )


@pytest.mark.parametrize("cat_id", list(GEOPACKAGE_FIXTURES))
def test_config_generation_produces_expected_artifacts(cat_id, tmp_path, monkeypatch, require):
    """Structural guard, independent of the golden: the core artifacts exist."""
    require(cat_id)
    produced = _generate_config(cat_id, tmp_path, monkeypatch)
    keys = list(produced)
    assert "realization.json" in keys
    assert "troute.yaml" in keys
    assert any(k.startswith("cat_config/CFE/") and k.endswith(".ini") for k in keys), (
        "no CFE configs"
    )
    assert any(k.startswith("cat_config/NOAH-OWP-M/") and k.endswith(".input") for k in keys), (
        "no NOAH configs"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
