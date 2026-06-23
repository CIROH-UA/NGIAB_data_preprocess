"""Config-generation regression tests.

Fixture geopackages:
    tests/golden/geopackage/cat-1555522_subset.gpkg
    tests/golden/geopackage/gage-10109001_subset.gpkg
"""

import difflib
import json
import math
import os
import re
import shutil
from datetime import datetime
from pathlib import Path

import pytest

from data_processing.create_realization import (
    get_model_attributes,
    make_cfe_config,
    make_noahowp_config,
    make_dhbv2_config,
    make_lstm_config,
    make_sacsma_config,
    make_snow17_config,
    configure_troute,
)
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


NUMERIC_RE = re.compile(r"(-?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?)")


def _compare_number_tokens(a: str, b: str, rel_tol: float = 1e-6, abs_tol: float = 1e-9) -> bool:
    try:
        return math.isclose(float(a), float(b), rel_tol=rel_tol, abs_tol=abs_tol)
    except ValueError:
        return False


def _is_float_token(tok: str) -> bool:
    """Whether a numeric token should get tolerant comparison.

    Only real floats (those with a decimal point or exponent) drift across
    platforms. Bare integers -- catchment IDs, ISLTYP/IVGTYP codes, nts, counts --
    are compared exactly so a genuine off-by-one change isn't masked by tolerance.
    """
    return any(c in tok for c in ".eE")


def _compare_line_tolerant(a: str, b: str) -> bool:
    if a == b:
        return True

    a_parts = NUMERIC_RE.split(a)
    b_parts = NUMERIC_RE.split(b)
    if len(a_parts) != len(b_parts):
        return False

    for a_part, b_part in zip(a_parts, b_parts):
        if a_part == b_part:
            continue
        # Tolerance applies ONLY when both tokens are floats. Differing text,
        # or differing integers, must match exactly.
        if (
            NUMERIC_RE.fullmatch(a_part)
            and NUMERIC_RE.fullmatch(b_part)
            and _is_float_token(a_part)
            and _is_float_token(b_part)
        ):
            if not _compare_number_tokens(a_part, b_part):
                return False
        else:
            return False
    return True


def _compare_text_tolerant(a: str, b: str) -> bool:
    if a == b:
        return True

    a_lines = a.splitlines()
    b_lines = b.splitlines()
    if len(a_lines) != len(b_lines):
        return False

    return all(_compare_line_tolerant(a_line, b_line) for a_line, b_line in zip(a_lines, b_lines))


def _compare_json_tolerant(a, b) -> bool:
    """Compare JSON-like structures with numeric tolerance for floats."""
    if type(a) is not type(b):
        return False

    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_compare_json_tolerant(a[k], b[k]) for k in a)

    if isinstance(a, list):
        return len(a) == len(b) and all(_compare_json_tolerant(av, bv) for av, bv in zip(a, b))

    if isinstance(a, bool):
        return a == b

    if isinstance(a, (int, float)):
        return (
            a == b
            if isinstance(a, int)
            else math.isclose(float(a), float(b), rel_tol=1e-6, abs_tol=1e-9)
        )

    return a == b


def _config_text_equivalent(golden_text: str, produced_text: str) -> bool:
    if golden_text == produced_text:
        return True

    try:
        golden_data = json.loads(golden_text)
        produced_data = json.loads(produced_text)
    except json.JSONDecodeError:
        return _compare_text_tolerant(golden_text, produced_text)
    return _compare_json_tolerant(golden_data, produced_data)


def _generate_config(cat_id: str, tmp_root: Path, monkeypatch) -> dict:
    """Run the default builder and return {relative_path: normalized_text}."""
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: Path(tmp_root)))

    paths = FilePaths(cat_id)
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPACKAGE_FIXTURES[cat_id], paths.geopackage_path)

    conf_df = get_model_attributes(paths.geopackage_path)
    make_cfe_config(conf_df, paths, {})
    make_noahowp_config(paths.config_dir, conf_df, START, END)
    make_snow17_config(paths.config_dir, conf_df, START, END)
    make_sacsma_config(paths.config_dir, conf_df, START, END)
    make_lstm_config(paths.geopackage_path, paths.config_dir)
    make_dhbv2_config(paths.geopackage_path, paths.config_dir, START, END)
    make_dhbv2_config(
        paths.geopackage_path,
        paths.config_dir,
        START,
        END,
        template_path=FilePaths.template_dhbv2_daily_config,
    )
    configure_troute(cat_id, paths.config_dir, START, END)

    produced = {}
    for f in sorted(paths.config_dir.rglob("*")):
        if f.is_file() and f.suffix != ".gpkg" and f.name != "realization.json":
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

    # 2) each file's (normalized) content must match, with numeric tolerance.
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
    assert "troute.yaml" in keys
    assert any(k.startswith("cat_config/CFE/") and k.endswith(".ini") for k in keys), (
        "no CFE configs"
    )
    assert any(k.startswith("cat_config/NOAH-OWP-M/") and k.endswith(".input") for k in keys), (
        "no NOAH configs"
    )
    assert any(k.startswith("cat_config/SAC-SMA/cat-") and k.endswith(".input") for k in keys), (
        "no SAC-SMA configs"
    )
    assert any(k.startswith("cat_config/SAC-SMA/params-") and k.endswith(".txt") for k in keys), (
        "no SAC-SMA params"
    )
    assert any(k.startswith("cat_config/SNOW17/cat-") and k.endswith(".input") for k in keys), (
        "no SNOW17 configs"
    )
    assert any(k.startswith("cat_config/SNOW17/params-") and k.endswith(".txt") for k in keys), (
        "no SNOW17 params"
    )
    assert any(k.startswith("cat_config/lstm/") and k.endswith(".yml") for k in keys), (
        "no LSTM configs"
    )
    assert any(k.startswith("cat_config/dhbv2/") and k.endswith(".yml") for k in keys), (
        "no dHBV2 hourly configs"
    )
    assert any(k.startswith("cat_config/dhbv2_daily/") and k.endswith(".yml") for k in keys), (
        "no dHBV2 daily configs"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
