"""Shared golden-comparison helpers for the config/realization regression suites.

Nothing in this file collects as a test (no ``test_*`` names).
"""

import json
import math
import re
from datetime import datetime
from pathlib import Path

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

NUMERIC_RE = re.compile(r"(-?(?:\d+\.\d*|\.\d+|\d+)(?:[eE][-+]?\d+)?)")

SUMMA_GAGE_FORCING_IDS = [
    "cat-2861379",
    "cat-2861380",
    "cat-2861387",
    "cat-2861414",
    "cat-2861421",
    "cat-2861429",
    "cat-2861431",
    "cat-2861436",
    "cat-2861438",
    "cat-2861442",
    "cat-2861446",
    "cat-2861447",
    "cat-2861449",
    "cat-2861452",
    "cat-2861453",
    "cat-2861471",
    "cat-2861472",
    "cat-2861475",
    "cat-2861488",
    "cat-2861382",
    "cat-2861383",
    "cat-2861384",
    "cat-2861385",
    "cat-2861388",
    "cat-2861389",
    "cat-2861391",
    "cat-2861419",
    "cat-2861420",
    "cat-2861423",
    "cat-2861427",
    "cat-2861430",
    "cat-2861433",
    "cat-2861439",
    "cat-2861457",
    "cat-2861458",
    "cat-2861468",
    "cat-2861477",
    "cat-2861478",
    "cat-2861485",
    "cat-2861474",
    "cat-2861487",
    "cat-2861476",
    "cat-2861486",
    "cat-2861484",
    "cat-2861480",
    "cat-2861482",
    "cat-2861481",
    "cat-2861483",
    "cat-2861479",
    "cat-2861473",
    "cat-2861470",
    "cat-2861469",
    "cat-2861467",
    "cat-2861466",
    "cat-2861381",
    "cat-2861462",
    "cat-2861464",
    "cat-2861463",
    "cat-2861465",
    "cat-2861461",
    "cat-2861459",
    "cat-2861460",
    "cat-2861455",
    "cat-2861456",
    "cat-2861454",
    "cat-2861451",
    "cat-2861450",
    "cat-2861448",
    "cat-2861445",
    "cat-2861444",
    "cat-2861441",
    "cat-2861443",
    "cat-2861440",
    "cat-2861437",
    "cat-2861435",
    "cat-2861434",
    "cat-2861432",
    "cat-2861386",
    "cat-2861428",
    "cat-2861426",
    "cat-2861425",
    "cat-2861424",
    "cat-2861390",
    "cat-2861422",
    "cat-2861415",
    "cat-2861417",
    "cat-2861418",
    "cat-2861416",
]


def normalize(text: str, output_dir: Path) -> str:
    """Strip the few non-deterministic / machine-specific bits.

    - troute.yaml embeds ``multiprocessing.cpu_count()`` in ``cpu_pool``.
    - any absolute path under the temp output dir (defensive; the templates use
    relative paths, but normalize in case that changes).
    """
    text = text.replace(str(output_dir), "<OUTPUT_DIR>")
    text = re.sub(r"cpu_pool:\s*\d+", "cpu_pool: <CPU>", text)
    return text


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


def config_text_equivalent(golden_text: str, produced_text: str) -> bool:
    """True if two config files are equal up to float tolerance (JSON or text)."""
    if golden_text == produced_text:
        return True

    try:
        golden_data = json.loads(golden_text)
        produced_data = json.loads(produced_text)
    except json.JSONDecodeError:
        return _compare_text_tolerant(golden_text, produced_text)
    return _compare_json_tolerant(golden_data, produced_data)


def write_golden_json(golden_path: Path, produced: dict) -> None:
    """Write a ``{rel_path: text}`` golden using the committed serialization."""
    golden_path.parent.mkdir(parents=True, exist_ok=True)
    golden_path.write_text(json.dumps(produced, indent=2, sort_keys=True) + "\n")
