"""One-off: regenerate realization and config goldens from the modular builder.

Mirrors the test fixtures in tests/test_modular_realization.py and
tests/test_modular_config.py exactly (same fixed START/END, routing on, Prompt.ask
stubbed, get_working_dir pointed at a temp dir). Writes a golden only when its
parsed content actually changed, so existing multi-model goldens remain byte-
untouched unless the produced content truly moved.
"""

import json
import shutil
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

sys.path.insert(0, "modules")

from data_processing.create_configs import (  # pylint: disable=wrong-import-position
    create_modular_configs,
)
from data_processing.file_paths import FilePaths  # pylint: disable=wrong-import-position

# import data_processing.create_realization as mr  # pylint: disable=wrong-import-position
from data_processing.create_realization import (  # pylint: disable=wrong-import-position
    create_modular_realization,
)
from test_modular_realization import GOLDEN_CASES

from golden_utils import (  # pylint: disable=wrong-import-position
    END,
    GEOPACKAGE_FIXTURES,
    GOLDEN_CONFIG_DIR,
    START,
    SUMMA_GAGE_FORCING_IDS,
    normalize,
    write_golden_json,
)

GOLDEN_DIR = Path("tests/golden/realization")

CONFIG_MODELS = ["cfe", "nom", "snow17", "sac-sma", "lstm", "dhbv2", "dhbv2_daily", "casam", "pet"]

# Stub the interactive overwrite prompt to always accept.
# mr.Prompt.ask = staticmethod(lambda *a, **k: "y")  # type: ignore


def _write_summa_forcing_fixture(forcing_path: Path, cat_id: str) -> None:
    forcing_path.parent.mkdir(parents=True, exist_ok=True)

    time = pd.date_range(START.strftime("%Y-%m-%d"), END.strftime("%Y-%m-%d"), freq="h")
    if cat_id == "cat-1555522":
        ids = ["cat-1555522"]
    else:
        ids = SUMMA_GAGE_FORCING_IDS

    ds = xr.Dataset(
        {
            "APCP_surface": (
                ("catchment-id", "time"),
                np.zeros((len(ids), len(time)), dtype=np.float32),
            ),
            "DSWRF_surface": (
                ("catchment-id", "time"),
                np.zeros((len(ids), len(time)), dtype=np.float32),
            ),
        },
        coords={
            "catchment-id": ids,
            "time": time,
            "ids": (("catchment-id",), np.array(ids, dtype="U11")),
        },
    )
    ds.to_netcdf(forcing_path)


def _generate_config_golden(cat_id: str, tmp_root: str) -> dict:
    FilePaths.get_working_dir = classmethod(lambda cls, _tmp=tmp_root: Path(_tmp))  # type: ignore

    paths = FilePaths(cat_id)
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPACKAGE_FIXTURES[cat_id], paths.geopackage_path)

    create_modular_configs(cat_id, START, END, CONFIG_MODELS, routing=True)

    produced = {}
    for f in sorted(paths.config_dir.rglob("*")):
        if f.is_file() and f.suffix != ".gpkg" and f.name != "realization.json":
            rel = str(f.relative_to(paths.config_dir))
            produced[rel] = normalize(
                f.read_text(errors="replace"),
                paths.output_dir,  # type: ignore[arg-type]
            )
    return produced


def _generate_summa_golden(cat_id: str, tmp_root: str) -> dict:
    FilePaths.get_working_dir = classmethod(lambda cls, _tmp=tmp_root: Path(_tmp))  # type: ignore

    paths = FilePaths(cat_id)
    paths.config_dir.mkdir(parents=True, exist_ok=True)
    paths.forcings_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPACKAGE_FIXTURES[cat_id], paths.geopackage_path)
    _write_summa_forcing_fixture(paths.forcings_file, cat_id)
    FilePaths.conus_hydrofabric = GEOPACKAGE_FIXTURES[cat_id]  # type: ignore[attr-defined]

    create_modular_configs(cat_id, START, END, ["summa"], routing=False)

    produced = {}
    for f in sorted(paths.config_dir.rglob("*")):
        if (
            f.is_file()
            and f.suffix != ".nc"
            and f.name != "realization.json"
            and f.suffix != ".gpkg"
        ):
            rel = str(f.relative_to(paths.config_dir))
            produced[rel] = normalize(f.read_text(errors="replace"), paths.config_dir)
    return produced

def main():
    """regen goldens"""
    for models, golden_name, _ in GOLDEN_CASES:
        with tempfile.TemporaryDirectory() as tmp:
            FilePaths.get_working_dir = classmethod(lambda cls, _tmp=tmp: Path(_tmp))  # type: ignore
            cat_test_paths = FilePaths("cat-test")
            cat_test_paths.config_dir.mkdir(parents=True, exist_ok=True)
            create_modular_realization("cat-test", START, END, models, routing=True)
            produced_realizations = json.loads(
                (cat_test_paths.config_dir / "realization.json").read_text()
            )

        golden_path = GOLDEN_DIR / golden_name
        old = json.loads(golden_path.read_text()) if golden_path.exists() else None
        if old == produced_realizations:
            print(f"  unchanged  {golden_name}")
            continue
        # Match the committed golden serialization (2-space indent, trailing newline).
        write_golden_json(golden_path, produced_realizations)
        print(f"  UPDATED    {golden_name}")

    for cat_id_gpkg in GEOPACKAGE_FIXTURES:
        with tempfile.TemporaryDirectory() as tmp:
            produced_configs = _generate_config_golden(cat_id_gpkg, tmp)

        golden_path = GOLDEN_CONFIG_DIR / f"{cat_id_gpkg}.json"
        old = json.loads(golden_path.read_text()) if golden_path.exists() else None
        if old == produced_configs:
            print(f"  unchanged  {golden_path.name}")
            continue
        write_golden_json(golden_path, produced_configs)
        print(f"  UPDATED    {golden_path.name}")

    for cat_id_gpkg in GEOPACKAGE_FIXTURES:
        with tempfile.TemporaryDirectory() as tmp:
            produced_summa = _generate_summa_golden(cat_id_gpkg, tmp)

        golden_path = GOLDEN_CONFIG_DIR / f"{cat_id_gpkg}-summa.json"
        old = json.loads(golden_path.read_text()) if golden_path.exists() else None
        if old == produced_summa:
            print(f"  unchanged  {golden_path.name}")
            continue
        write_golden_json(golden_path, produced_summa)
        print(f"  UPDATED    {golden_path.name}")

    print("done")

if __name__ == "__main__":
    main()