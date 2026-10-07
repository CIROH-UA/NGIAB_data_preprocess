"""Tests SUMMA configuration generation functions."""

import difflib
import json
import shutil
from datetime import datetime
from pathlib import Path
import os

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from data_processing.create_realization import (
    get_hru_order,
    make_summa_attributes,
    make_summa_trialParams,
    make_summa_coldState,
    make_summa_config,
)
from data_processing.file_paths import FilePaths

GOLDEN_GPKG_DIR = Path(__file__).parent / "golden" / "geopackage"
GOLDEN_CONFIG_DIR = Path(__file__).parent / "golden" / "config"
GOLDEN_SUMMA_FILE = GOLDEN_CONFIG_DIR / "cat-1555522-summa.json"
GOLDEN_GAGE_SUMMA_FILE = GOLDEN_CONFIG_DIR / "gage-10109001-summa.json"
GOLDEN_NC_DIR = Path(__file__).parent / "golden" / "netcdf"
GEOPACKAGE_FIXTURES = {
    "cat-1555522": GOLDEN_GPKG_DIR / "cat-1555522_subset.gpkg",
    "gage-10109001": GOLDEN_GPKG_DIR / "gage-10109001_subset.gpkg",
}
START_DATE = "2020-01-01"
END_DATE = "2020-01-02"
FIXED_START = datetime(2020, 1, 1, 0, 0, 0)
FIXED_END = datetime(2020, 1, 2, 0, 0, 0)

UPDATE_GOLDEN = os.environ.get("UPDATE_GOLDEN") == "1"


def _normalize(text: str, output_dir: Path) -> str:
    return text.replace(str(output_dir), "<OUTPUT_DIR>")


def _check_against_golden(produced: dict, golden_file: Path, label: str) -> None:
    """Compare produced config text to a golden JSON, or rewrite it if UPDATE_GOLDEN=1."""
    if UPDATE_GOLDEN:
        golden_file.parent.mkdir(parents=True, exist_ok=True)
        golden_file.write_text(json.dumps(produced, indent=2, sort_keys=True) + "\n")
        pytest.skip(f"UPDATE_GOLDEN=1: rewrote {golden_file}")

    assert golden_file.exists(), (
        f"missing golden {golden_file}. Generate it with: "
        "UPDATE_GOLDEN=1 uv run pytest tests/test_summa_config_generation.py"
    )
    golden = json.loads(golden_file.read_text())

    missing = sorted(set(golden) - set(produced))
    extra = sorted(set(produced) - set(golden))
    assert not missing and not extra, (
        f"SUMMA config file set changed for {label}.\n  missing: {missing}\n  extra: {extra}"
    )

    changed = [p for p in sorted(golden) if golden[p] != produced[p]]
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
            f"{len(changed)} SUMMA config file(s) changed for {label}: {changed}\n"
            f"first diff ({first}):\n{diff}"
        )


GAGE_FORCING_IDS = [
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
GAGE_HRU_IDS = [int(gid.split("-")[1]) for gid in GAGE_FORCING_IDS]


def _filter_dataset_dict(dataset_dict: dict) -> dict:
    filtered = {
        "coords": {},
        "dims": dataset_dict.get("dims", {}),
        "data_vars": {},
    }

    for name, item in dataset_dict.get("coords", {}).items():
        filtered["coords"][name] = {
            "dims": item["dims"],
            "data": item["data"],
        }

    for name, item in dataset_dict.get("data_vars", {}).items():
        filtered["data_vars"][name] = {
            "dims": item["dims"],
            "data": item["data"],
        }

    return filtered


def _assert_dataset_matches_expected(ds: xr.Dataset, expected: dict, rtol=1e-9, atol=1e-12):
    actual = _filter_dataset_dict(ds.to_dict(data=True))
    want = _filter_dataset_dict(expected)

    # structure is exact
    assert actual["dims"] == want["dims"], f"dims differ: {actual['dims']} != {want['dims']}"
    assert set(actual["coords"]) == set(want["coords"]), "coord set differs"
    assert set(actual["data_vars"]) == set(want["data_vars"]), "data_var set differs"

    for section in ("coords", "data_vars"):
        for name, want_var in want[section].items():
            got = actual[section][name]
            assert got["dims"] == want_var["dims"], f"{name}: dims differ"
            got_arr = np.asarray(got["data"])
            want_arr = np.asarray(want_var["data"])
            assert got_arr.shape == want_arr.shape, f"{name}: shape differs"
            # floats (pyproj lon/lat) get tolerance; ints (IDs, soil/veg type codes,
            # downHRUindex) stay exact so a real off-by-one isn't masked.
            if np.issubdtype(want_arr.dtype, np.floating) or np.issubdtype(
                got_arr.dtype, np.floating
            ):
                np.testing.assert_allclose(
                    got_arr,
                    want_arr,
                    rtol=rtol,
                    atol=atol,
                    err_msg=f"{name}: values differ beyond tolerance",
                )
            else:
                assert np.array_equal(got_arr, want_arr), f"{name}: {got_arr} != {want_arr}"


def _json_default(obj):
    """Serialize numpy scalars/arrays that can appear in netCDF attrs."""
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"not JSON serializable: {type(obj)}")


def _dataset_to_golden(ds: xr.Dataset) -> dict:
    """Dataset -> golden dict. History is dropped because it's a generation timestamp."""
    golden = _filter_dataset_dict(ds.to_dict(data=True))
    golden["attrs"] = {k: v for k, v in ds.attrs.items() if k != "History"}
    return golden


def _load_dataset_golden(golden_file: Path) -> dict:
    """Load a golden dict, restoring dims to tuples so they compare equal to xarray's."""
    golden = json.loads(golden_file.read_text())
    for section in ("coords", "data_vars"):
        for var in golden.get(section, {}).values():
            var["dims"] = tuple(var["dims"])
    return golden


def _check_dataset_against_golden(ds: xr.Dataset, golden_file: Path) -> None:
    """Compare a dataset to its golden JSON, or rewrite the golden if UPDATE_GOLDEN=1."""
    if UPDATE_GOLDEN:
        golden_file.parent.mkdir(parents=True, exist_ok=True)
        golden_file.write_text(
            json.dumps(_dataset_to_golden(ds), indent=2, sort_keys=True, default=_json_default)
            + "\n"
        )
        pytest.skip(f"UPDATE_GOLDEN=1: rewrote {golden_file}")

    assert golden_file.exists(), (
        f"missing golden {golden_file}. Generate it with: "
        "UPDATE_GOLDEN=1 uv run pytest tests/test_summa_config_generation.py"
    )
    expected = _load_dataset_golden(golden_file)
    _assert_dataset_matches_expected(ds, expected)
    for key, value in expected.get("attrs", {}).items():
        assert ds.attrs.get(key) == value, f"attr {key!r}: {ds.attrs.get(key)!r} != {value!r}"


def _generate_summa_config(cat_id: str, forcing_path: Path, tmp_root: Path, monkeypatch) -> dict:
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: Path(tmp_root)))
    output_dir = Path(tmp_root) / "config"

    summa_model_config = output_dir / "model_config" / "SUMMA"
    summa_model_config.mkdir(parents=True, exist_ok=True)

    for path in FilePaths.summa_file_dir.glob("*"):
        if not path.is_file() or path.suffix == ".nc":
            continue
        if path.name == "fileManager.txt":
            template = path.read_text()
            (summa_model_config / path.name).write_text(
                template.format(
                    start_time=FIXED_START.strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=FIXED_END.strftime("%Y-%m-%d %H:%M:%S"),
                )
            )
        else:
            shutil.copy(path, summa_model_config / path.name)

    hru_ids = get_hru_order(forcing_path)
    make_summa_attributes(hru_ids, GEOPACKAGE_FIXTURES[cat_id]).to_netcdf(
        summa_model_config / "attributes.nc"
    )
    make_summa_trialParams(
        hru_ids, int((FIXED_END - FIXED_START).total_seconds() / 3600)
    ).to_netcdf(summa_model_config / "trialParams.nc")
    ds, encoding = make_summa_coldState(hru_ids)
    ds.to_netcdf(summa_model_config / "coldState.nc", encoding=encoding)
    make_summa_config(hru_ids, output_dir)

    produced = {}
    for f in sorted(output_dir.rglob("*")):
        if f.is_file() and f.suffix != ".nc":
            produced[str(f.relative_to(output_dir))] = _normalize(
                f.read_text(encoding="utf-8", errors="replace"), output_dir
            )
    return produced


@pytest.fixture(name="cat_1555522_forcing_output")
def cat_1555522_forcing_output_fixture(tmp_path, monkeypatch):
    """Sets up reference forcing file."""
    working_dir = tmp_path / "ngiab_work"
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: working_dir))
    cat_id = "cat-1555522"

    forcing_dir = working_dir / cat_id / "forcings"
    forcing_dir.mkdir(parents=True, exist_ok=True)
    forcing_path = forcing_dir / "forcings.nc"

    time = pd.date_range(START_DATE, END_DATE, freq="h")
    ids = ["cat-1555522"]
    ds = xr.Dataset(
        {
            "APCP_surface": (("catchment-id", "time"), np.zeros((1, len(time)), dtype=np.float32)),
            "DSWRF_surface": (("catchment-id", "time"), np.zeros((1, len(time)), dtype=np.float32)),
        },
        coords={
            "catchment-id": ids,
            "time": time,
            "ids": (("catchment-id",), np.array(ids, dtype="U11")),
        },
    )
    ds.to_netcdf(forcing_path)

    return {
        "output_dir": working_dir / cat_id,
        "forcings_nc": forcing_path,
    }


@pytest.fixture(name="gage_10109001_forcing_output")
def gage_10109001_forcing_output_fixture(tmp_path, monkeypatch):
    """Synthetic forcings for the 88-catchment gage. IDs MUST be in GAGE_FORCING_IDS
    order -- get_hru_order preserves forcing order, which sets attrib_file_HRU_order."""
    working_dir = tmp_path / "ngiab_work"
    monkeypatch.setattr(FilePaths, "get_working_dir", classmethod(lambda cls: working_dir))
    cat_id = "gage-10109001"

    forcing_dir = working_dir / cat_id / "forcings"
    forcing_dir.mkdir(parents=True, exist_ok=True)
    forcing_path = forcing_dir / "forcings.nc"

    time = pd.date_range(START_DATE, END_DATE, freq="h")
    ids = GAGE_FORCING_IDS
    n = len(ids)
    ds = xr.Dataset(
        {
            "APCP_surface": (("catchment-id", "time"), np.zeros((n, len(time)), dtype=np.float32)),
            "DSWRF_surface": (("catchment-id", "time"), np.zeros((n, len(time)), dtype=np.float32)),
        },
        coords={
            "catchment-id": ids,
            "time": time,
            "ids": (("catchment-id",), np.array(ids, dtype="U11")),
        },
    )
    ds.to_netcdf(forcing_path)
    return {"output_dir": working_dir / cat_id, "forcings_nc": forcing_path}


def test_get_hru_order_returns_expected_ids(cat_1555522_forcing_output):
    """Checks get_hru_order."""
    ids = get_hru_order(cat_1555522_forcing_output["forcings_nc"])
    assert ids == [1555522]


def test_make_summa_attributes_netcdf_matches_expected(tmp_path):
    """Checks attributes.nc."""
    ds = make_summa_attributes([1555522], GEOPACKAGE_FIXTURES["cat-1555522"])
    output_path = tmp_path / "attributes.nc"
    ds.to_netcdf(output_path)

    with xr.open_dataset(output_path) as actual_ds:
        _check_dataset_against_golden(actual_ds, GOLDEN_NC_DIR / "cat-1555522-attributes.json")


def test_make_summa_coldState_netcdf_matches_expected(tmp_path):  # pylint: disable=invalid-name
    """Checks coldState.nc."""
    ds, encoding = make_summa_coldState([1555522])
    output_path = tmp_path / "coldState.nc"
    ds.to_netcdf(output_path, encoding=encoding)

    with xr.open_dataset(output_path) as actual_ds:
        _check_dataset_against_golden(actual_ds, GOLDEN_NC_DIR / "cat-1555522-coldState.json")


def test_make_summa_trialParams_netcdf_matches_expected(tmp_path):  # pylint: disable=invalid-name
    """Checks trialParams.nc."""
    ds = make_summa_trialParams([1555522], int((FIXED_END - FIXED_START).total_seconds() / 3600))
    output_path = tmp_path / "trialParams.nc"
    ds.to_netcdf(output_path)

    with xr.open_dataset(output_path) as actual_ds:
        _check_dataset_against_golden(actual_ds, GOLDEN_NC_DIR / "cat-1555522-trialParams.json")


def test_summa_config_generation_matches_golden(cat_1555522_forcing_output, tmp_path, monkeypatch):
    """Checks all non-netCDF config files."""
    produced = _generate_summa_config(
        "cat-1555522", cat_1555522_forcing_output["forcings_nc"], tmp_path, monkeypatch
    )
    _check_against_golden(produced, GOLDEN_SUMMA_FILE, "cat-1555522")


def test_summa_config_generation_produces_expected_artifacts(
    cat_1555522_forcing_output, tmp_path, monkeypatch
):
    """Checks that all non-netCDF files exist."""
    produced = _generate_summa_config(
        "cat-1555522", cat_1555522_forcing_output["forcings_nc"], tmp_path, monkeypatch
    )
    assert "cat_config/SUMMA/cat-1555522.input" in produced
    assert "model_config/SUMMA/fileManager.txt" in produced
    assert "model_config/SUMMA/README.md" in produced
    assert "model_config/SUMMA/forcingFileList.txt" in produced
    assert "model_config/SUMMA/basinParamInfo.txt" in produced
    assert "model_config/SUMMA/localParamInfo.txt" in produced
    assert "model_config/SUMMA/modelDecisions.txt" in produced
    assert "model_config/SUMMA/outputControl.txt" in produced
    assert "model_config/SUMMA/TBL_GENPARM.TBL" in produced
    assert "model_config/SUMMA/TBL_MPTABLE.TBL" in produced
    assert "model_config/SUMMA/TBL_SOILPARM.TBL" in produced
    assert "model_config/SUMMA/TBL_VEGPARM.TBL" in produced


def test_gage_get_hru_order_returns_expected_ids(gage_10109001_forcing_output):
    """Checks get_hru_order for a multi-catchment simulation."""
    ids = get_hru_order(gage_10109001_forcing_output["forcings_nc"])
    assert ids == GAGE_HRU_IDS


def test_gage_make_summa_attributes_matches_golden(tmp_path):
    """Checks attributes.nc for a multi-catchment simulation."""
    ds = make_summa_attributes(GAGE_HRU_IDS, GEOPACKAGE_FIXTURES["gage-10109001"])
    output_path = tmp_path / "attributes.nc"
    ds.to_netcdf(output_path)

    with xr.open_dataset(output_path) as actual:
        assert actual.sizes["hru"] == len(GAGE_HRU_IDS)
        assert actual.sizes["gru"] == len(GAGE_HRU_IDS)
        assert actual["hruId"].values.tolist() == GAGE_HRU_IDS
        _check_dataset_against_golden(actual, GOLDEN_NC_DIR / "gage-10109001-attributes.json")


def test_gage_make_summa_coldState_matches_golden(tmp_path):  # pylint: disable=invalid-name
    """Checks coldState.nc for a multi-catchment simulation."""
    ds, encoding = make_summa_coldState(GAGE_HRU_IDS)
    output_path = tmp_path / "coldState.nc"
    ds.to_netcdf(output_path, encoding=encoding)

    with xr.open_dataset(output_path) as actual:
        assert actual.sizes["hru"] == len(GAGE_HRU_IDS)
        assert actual["hruId"].values.tolist() == GAGE_HRU_IDS
        _check_dataset_against_golden(actual, GOLDEN_NC_DIR / "gage-10109001-coldState.json")


def test_gage_make_summa_trialParams_matches_golden(tmp_path):  # pylint: disable=invalid-name
    """Checks trialParams.nc for a multi-catchment simulation."""
    timesteps = int((FIXED_END - FIXED_START).total_seconds() / 3600)
    ds = make_summa_trialParams(GAGE_HRU_IDS, timesteps)
    output_path = tmp_path / "trialParams.nc"
    ds.to_netcdf(output_path)

    with xr.open_dataset(output_path) as actual:
        assert actual.sizes["hru"] == len(GAGE_HRU_IDS)
        assert actual["hruId"].values.tolist() == GAGE_HRU_IDS
        np.testing.assert_allclose(actual["maxstep"].values, timesteps * 3600)
        _check_dataset_against_golden(actual, GOLDEN_NC_DIR / "gage-10109001-trialParams.json")


def test_gage_summa_config_generation_matches_golden(
    gage_10109001_forcing_output, tmp_path, monkeypatch
):
    """Checks that the multi-catchment non-netCDF files match the reference."""
    produced = _generate_summa_config(
        "gage-10109001", gage_10109001_forcing_output["forcings_nc"], tmp_path, monkeypatch
    )
    _check_against_golden(produced, GOLDEN_GAGE_SUMMA_FILE, "gage-10109001")


def test_gage_summa_config_generation_produces_expected_artifacts(
    gage_10109001_forcing_output, tmp_path, monkeypatch
):
    """Checks that all non-netCDF files for a multi-catchment simulation were generated."""
    produced = _generate_summa_config(
        "gage-10109001", gage_10109001_forcing_output["forcings_nc"], tmp_path, monkeypatch
    )
    cat_inputs = [k for k in produced if k.startswith("cat_config/SUMMA/") and k.endswith(".input")]
    assert len(cat_inputs) == len(GAGE_HRU_IDS)
    assert "cat_config/SUMMA/cat-2861379.input" in produced  # first in HRU order
    assert "cat_config/SUMMA/cat-2861416.input" in produced  # last in HRU order
    for name in (
        "fileManager.txt",
        "README.md",
        "forcingFileList.txt",
        "basinParamInfo.txt",
        "localParamInfo.txt",
        "modelDecisions.txt",
        "outputControl.txt",
        "TBL_GENPARM.TBL",
        "TBL_MPTABLE.TBL",
        "TBL_SOILPARM.TBL",
        "TBL_VEGPARM.TBL",
    ):
        assert f"model_config/SUMMA/{name}" in produced
