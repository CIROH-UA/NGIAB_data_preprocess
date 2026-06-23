"""Tests SUMMA configuration generation functions."""

import difflib
import json
import shutil
from datetime import datetime
from pathlib import Path

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
GEOPACKAGE_FIXTURES = {
    "cat-1555522": GOLDEN_GPKG_DIR / "cat-1555522_subset.gpkg",
    "gage-10109001": GOLDEN_GPKG_DIR / "gage-10109001_subset.gpkg",
}
START_DATE = "2020-01-01"
END_DATE = "2020-01-02"
FIXED_START = datetime(2020, 1, 1, 0, 0, 0)
FIXED_END = datetime(2020, 1, 2, 0, 0, 0)


def _normalize(text: str, output_dir: Path) -> str:
    return text.replace(str(output_dir), "<OUTPUT_DIR>")


EXPECTED_SUMMA_ATTRIBUTES = {
    "coords": {},
    "attrs": {
        "Author": "Created by ngiab preprocessor SUMMA workflow script",
        "History": "Created 2026/06/23 11:08:48",
    },
    "dims": {"hru": 1, "gru": 1},
    "data_vars": {
        "hruId": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index of hydrological response unit (HRU)"},
            "data": [1555522],
        },
        "gruId": {
            "dims": ("gru",),
            "attrs": {"units": "-", "long_name": "Index of grouped response unit (GRU)"},
            "data": [1555522],
        },
        "hru2gruId": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index of GRU to which the HRU belongs"},
            "data": [1555522],
        },
        "downHRUindex": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index of downslope HRU (0 = basin outlet)"},
            "data": [0],
        },
        "longitude": {
            "dims": ("hru",),
            "attrs": {"units": "Decimal degree east", "long_name": "Longitude of HRUs centroid"},
            "data": [-95.61994939481728],
        },
        "latitude": {
            "dims": ("hru",),
            "attrs": {"units": "Decimal degree north", "long_name": "Latitude of HRUs centroid"},
            "data": [39.14351071467262],
        },
        "elevation": {
            "dims": ("hru",),
            "attrs": {"units": "m", "long_name": "Mean HRU elevation"},
            "data": [305.94258126046924],
        },
        "HRUarea": {
            "dims": ("hru",),
            "attrs": {"units": "m^2", "long_name": "Area of HRU"},
            "data": [9761400.035999982],
        },
        "tan_slope": {
            "dims": ("hru",),
            "attrs": {"units": "m m-1", "long_name": "Average tangent slope of HRU"},
            "data": [0.32140451308275],
        },
        "contourLength": {
            "dims": ("hru",),
            "attrs": {"units": "m", "long_name": "Contour length of HRU"},
            "data": [100.0],
        },
        "slopeTypeIndex": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index defining slope"},
            "data": [1],
        },
        "soilTypeIndex": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index defining soil type"},
            "data": [9],
        },
        "vegTypeIndex": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index defining vegetation type"},
            "data": [5],
        },
        "mHeight": {
            "dims": ("hru",),
            "attrs": {"units": "m", "long_name": "Measurement height above bare ground"},
            "data": [1.5],
        },
    },
}

EXPECTED_SUMMA_COLD_STATE = {
    "coords": {
        "midSoil": {"dims": ("midSoil",), "attrs": {}, "data": [0, 1, 2]},
    },
    "attrs": {
        "Author": "Created by SUMMA workflow scripts",
        "History": "Created 2026/06/23 11:08:48",
        "Purpose": "Create a cold state .nc file for initial SUMMA runs",
    },
    "dims": {"hru": 1, "scalarv": 1, "midToto": 3, "ifcToto": 4, "midSoil": 3},
    "data_vars": {
        "hruId": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index of hydrological response unit (HRU)"},
            "data": [1555522],
        },
        "dt_init": {
            "dims": ("scalarv", "hru"),
            "attrs": {},
            "data": [[3600.0]],
        },
        "nSoil": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[3]]},
        "nSnow": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0]]},
        "scalarCanopyIce": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0.0]]},
        "scalarCanopyLiq": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0.0]]},
        "scalarSnowDepth": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0.0]]},
        "scalarSWE": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0.0]]},
        "scalarSfcMeltPond": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0.0]]},
        "scalarAquiferStorage": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[2.5]]},
        "scalarSnowAlbedo": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[0.0]]},
        "scalarCanairTemp": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[283.16]]},
        "scalarCanopyTemp": {"dims": ("scalarv", "hru"), "attrs": {}, "data": [[283.16]]},
        "mLayerTemp": {
            "dims": ("midToto", "hru"),
            "attrs": {},
            "data": [[283.16], [283.16], [283.16]],
        },
        "mLayerVolFracIce": {
            "dims": ("midToto", "hru"),
            "attrs": {},
            "data": [[0.0], [0.0], [0.0]],
        },
        "mLayerVolFracLiq": {
            "dims": ("midToto", "hru"),
            "attrs": {},
            "data": [[0.2], [0.2], [0.2]],
        },
        "mLayerMatricHead": {
            "dims": ("midToto", "hru"),
            "attrs": {},
            "data": [[-1.0], [-1.0], [-1.0]],
        },
        "iLayerHeight": {
            "dims": ("ifcToto", "hru"),
            "attrs": {},
            "data": [[0.0], [0.2], [0.5], [1.0]],
        },
        "mLayerDepth": {"dims": ("midToto", "hru"), "attrs": {}, "data": [[0.2], [0.3], [0.5]]},
    },
}

EXPECTED_SUMMA_TRIAL_PARAMS = {
    "coords": {},
    "attrs": {
        "Author": "Created by ngiab preprocessor SUMMA workflow script",
        "History": "Created 2026/06/23 11:08:48",
        "Purpose": "Create a trial parameter .nc file for initial SUMMA runs",
    },
    "dims": {"hru": 1},
    "data_vars": {
        "hruId": {
            "dims": ("hru",),
            "attrs": {"units": "-", "long_name": "Index of hydrological response unit (HRU)"},
            "data": [1555522],
        },
        "maxstep": {"dims": ("hru",), "attrs": {}, "data": [86400.0]},
    },
}

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

EXPECTED_GAGE_SUMMA_ATTRIBUTE_FIRST_LAST = {
    "hruId_first": 2861379,
    "hruId_last": 2861416,
    "longitude_first": -111.59267938862689,
    "longitude_last": -111.77737686266926,
    "latitude_first": 42.00456745631874,
    "latitude_last": 41.75600498677581,
    "elevation_first": 2463.9530855860044,
    "elevation_last": 1712.4208968464359,
    "HRUarea_first": 16913249.424001046,
    "HRUarea_last": 6473249.406000818,
    "tan_slope_first": 0.07485539035668641,
    "tan_slope_last": 0.06413170338731738,
    "soilTypeIndex_first": 4,
    "soilTypeIndex_last": 4,
    "vegTypeIndex_first": 11,
    "vegTypeIndex_last": 8,
}

EXPECTED_GAGE_SUMMA_COLD_STATE = {
    "attrs": {
        "Author": "Created by SUMMA workflow scripts",
        "History": "Created 2026/06/23 12:04:47",
        "Purpose": "Create a cold state .nc file for initial SUMMA runs",
    },
    "dims": {"hru": 88, "scalarv": 1, "midToto": 3, "ifcToto": 4, "midSoil": 3},
}

EXPECTED_GAGE_SUMMA_TRIAL_PARAMS = {
    "attrs": {
        "Author": "Created by ngiab preprocessor SUMMA workflow script",
        "History": "Created 2026/06/23 12:04:47",
        "Purpose": "Create a trial parameter .nc file for initial SUMMA runs",
    },
    "dims": {"hru": 88},
}


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


def _assert_dataset_matches_expected(ds: xr.Dataset, expected: dict):
    actual_dict = ds.to_dict(data=True)
    assert _filter_dataset_dict(actual_dict) == _filter_dataset_dict(expected)


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
    hru_ids = [1555522]
    ds = make_summa_attributes(hru_ids, GEOPACKAGE_FIXTURES["cat-1555522"])
    output_path = tmp_path / "attributes.nc"
    ds.to_netcdf(output_path)

    with xr.open_dataset(output_path) as actual_ds:
        _assert_dataset_matches_expected(actual_ds, EXPECTED_SUMMA_ATTRIBUTES)


def test_make_summa_coldState_netcdf_matches_expected(tmp_path):  # pylint: disable=invalid-name
    """Checks coldState.nc."""
    hru_ids = [1555522]
    ds, encoding = make_summa_coldState(hru_ids)
    output_path = tmp_path / "coldState.nc"
    ds.to_netcdf(output_path, encoding=encoding)

    with xr.open_dataset(output_path) as actual_ds:
        _assert_dataset_matches_expected(actual_ds, EXPECTED_SUMMA_COLD_STATE)


def test_make_summa_trialParams_netcdf_matches_expected(tmp_path):  # pylint: disable=invalid-name
    """Checks trialParams.nc."""
    hru_ids = [1555522]
    ds = make_summa_trialParams(hru_ids, int((FIXED_END - FIXED_START).total_seconds() / 3600))
    output_path = tmp_path / "trialParams.nc"
    ds.to_netcdf(output_path)

    with xr.open_dataset(output_path) as actual_ds:
        _assert_dataset_matches_expected(actual_ds, EXPECTED_SUMMA_TRIAL_PARAMS)


def test_summa_config_generation_matches_golden(cat_1555522_forcing_output, tmp_path, monkeypatch):
    """Checks all non-netCDF config files."""
    produced = _generate_summa_config(
        "cat-1555522", cat_1555522_forcing_output["forcings_nc"], tmp_path, monkeypatch
    )
    assert GOLDEN_SUMMA_FILE.exists(), (
        f"missing golden {GOLDEN_SUMMA_FILE}. Generate it with: "
        "UPDATE_GOLDEN=1 uv run pytest tests/test_summa_config_generation.py"
    )

    golden = json.loads(GOLDEN_SUMMA_FILE.read_text())

    missing = sorted(set(golden) - set(produced))
    extra = sorted(set(produced) - set(golden))
    assert not missing and not extra, (
        f"SUMMA config file set changed.\n  missing: {missing}\n  extra: {extra}"
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
            f"{len(changed)} SUMMA config file(s) changed: {changed}\nfirst diff ({first}):\n{diff}"
        )


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


def test_gage_make_summa_attributes_first_last(tmp_path):
    """Checks attributes.nc for a multi-catchment simulation."""
    ds = make_summa_attributes(GAGE_HRU_IDS, GEOPACKAGE_FIXTURES["gage-10109001"])
    output_path = tmp_path / "attributes.nc"
    ds.to_netcdf(output_path)

    e = EXPECTED_GAGE_SUMMA_ATTRIBUTE_FIRST_LAST
    with xr.open_dataset(output_path) as actual:
        assert actual.sizes["hru"] == len(GAGE_HRU_IDS)
        assert actual.sizes["gru"] == len(GAGE_HRU_IDS)
        assert int(actual["hruId"].values[0]) == e["hruId_first"]
        assert int(actual["hruId"].values[-1]) == e["hruId_last"]
        for var in ("longitude", "latitude", "elevation", "HRUarea", "tan_slope"):
            np.testing.assert_allclose(actual[var].values[0], e[f"{var}_first"], rtol=1e-9)
            np.testing.assert_allclose(actual[var].values[-1], e[f"{var}_last"], rtol=1e-9)
        assert int(actual["soilTypeIndex"].values[0]) == e["soilTypeIndex_first"]
        assert int(actual["soilTypeIndex"].values[-1]) == e["soilTypeIndex_last"]
        assert int(actual["vegTypeIndex"].values[0]) == e["vegTypeIndex_first"]
        assert int(actual["vegTypeIndex"].values[-1]) == e["vegTypeIndex_last"]


def test_gage_make_summa_coldState_dims_and_structure(tmp_path):  # pylint: disable=invalid-name
    """Checks coldState.nc for a multi-catchment simulation."""
    ds, encoding = make_summa_coldState(GAGE_HRU_IDS)
    output_path = tmp_path / "coldState.nc"
    ds.to_netcdf(output_path, encoding=encoding)

    expected = EXPECTED_GAGE_SUMMA_COLD_STATE
    with xr.open_dataset(output_path) as actual:
        assert dict(actual.sizes) == expected["dims"]
        assert actual.attrs["Author"] == expected["attrs"]["Author"]
        assert actual.attrs["Purpose"] == expected["attrs"]["Purpose"]
        assert "History" in actual.attrs  # value is a generation timestamp
        assert int(actual["hruId"].values[0]) == GAGE_HRU_IDS[0]
        assert int(actual["hruId"].values[-1]) == GAGE_HRU_IDS[-1]


def test_gage_make_summa_trialParams_dims_and_structure(tmp_path):  # pylint: disable=invalid-name
    """Checks trialParams for a multi-catchment simulation."""
    timesteps = int((FIXED_END - FIXED_START).total_seconds() / 3600)
    ds = make_summa_trialParams(GAGE_HRU_IDS, timesteps)
    output_path = tmp_path / "trialParams.nc"
    ds.to_netcdf(output_path)

    expected = EXPECTED_GAGE_SUMMA_TRIAL_PARAMS
    with xr.open_dataset(output_path) as actual:
        assert dict(actual.sizes) == expected["dims"]
        assert actual.attrs["Author"] == expected["attrs"]["Author"]
        assert actual.attrs["Purpose"] == expected["attrs"]["Purpose"]
        assert "History" in actual.attrs
        assert int(actual["hruId"].values[0]) == GAGE_HRU_IDS[0]
        assert int(actual["hruId"].values[-1]) == GAGE_HRU_IDS[-1]
        np.testing.assert_allclose(actual["maxstep"].values, timesteps * 3600)


def test_gage_summa_config_generation_matches_golden(
    gage_10109001_forcing_output, tmp_path, monkeypatch
):
    """Checks that the multi-catchment non-netCDF files match the reference."""
    produced = _generate_summa_config(
        "gage-10109001", gage_10109001_forcing_output["forcings_nc"], tmp_path, monkeypatch
    )
    assert GOLDEN_GAGE_SUMMA_FILE.exists(), (
        f"missing golden {GOLDEN_GAGE_SUMMA_FILE}. Save the gage golden there."
    )
    golden = json.loads(GOLDEN_GAGE_SUMMA_FILE.read_text())

    missing = sorted(set(golden) - set(produced))
    extra = sorted(set(produced) - set(golden))
    assert not missing and not extra, (
        f"SUMMA config file set changed for gage-10109001.\n  missing: {missing}\n  extra: {extra}"
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
            f"{len(changed)} SUMMA config file(s) changed for gage-10109001: {changed}\n"
            f"first diff ({first}):\n{diff}"
        )


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
