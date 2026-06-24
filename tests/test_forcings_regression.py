"""Forcings regression tests.

Fixture geopackages expected:
    tests/golden/geopackage/gage-10109001_subset.gpkg
"""

import logging
import shutil
import subprocess
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from data_processing.file_paths import FilePaths

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_PATH = FilePaths.config_file
HYDROFABRIC_PATH = FilePaths.conus_hydrofabric
GOLDEN_GPKG_DIR = Path(__file__).parent / "golden" / "geopackage"

pytestmark = pytest.mark.integration

# input id -> committed subset geopackage used as the fixture
GEOPACKAGE_FIXTURES = {
    "gage-10109001": GOLDEN_GPKG_DIR / "gage-10109001_subset.gpkg",
}


def run_forcings(input_id, start_date, end_date, output_name, source="aorc"):
    """Place the pre-saved geopackage, then run the CLI forcings step ONLY."""
    fixture_gpkg = GEOPACKAGE_FIXTURES.get(input_id)
    if fixture_gpkg is None or not fixture_gpkg.exists():
        pytest.skip(
            f"missing geopackage fixture for {input_id} (expected {fixture_gpkg}). "
            "Save the subset geopackage there or update GEOPACKAGE_FIXTURES."
        )

    # Use the working dir the preprocessor is already configured with (same as the
    # original test); only the geopackage placement + '-f only' invocation differ.
    # pytest.skip(f"no preprocessor working dir configured at {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        output_root = Path(f.readline().strip()).expanduser()

    output_path = output_root / output_name
    if output_path.exists():
        shutil.rmtree(output_path)

    # Pre-place the geopackage where the forcings step looks for it. Because
    # config_dir now exists, main() will NOT auto-enable subsetting.
    config_dir = output_path / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(fixture_gpkg, config_dir / f"{output_name}_subset.gpkg")

    cmd = [
        "uv",
        "run",
        "cli",
        "-i",
        input_id,
        "-f",  # forcings only -- no '-s'
        "--start_date",
        start_date,
        "--end_date",
        end_date,
        "--source",
        source,
        "-o",
        output_name,
    ]
    try:
        subprocess.run(cmd, capture_output=True, text=True, check=True, timeout=600)
    except subprocess.CalledProcessError as e:
        pytest.fail(f"forcings CLI failed: {e.stderr}")
    except subprocess.TimeoutExpired:
        pytest.fail("forcings CLI timed out")

    forcings_nc = output_path / "forcings" / "forcings.nc"
    assert forcings_nc.exists(), f"forcings not created: {forcings_nc}"
    return {
        "output_dir": output_path,
        "start_date": start_date,
        "end_date": end_date,
        "gpkg_path": config_dir / f"{output_name}_subset.gpkg",
        "raw_nc": output_path / "forcings" / "raw_gridded_data.nc",
        "forcings_nc": forcings_nc,
    }


# @pytest.fixture(scope="module", name="cat_1555522_output")
# def cat_1555522_output_fixture():
#     """Single catchment: cat-1555522, 1 day."""
#     return run_forcings("cat-1555522", "2020-01-01", "2020-01-02", "cat-1555522")


@pytest.fixture(scope="module", name="gage_10109001_output")
def gage_10109001_output_fixture():
    """Multi-catchment gage: gage-10109001, 9 days."""
    # input ID is the cat-id because giving it a gage ID forces a lookup through the big hydrofabric
    return run_forcings("cat-2861416", "2019-10-01", "2019-10-10", "gage-10109001")


@pytest.fixture(scope="session", autouse=True)
def _isolated_working_dir(tmp_path_factory):
    """Point the preprocessor at an isolated working dir for this test session.

    Nothing creates ~/.ngiab until the subset/hydrofabric step runs, and we skip
    subsetting here -- so on a clean runner we must create the parent ourselves,
    or set_working_dir() raises FileNotFoundError. We also restore any existing
    config so we don't clobber a dev's real working dir.
    """
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    saved = CONFIG_PATH.read_text(encoding="utf-8") if CONFIG_PATH.exists() else None
    FilePaths.set_working_dir(tmp_path_factory.mktemp("ngiab_work"))
    try:
        yield
    finally:
        if saved is None:
            CONFIG_PATH.unlink(missing_ok=True)
        else:
            CONFIG_PATH.write_text(saved, encoding="utf-8")


@pytest.fixture(scope="session", autouse=True)
def _setup_hydrofabric(tmp_path_factory):
    """Sets up a small hydrofabric in the usual location of the CONUS HF."""
    if HYDROFABRIC_PATH.exists():  # don't overwrite existing conus hf if testing locally
        return
    HYDROFABRIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPACKAGE_FIXTURES["gage-10109001"], HYDROFABRIC_PATH)


# =============================================================================
# Test configurations
# =============================================================================

FORCING_VARS = [
    "SPFH_2maboveground",
    "DSWRF_surface",
    "VGRD_10maboveground",
    "DLWRF_surface",
    "APCP_surface",
    "UGRD_10maboveground",
    "PRES_surface",
    "TMP_2maboveground",
    "precip_rate",
    "ids",
    "Time",
]

PHYSICAL_RANGES = {
    "TMP_2maboveground": (200, 330),
    "PRES_surface": (50000, 110000),
    "SPFH_2maboveground": (0, 0.05),
    "DSWRF_surface": (0, 1400),
    "DLWRF_surface": (0, 600),
    "APCP_surface": (0, 500),
    "precip_rate": (0, 0.2),
}

# CAT_1555522_REGRESSION = {
#     "dims": {"catchment-id": 1, "time": 25},
#     "catchment_ids": ["cat-1555522"],
#     "stats": {
#         "TMP_2maboveground": {"min": 270.04, "max": 287.06, "mean": 276.30},
#         "PRES_surface": {"min": 96235.0, "max": 97941.0, "mean": 97159.5},
#         "DSWRF_surface": {"min": 0.0, "max": 366.91, "mean": 85.40},
#         "DLWRF_surface": {"min": 207.95, "max": 248.33, "mean": 222.67},
#         "SPFH_2maboveground": {"min": 0.0024, "max": 0.00464, "mean": 0.00313},
#     },
#     "sample_values": {
#         "TMP_2maboveground": [275.938, 274.598, 273.735, 272.985, 272.476],
#         "PRES_surface": [97941.0, 97872.3, 97857.2, 97852.3, 97829.1],
#     },
#     "time_values": [1577836800, 1577840400, 1577844000, 1577847600, 1577851200],
# }

# GAGE_10109001_REGRESSION = {
#     "dims": {"catchment-id": 88, "time": 217},
#     "catchment_ids": [
#         "cat-2861379",
#         "cat-2861380",
#         "cat-2861387",
#         "cat-2861414",
#         "cat-2861421",
#         "cat-2861429",
#         "cat-2861431",
#         "cat-2861436",
#         "cat-2861438",
#         "cat-2861442",
#     ],  # First 10 for spot check
#     "stats": {
#         "TMP_2maboveground": {"min": 266.08, "max": 293.25, "mean": 276.13},
#         "PRES_surface": {"min": 72895.4, "max": 85003.4, "mean": 77537.8},
#         "DSWRF_surface": {"min": 0.0, "max": 711.17, "mean": 179.39},
#         "DLWRF_surface": {"min": 177.51, "max": 322.51, "mean": 222.13},
#         "SPFH_2maboveground": {"min": 0.00122, "max": 0.00588, "mean": 0.00333},
#         "APCP_surface": {"min": 0.0, "max": 4.696, "mean": 0.0233},
#     },
#     "sample_values": {
#         "TMP_2maboveground": [274.370, 272.429, 270.498, 268.974, 269.294],
#         "PRES_surface": [74866.3, 74861.7, 74884.5, 74898.7, 74877.5],
#     },
#     "time_values": [1569888000, 1569891600, 1569895200, 1569898800, 1569902400],
# }


# =============================================================================
# cat-1555522 (single catchment)
# =============================================================================


# class TestCat1555522GriddedForcings:
#     """Single catchment raw netCDF tests"""

#     def test_netcdf_structure(self, cat_1555522_output):
#         """Checks structure of raw netCDF"""
#         nc = cat_1555522_output["raw_nc"]
#         assert nc.exists()
#         with xr.open_dataset(nc) as ds:
#             assert "time" in ds.dims
#             assert any(d in ds.dims for d in ("x", "lon"))
#             assert any(d in ds.dims for d in ("y", "lat"))

#     def test_netcdf_time_range(self, cat_1555522_output):
#         """Checks time range of raw netCDF"""
#         with xr.open_dataset(cat_1555522_output["raw_nc"]) as ds:
#             assert ds.time.min().values >= np.datetime64(cat_1555522_output["start_date"])
#             assert ds.time.max().values <= np.datetime64(cat_1555522_output["end_date"])


# class TestCat1555522ProcessedForcings:
#     """Single catchment processed forcings tests"""

#     def test_structure(self, cat_1555522_output):
#         """Checks structure of processed netCDF"""
#         nc = cat_1555522_output["forcings_nc"]
#         assert nc.exists()
#         with xr.open_dataset(nc) as ds:
#             assert ds.sizes["catchment-id"] == CAT_1555522_REGRESSION["dims"]["catchment-id"]
#             assert ds.sizes["time"] == CAT_1555522_REGRESSION["dims"]["time"]
#             for var in FORCING_VARS:
#                 assert var in ds.data_vars or var in ds.coords

#     def test_catchment_ids(self, cat_1555522_output):
#         """Checks catchment IDs of processed netCDF"""
#         gpkg_ids = set(gpd.read_file(cat_1555522_output["gpkg_path"], layer="divides")["divide_id"])
#         with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
#             nc_ids = set(ds["ids"].values)
#         assert gpkg_ids == nc_ids

#     def test_value_ranges(self, cat_1555522_output):
#         """Checks for valid values in processed netCDF"""
#         with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
#             for var, (lo, hi) in PHYSICAL_RANGES.items():
#                 if var in ds.data_vars:
#                     data = ds[var].values
#                     assert np.nanmin(data) >= lo, f"{var} below min"
#                     assert np.nanmax(data) <= hi, f"{var} above max"

#     def test_regression_stats(self, cat_1555522_output):
#         """Checks min, max, and mean of values in processed netCDF"""
#         with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
#             for var, expected in CAT_1555522_REGRESSION["stats"].items():
#                 data = ds[var].values
#                 np.testing.assert_allclose(np.nanmin(data), expected["min"], rtol=0.01)
#                 np.testing.assert_allclose(np.nanmax(data), expected["max"], rtol=0.01)
#                 np.testing.assert_allclose(np.nanmean(data), expected["mean"], rtol=0.01)

#     def test_regression_sample_values(self, cat_1555522_output):
#         """Checks specific values in processed netCDF"""
#         with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
#             for var, expected in CAT_1555522_REGRESSION["sample_values"].items():
#                 actual = ds[var].isel({"catchment-id": 0, "time": slice(0, 5)}).values
#                 np.testing.assert_allclose(actual, expected, rtol=0.001)

#     def test_regression_time_values(self, cat_1555522_output):
#         """Checks times in processed netCDF"""
#         with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
#             actual = ds["Time"].isel({"catchment-id": 0, "time": slice(0, 5)}).values.tolist()
#             assert actual == CAT_1555522_REGRESSION["time_values"]


# =============================================================================
# gage-10109001 (multi-catchment)
# =============================================================================


class TestGage10109001GriddedForcings:
    """Multi catchment raw netCDF tests"""

    def test_netcdf_structure(self, gage_10109001_output):
        """Checks structure of raw netCDF"""
        nc = gage_10109001_output["raw_nc"]
        assert nc.exists()
        with xr.open_dataset(nc) as ds:
            assert "time" in ds.dims
            assert any(d in ds.dims for d in ("x", "lon"))
            assert any(d in ds.dims for d in ("y", "lat"))

    def test_netcdf_time_range(self, gage_10109001_output):
        """Checks time range of raw netCDF"""
        with xr.open_dataset(gage_10109001_output["raw_nc"]) as ds:
            assert ds.time.min().values >= np.datetime64(gage_10109001_output["start_date"])
            assert ds.time.max().values <= np.datetime64(gage_10109001_output["end_date"])


class TestGage10109001ProcessedForcings:
    """Multi catchment processed netCDF tests"""

    def test_structure(self, gage_10109001_output):
        """Checks structure of processed netCDF"""
        nc = gage_10109001_output["forcings_nc"]
        assert nc.exists()
        with xr.open_dataset(nc) as ds:
            assert ds.sizes["catchment-id"] == GAGE_10109001_REGRESSION["dims"]["catchment-id"]
            assert ds.sizes["time"] == GAGE_10109001_REGRESSION["dims"]["time"]
            for var in FORCING_VARS:
                assert var in ds.data_vars or var in ds.coords

    def test_catchment_ids_subset(self, gage_10109001_output):
        """Checks catchment IDs of processed netCDF"""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            nc_ids = set(ds["ids"].values)
        for cat_id in GAGE_10109001_REGRESSION["catchment_ids"]:
            assert cat_id in nc_ids

    def test_catchment_ids_match_gpkg(self, gage_10109001_output):
        """Checks that catchment IDs are the same as the gpkg"""
        gpkg_ids = set(
            gpd.read_file(gage_10109001_output["gpkg_path"], layer="divides")["divide_id"]
        )
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            nc_ids = set(ds["ids"].values)
        assert gpkg_ids == nc_ids

    def test_value_ranges(self, gage_10109001_output):
        """Checks for appropriate values in processed netCDF"""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var, (lo, hi) in PHYSICAL_RANGES.items():
                if var in ds.data_vars:
                    data = ds[var].values
                    assert np.nanmin(data) >= lo, f"{var} below min"
                    assert np.nanmax(data) <= hi, f"{var} above max"

    def test_no_all_nan(self, gage_10109001_output):
        """Checks that not all values are NaN in processed netCDF"""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var in ds.data_vars:
                if ds[var].dtype in (np.float32, np.float64):
                    assert not np.all(np.isnan(ds[var].values)), f"{var} is all NaN"

    def test_regression_stats(self, gage_10109001_output):
        """Checks min, max, and mean of values in processed netCDF"""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var, expected in GAGE_10109001_REGRESSION["stats"].items():
                data = ds[var].values
                np.testing.assert_allclose(np.nanmin(data), expected["min"], rtol=0.01)
                np.testing.assert_allclose(np.nanmax(data), expected["max"], rtol=0.01)
                np.testing.assert_allclose(np.nanmean(data), expected["mean"], rtol=0.01)

    def test_regression_sample_values(self, gage_10109001_output):
        """Checks specific values in processed netCDF"""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var, expected in GAGE_10109001_REGRESSION["sample_values"].items():
                actual = ds[var].isel({"catchment-id": 0, "time": slice(0, 5)}).values
                np.testing.assert_allclose(actual, expected, rtol=0.001)

    def test_regression_time_values(self, gage_10109001_output):
        """Checks times in processed netCDF"""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            actual = ds["Time"].isel({"catchment-id": 0, "time": slice(0, 5)}).values.tolist()
            assert actual == GAGE_10109001_REGRESSION["time_values"]


# =============================================================================
# End-to-end (forcings only)
# =============================================================================


class TestForcingsPipeline:
    """Tests to check forcing output files"""

    @pytest.mark.parametrize("fixture_name", ["gage_10109001_output"])
    def test_outputs_exist(self, fixture_name, request):
        """Checks that output netCDFs exist"""
        output = request.getfixturevalue(fixture_name)
        assert output["raw_nc"].exists()
        assert output["forcings_nc"].exists()

    @pytest.mark.parametrize("fixture_name", ["gage_10109001_output"])
    def test_output_size_reasonable(self, fixture_name, request):
        """Suspicious output size mgiht flag that something went wrong with the forcing generation
        process"""
        output = request.getfixturevalue(fixture_name)
        size_mb = sum(f.stat().st_size for f in output["output_dir"].rglob("*") if f.is_file()) / (
            1024 * 1024
        )
        assert 0.1 < size_mb < 1000, f"Suspicious output size: {size_mb:.2f} MB"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
