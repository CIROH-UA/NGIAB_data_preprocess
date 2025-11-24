import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
import xarray as xr

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONFIG_PATH = Path.home() / ".ngiab" / "ngiab_preprocess"


@pytest.fixture(scope="module")
def test_output_dir():
    """Create a temporary directory for test outputs."""
    temp_path = Path(tempfile.mkdtemp(prefix="ngiab_test_"))
    yield temp_path

    if temp_path.exists():
        shutil.rmtree(temp_path)


def run_cli(input_id, start_date, end_date, output_name, source="aorc"):
    """Run the CLI and return output paths."""
    # Read config to get output root
    if CONFIG_PATH.exists():
        import json

        config = json.loads(CONFIG_PATH.read_text())
        output_root = Path(config.get("output_dir", Path.home() / "ngiab_preprocess_output"))
    else:
        output_root = Path.home() / "ngiab_preprocess_output"

    output_path = output_root / output_name

    # Clean up any existing output directory
    if output_path.exists():
        shutil.rmtree(output_path)

    cmd = [
        "uv",
        "run",
        "cli",
        "-i",
        input_id,
        "-s",
        "-f",
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
        pytest.fail(f"CLI failed: {e.stderr}")
    except subprocess.TimeoutExpired:
        pytest.fail("CLI timed out")

    assert output_path.exists(), f"Output directory not created: {output_path}"
    return {
        "output_dir": output_path,
        "start_date": start_date,
        "end_date": end_date,
        "gpkg_path": output_path / "config" / f"{output_name}_subset.gpkg",
        "raw_nc": output_path / "forcings" / "raw_gridded_data.nc",
        "forcings_nc": output_path / "forcings" / "forcings.nc",
    }


@pytest.fixture(scope="module")
def cat_1555522_output():
    """Single catchment test: cat-1555522, 1 day."""
    return run_cli("cat-1555522", "2020-01-01", "2020-01-02", "test_cat_1555522")


@pytest.fixture(scope="module")
def gage_10109001_output():
    """Multi-catchment gage test: gage-10109001, 9 days."""
    return run_cli("gage-10109001", "2019-10-01", "2019-10-10", "test_gage_10109001")


# =============================================================================
# Test configurations
# =============================================================================

GEOPACKAGE_LAYERS = [
    "divides",
    "divide-attributes",
    "flowpath-attributes",
    "flowpath-attributes-ml",
    "flowpaths",
    "hydrolocations",
    "nexus",
    "pois",
    "lakes",
    "network",
]

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

CAT_1555522_REGRESSION = {
    "dims": {"catchment-id": 1, "time": 25},
    "catchment_ids": ["cat-1555522"],
    "table_counts": {"divides": 1, "flowpaths": 1, "nexus": 1},
    "stats": {
        "TMP_2maboveground": {"min": 270.04, "max": 287.06, "mean": 276.30},
        "PRES_surface": {"min": 96235.0, "max": 97941.0, "mean": 97159.5},
        "DSWRF_surface": {"min": 0.0, "max": 366.91, "mean": 85.40},
        "DLWRF_surface": {"min": 207.95, "max": 248.33, "mean": 222.67},
        "SPFH_2maboveground": {"min": 0.0024, "max": 0.00464, "mean": 0.00313},
    },
    "sample_values": {
        "TMP_2maboveground": [275.938, 274.598, 273.735, 272.985, 272.476],
        "PRES_surface": [97941.0, 97872.3, 97857.2, 97852.3, 97829.1],
    },
    "time_values": [1577836800, 1577840400, 1577844000, 1577847600, 1577851200],
}

GAGE_10109001_REGRESSION = {
    "dims": {"catchment-id": 88, "time": 217},
    "catchment_ids": [
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
    ],  # First 10 for spot check
    "table_counts": {"divides": 88, "flowpaths": 88, "nexus": 38},
    "stats": {
        "TMP_2maboveground": {"min": 266.08, "max": 293.25, "mean": 276.13},
        "PRES_surface": {"min": 72895.4, "max": 85003.4, "mean": 77537.8},
        "DSWRF_surface": {"min": 0.0, "max": 711.17, "mean": 179.39},
        "DLWRF_surface": {"min": 177.51, "max": 322.51, "mean": 222.13},
        "SPFH_2maboveground": {"min": 0.00122, "max": 0.00588, "mean": 0.00333},
        "APCP_surface": {"min": 0.0, "max": 4.696, "mean": 0.0233},
    },
    "sample_values": {
        "TMP_2maboveground": [274.370, 272.429, 270.498, 268.974, 269.294],
        "PRES_surface": [74866.3, 74861.7, 74884.5, 74898.7, 74877.5],
    },
    "time_values": [1569888000, 1569891600, 1569895200, 1569898800, 1569902400],
}


# =============================================================================
# cat-1555522 Tests (Single Catchment)
# =============================================================================


class TestCat1555522Geopackage:
    """Geopackage tests for cat-1555522."""

    def test_geopackage_layers(self, cat_1555522_output):
        gpkg = cat_1555522_output["gpkg_path"]
        assert gpkg.exists()
        actual = set(gpd.list_layers(gpkg)["name"])
        assert not (set(GEOPACKAGE_LAYERS) - actual), f"Missing layers: {set(GEOPACKAGE_LAYERS) - actual}"

    @pytest.mark.parametrize("layer", ["divides", "flowpaths", "nexus"])
    def test_table_row_counts(self, cat_1555522_output, layer):
        gdf = gpd.read_file(cat_1555522_output["gpkg_path"], layer=layer)
        assert len(gdf) == CAT_1555522_REGRESSION["table_counts"][layer]


class TestCat1555522GriddedForcings:
    """Raw gridded forcing tests for cat-1555522."""

    def test_netcdf_structure(self, cat_1555522_output):
        nc = cat_1555522_output["raw_nc"]
        assert nc.exists()
        with xr.open_dataset(nc) as ds:
            assert "time" in ds.dims
            assert any(d in ds.dims for d in ("x", "lon"))
            assert any(d in ds.dims for d in ("y", "lat"))

    def test_netcdf_time_range(self, cat_1555522_output):
        with xr.open_dataset(cat_1555522_output["raw_nc"]) as ds:
            assert ds.time.min().values >= np.datetime64(cat_1555522_output["start_date"])
            assert ds.time.max().values <= np.datetime64(cat_1555522_output["end_date"])


class TestCat1555522ProcessedForcings:
    """Processed forcing tests for cat-1555522."""

    def test_structure(self, cat_1555522_output):
        nc = cat_1555522_output["forcings_nc"]
        assert nc.exists()
        with xr.open_dataset(nc) as ds:
            assert ds.sizes["catchment-id"] == CAT_1555522_REGRESSION["dims"]["catchment-id"]
            assert ds.sizes["time"] == CAT_1555522_REGRESSION["dims"]["time"]
            for var in FORCING_VARS:
                assert var in ds.data_vars or var in ds.coords

    def test_catchment_ids(self, cat_1555522_output):
        gpkg_ids = set(gpd.read_file(cat_1555522_output["gpkg_path"], layer="divides")["divide_id"])
        with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
            nc_ids = set(ds["ids"].values)
        assert gpkg_ids == nc_ids

    def test_value_ranges(self, cat_1555522_output):
        with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
            for var, (lo, hi) in PHYSICAL_RANGES.items():
                if var in ds.data_vars:
                    data = ds[var].values
                    assert np.nanmin(data) >= lo, f"{var} below min"
                    assert np.nanmax(data) <= hi, f"{var} above max"

    def test_regression_stats(self, cat_1555522_output):
        with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
            for var, expected in CAT_1555522_REGRESSION["stats"].items():
                data = ds[var].values
                np.testing.assert_allclose(np.nanmin(data), expected["min"], rtol=0.01)
                np.testing.assert_allclose(np.nanmax(data), expected["max"], rtol=0.01)
                np.testing.assert_allclose(np.nanmean(data), expected["mean"], rtol=0.01)

    def test_regression_sample_values(self, cat_1555522_output):
        with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
            for var, expected in CAT_1555522_REGRESSION["sample_values"].items():
                actual = ds[var].isel({"catchment-id": 0, "time": slice(0, 5)}).values
                np.testing.assert_allclose(actual, expected, rtol=0.001)

    def test_regression_time_values(self, cat_1555522_output):
        with xr.open_dataset(cat_1555522_output["forcings_nc"]) as ds:
            actual = ds["Time"].isel({"catchment-id": 0, "time": slice(0, 5)}).values.tolist()
            assert actual == CAT_1555522_REGRESSION["time_values"]


# =============================================================================
# gage-10109001 Tests (Multi-Catchment)
# =============================================================================


class TestGage10109001Geopackage:
    """Geopackage tests for gage-10109001."""

    def test_geopackage_layers(self, gage_10109001_output):
        gpkg = gage_10109001_output["gpkg_path"]
        assert gpkg.exists()
        actual = set(gpd.list_layers(gpkg)["name"])
        assert not (set(GEOPACKAGE_LAYERS) - actual)

    @pytest.mark.parametrize("layer", ["divides", "flowpaths", "nexus"])
    def test_table_row_counts(self, gage_10109001_output, layer):
        gdf = gpd.read_file(gage_10109001_output["gpkg_path"], layer=layer)
        assert len(gdf) == GAGE_10109001_REGRESSION["table_counts"][layer]


class TestGage10109001GriddedForcings:
    """Raw gridded forcing tests for gage-10109001."""

    def test_netcdf_structure(self, gage_10109001_output):
        nc = gage_10109001_output["raw_nc"]
        assert nc.exists()
        with xr.open_dataset(nc) as ds:
            assert "time" in ds.dims
            assert any(d in ds.dims for d in ("x", "lon"))
            assert any(d in ds.dims for d in ("y", "lat"))

    def test_netcdf_time_range(self, gage_10109001_output):
        with xr.open_dataset(gage_10109001_output["raw_nc"]) as ds:
            assert ds.time.min().values >= np.datetime64(gage_10109001_output["start_date"])
            assert ds.time.max().values <= np.datetime64(gage_10109001_output["end_date"])


class TestGage10109001ProcessedForcings:
    """Processed forcing tests for gage-10109001."""

    def test_structure(self, gage_10109001_output):
        nc = gage_10109001_output["forcings_nc"]
        assert nc.exists()
        with xr.open_dataset(nc) as ds:
            assert ds.sizes["catchment-id"] == GAGE_10109001_REGRESSION["dims"]["catchment-id"]
            assert ds.sizes["time"] == GAGE_10109001_REGRESSION["dims"]["time"]
            for var in FORCING_VARS:
                assert var in ds.data_vars or var in ds.coords

    def test_catchment_ids_subset(self, gage_10109001_output):
        """Check that expected catchment IDs are present (spot check first 10)."""
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            nc_ids = set(ds["ids"].values)
        for cat_id in GAGE_10109001_REGRESSION["catchment_ids"]:
            assert cat_id in nc_ids

    def test_catchment_ids_match_gpkg(self, gage_10109001_output):
        gpkg_ids = set(gpd.read_file(gage_10109001_output["gpkg_path"], layer="divides")["divide_id"])
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            nc_ids = set(ds["ids"].values)
        assert gpkg_ids == nc_ids

    def test_value_ranges(self, gage_10109001_output):
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var, (lo, hi) in PHYSICAL_RANGES.items():
                if var in ds.data_vars:
                    data = ds[var].values
                    assert np.nanmin(data) >= lo, f"{var} below min"
                    assert np.nanmax(data) <= hi, f"{var} above max"

    def test_no_all_nan(self, gage_10109001_output):
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var in ds.data_vars:
                if ds[var].dtype in (np.float32, np.float64):
                    assert not np.all(np.isnan(ds[var].values)), f"{var} is all NaN"

    def test_regression_stats(self, gage_10109001_output):
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var, expected in GAGE_10109001_REGRESSION["stats"].items():
                data = ds[var].values
                np.testing.assert_allclose(np.nanmin(data), expected["min"], rtol=0.01)
                np.testing.assert_allclose(np.nanmax(data), expected["max"], rtol=0.01)
                np.testing.assert_allclose(np.nanmean(data), expected["mean"], rtol=0.01)

    def test_regression_sample_values(self, gage_10109001_output):
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            for var, expected in GAGE_10109001_REGRESSION["sample_values"].items():
                actual = ds[var].isel({"catchment-id": 0, "time": slice(0, 5)}).values
                np.testing.assert_allclose(actual, expected, rtol=0.001)

    def test_regression_time_values(self, gage_10109001_output):
        with xr.open_dataset(gage_10109001_output["forcings_nc"]) as ds:
            actual = ds["Time"].isel({"catchment-id": 0, "time": slice(0, 5)}).values.tolist()
            assert actual == GAGE_10109001_REGRESSION["time_values"]


# =============================================================================
# End-to-End Tests
# =============================================================================


class TestEndToEnd:
    """End-to-end integration tests."""

    @pytest.mark.parametrize("fixture_name", ["cat_1555522_output", "gage_10109001_output"])
    def test_complete_pipeline(self, fixture_name, request):
        output = request.getfixturevalue(fixture_name)
        assert output["gpkg_path"].exists()
        assert output["raw_nc"].exists()
        assert output["forcings_nc"].exists()

    @pytest.mark.parametrize("fixture_name", ["cat_1555522_output", "gage_10109001_output"])
    def test_output_size_reasonable(self, fixture_name, request):
        output = request.getfixturevalue(fixture_name)
        size_mb = sum(f.stat().st_size for f in output["output_dir"].rglob("*") if f.is_file()) / (1024 * 1024)
        assert 0.1 < size_mb < 1000, f"Suspicious output size: {size_mb:.2f} MB"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
