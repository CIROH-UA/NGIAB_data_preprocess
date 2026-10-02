import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

# Import the functions to test
from data_processing.forcings import interpolate_nan_values

# Configure logging
logger = logging.getLogger(__name__)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


@pytest.fixture(scope="session")
def test_datasets():
    """Create synthetic test datasets."""
    # Create data that passes the `validate_dataset_format` checks
    times_dt64 = pd.date_range("2023-01-01", periods=5, freq="h").values
    x_coords = np.arange(0.5, 3.5, 1.0)
    y_coords = np.arange(10.5, 13.5, 1.0)

    # Set seed for reproducibility
    np.random.seed(42)

    # Dataset with NaNs
    temp_data_nan = np.random.rand(len(times_dt64), len(y_coords), len(x_coords)) * 30
    temp_data_nan[1, 1, 1] = np.nan  # Inject NaN 1
    temp_data_nan[3, 0, 0] = np.nan  # Inject NaN 2
    precip_data_nan = np.random.rand(len(times_dt64), len(y_coords), len(x_coords)) * 5
    precip_data_nan[2, 1, 0] = np.nan  # Inject NaN 3

    ds_with_nans = xr.Dataset(
        {
            "temperature": (("time", "y", "x"), temp_data_nan),
            "precipitation": (("time", "y", "x"), precip_data_nan),
            "non_numeric_var": (
                ("time",),
                [f"event_{i}" for i in range(len(times_dt64))],
            ),
        },
        coords={"time": times_dt64, "y": y_coords, "x": x_coords},
        attrs={"name": "test_dataset_with_nans", "crs": "EPSG:4326"},
    )

    # Dataset without NaNs (numeric vars only)
    ds_no_nans = ds_with_nans.copy(deep=True)
    ds_no_nans["temperature"] = ds_no_nans["temperature"].fillna(0.0)
    ds_no_nans["precipitation"] = ds_no_nans["precipitation"].fillna(0.0)
    ds_no_nans.attrs["name"] = "test_dataset_no_nans"

    # Count NaNs for verification
    num_nans_temp = ds_with_nans["temperature"].isnull().sum().item()
    num_nans_precip = ds_with_nans["precipitation"].isnull().sum().item()

    logger.info(f"Synthetic ds_with_nans 'temperature' initially has {num_nans_temp} NaNs")
    logger.info(f"Synthetic ds_with_nans 'precipitation' initially has {num_nans_precip} NaNs")

    return {
        "ds_with_nans": ds_with_nans,
        "ds_no_nans": ds_no_nans,
        "num_nans_temp": num_nans_temp,
        "num_nans_precip": num_nans_precip,
    }


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory() as temp_dir_name:
        temp_dir_path = Path(temp_dir_name)
        logger.info(f"Created temporary directory for test outputs: {temp_dir_path}")
        yield temp_dir_path
        # Cleanup is automatic when context manager exits


class TestInterpolation:
    """Tests for interpolate_nan_values function."""

    def test_interpolate_nan_values(self, test_datasets):
        """Test that NaNs are properly interpolated in numeric variables."""
        logger.info("Testing interpolate_nan_values")

        # Use a fresh copy for the test
        test_ds = test_datasets["ds_with_nans"].copy(deep=True)
        interpolate_nan_values(test_ds)

        # Check numeric variables were interpolated
        temp_nans_after = test_ds["temperature"].isnull().sum().item()
        precip_nans_after = test_ds["precipitation"].isnull().sum().item()

        assert temp_nans_after == 0, "Temperature NaNs remain after interpolation"
        assert precip_nans_after == 0, "Precipitation NaNs remain after interpolation"

        # Check non-numeric variables were not modified
        assert test_ds["non_numeric_var"].equals(
            test_datasets["ds_with_nans"]["non_numeric_var"]
        ), "Non-numeric variable was incorrectly modified"


def test_precipitation_is_interpolated_across_short_gaps_and_zero_across_long_ones(caplog):
    times = pd.date_range("2024-06-17 22:00", periods=34, freq="h")
    rain = np.zeros((2, len(times)))
    rain[:, 1] = 12.5 / 3600  # raining in the last hour before the gap
    rain[:, 2:26] = np.nan  # a missing day
    rain[:, 27] = 4.0 / 3600
    rain[:, 28:31] = np.nan  # three missing hours inside a storm
    rain[:, 31] = 1.0 / 3600
    temperature = np.tile(np.linspace(280.0, 313.0, len(times)), (2, 1))
    temperature[:, 2:26] = np.nan
    temperature[:, 28:31] = np.nan
    ds = xr.Dataset(
        {
            "precip_rate": (("catchment", "time"), rain.copy()),
            "APCP_surface": (("catchment", "time"), rain * 3600),
            "TMP_2maboveground": (("catchment", "time"), temperature),
        },
        coords={"catchment": ["cat-1", "cat-2"], "time": times},
    )
    ds["precip_rate"].attrs["units"] = "mm s^-1"
    with caplog.at_level(logging.WARNING):
        interpolate_nan_values(ds)
    assert not ds.to_array().isnull().any()
    # No rain is added across the missing day; a linear fill would put some 150 mm there.
    assert float(ds["precip_rate"][:, 2:26].sum()) == 0.0
    assert float(ds["APCP_surface"][:, 2:26].sum()) == 0.0
    # The three missing hours are interpolated between 4 and 1 mm/h.
    np.testing.assert_allclose(ds["APCP_surface"][:, 28:31], [[3.25, 2.5, 1.75]] * 2)
    np.testing.assert_allclose(ds["precip_rate"][:, 28:31] * 3600, [[3.25, 2.5, 1.75]] * 2)
    assert ds["precip_rate"].attrs["units"] == "mm s^-1"
    # Temperature is interpolated across both gaps.
    np.testing.assert_allclose(ds["TMP_2maboveground"][0], np.linspace(280.0, 313.0, len(times)))
    assert "precip_rate is missing at 27 of 34 time steps" in caplog.text
    assert "first at 2024-06-18T00:00:00, last at 2024-06-19T04:00:00" in caplog.text
    # Only the missing day is set to zero, and it is named.
    zeroed = [
        r.getMessage() for r in caplog.records if "precip_rate is set to zero" in r.getMessage()
    ]
    assert zeroed == [
        "precip_rate is set to zero from 2024-06-18T00:00:00 to 2024-06-18T23:00:00, 24 time steps"
    ]
