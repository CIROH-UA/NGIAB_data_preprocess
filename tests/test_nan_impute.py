import logging
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr

# Import the functions to test
from data_processing.dataset_utils import interpolate_nan_values, save_to_cache

# Configure logging
logger = logging.getLogger(__name__)
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


# @pytest.fixture(scope="session")
# def dask_client():
#     """Setup a Dask LocalCluster for testing."""
#     logger.info("Setting up Dask LocalCluster for testing...")
#     cluster = None
#     client = None

#     try:
#         cluster = LocalCluster(processes=False, n_workers=2, threads_per_worker=1)
#         client = DaskClient(cluster)
#         logger.info(f"Dask client started: {client.dashboard_link}")
#         yield client
#     except Exception as e:
#         logger.error(
#             f"Failed to start Dask client: {e}. Some Dask-dependent tests might fail or run serially."
#         )
#         yield None
#     finally:
#         if client:
#             client.close()
#         if cluster:
#             cluster.close()
#         logger.info("Dask client and cluster for test shut down.")


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


class TestSaveToCache:
    """Tests for save_to_cache function with different scenarios."""

    def test_with_imputation_and_nans(self, test_datasets, temp_dir):
        """Test save_to_cache with imputation ON and NaNs present."""
        logger.info("Testing save_to_cache with imputation ON and NaNs present")

        cache_path = temp_dir / "cache_imputed.nc"

        # Use a fresh copy
        test_ds = test_datasets["ds_with_nans"].copy(deep=True)
        reopened = save_to_cache(test_ds, cache_path)

        # Verify files were created
        assert cache_path.exists(), "Main cache file not created"

        # Verify content of main cache (should have no NaNs)
        with xr.open_dataset(cache_path, engine="h5netcdf") as ds_final:
            assert ds_final["temperature"].isnull().sum().item() == 0, "NaNs found in imputed cache"
            assert ds_final["temperature"].dtype == np.float32, "Final imputed cache not float32"

        reopened.close()  # Close the handle returned by save_to_cache

    def test_with_imputation_no_nans(self, test_datasets, temp_dir):
        """Test save_to_cache with imputation ON and NO NaNs present."""
        logger.info("Testing save_to_cache with imputation ON and NO NaNs")

        cache_path = temp_dir / "cache_no_nans.nc"
        raw_cache_path = temp_dir / "cache_no_nans_raw.nc"

        # Use a fresh copy
        test_ds = test_datasets["ds_no_nans"].copy(deep=True)
        reopened = save_to_cache(test_ds, cache_path)

        # Verify main cache exists but raw cache doesn't
        assert cache_path.exists(), "Main cache file not created"
        assert not raw_cache_path.exists(), "Raw cache file was created but shouldn't exist"

        # Verify content
        with xr.open_dataset(cache_path, engine="h5netcdf") as ds_final:
            assert ds_final["temperature"].isnull().sum().item() == 0, "NaNs found in no-NaN cache"
            assert ds_final["temperature"].dtype == np.float32, "Final no-NaN cache not float32"

        reopened.close()

    def test_without_imputation_with_nans(self, test_datasets, temp_dir):
        """Test save_to_cache with imputation OFF and NaNs present."""
        logger.info("Testing save_to_cache with imputation OFF and NaNs present")

        cache_path = temp_dir / "cache_imputation_off.nc"

        # Use a fresh copy
        test_ds = test_datasets["ds_with_nans"].copy(deep=True)
        reopened = save_to_cache(test_ds, cache_path, interpolate_nans=False)

        # Verify main cache exists but raw cache doesn't
        assert cache_path.exists(), "Main cache file not created"

        # Verify content (should have original NaNs)
        with xr.open_dataset(cache_path, engine="h5netcdf") as ds_final:
            assert (
                ds_final["temperature"].isnull().sum().item() == test_datasets["num_nans_temp"]
            ), f"Expected {test_datasets['num_nans_temp']} NaNs in imputation-off cache"
            assert ds_final["temperature"].dtype == np.float32, (
                "Final imputation-off cache not float32"
            )

        reopened.close()
