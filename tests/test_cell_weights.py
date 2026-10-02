"""get_cell_weights reads a polygon's cells from a raster whose extent is its cells' edges."""

import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from data_processing.forcings import get_cell_weights
from shapely.geometry import box


def test_a_polygon_inside_one_cell_is_covered_by_that_cell_alone():
    # Four unit cells a side: centres 0.5 .. 3.5, edges 0 .. 4, rows north first.
    x = np.arange(0.5, 4.0, 1.0)
    y = x[::-1]
    raster = xr.Dataset(
        {"v": (("y", "x"), np.zeros((4, 4), dtype=np.float32))}, coords={"x": x, "y": y}
    )
    gdf = gpd.GeoDataFrame(
        {"divide_id": ["cat-1"]}, geometry=[box(1.1, 1.1, 1.9, 1.9)], crs="EPSG:5070"
    )
    weights = get_cell_weights(raster, gdf, gdf.crs.to_wkt())
    # The cell spanning x 1..2, y 1..2: third row from the north, second column.
    assert list(weights.loc["cat-1", "cell_id"]) == [9]
    assert list(weights.loc["cat-1", "coverage"]) == pytest.approx([0.64])
