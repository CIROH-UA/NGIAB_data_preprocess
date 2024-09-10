import os
import logging
from typing import List
import geopandas as gpd
import pandas as pd
import pyarrow

from pyarrow import csv as pa_csv, parquet as pa_parquet, compute as pa_compute

from pathlib import Path
from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import (
    create_empty_gpkg,
    subset_table,
    update_geopackage_metadata,
    add_triggers_to_gpkg,
)
from data_processing.graph_utils import get_upstream_ids


logger = logging.getLogger(__name__)


def create_subset_gpkg(ids: List[str], hydrofabric: str, paths: file_paths) -> Path:

    subset_gpkg_name = paths.geopackage_path
    subset_gpkg_name.parent.mkdir(parents=True, exist_ok=True)
    if os.path.exists(subset_gpkg_name):
        os.remove(subset_gpkg_name)

    create_empty_gpkg(subset_gpkg_name)

    subset_tables = [
        "divides",
        "nexus",
        "flowpaths",
        "flowpath_edge_list",
        "flowpath_attributes",
        "hydrolocations",
        # Commented out for v20.1 gpkg
        # "lakes",
    ]

    for table in subset_tables:
        subset_table(table, ids, hydrofabric, str(subset_gpkg_name.absolute()))

    add_triggers_to_gpkg(subset_gpkg_name)

    update_geopackage_metadata(subset_gpkg_name)


def subset_parquet(ids: List[str], paths: file_paths) -> None:
    cat_ids = [x.replace("wb", "cat") for x in ids]
    parquet_path = paths.model_attributes
    output_dir = paths.subset_dir
    logger.debug(str(parquet_path))
    logger.debug("Reading parquet")
    logger.info("Extracting model attributes")
    table = pa_parquet.read_table(parquet_path)
    logger.debug("Filtering parquet")
    filtered_table = table.filter(
        pa_compute.is_in(table.column("divide_id"), value_set=pyarrow.array(cat_ids))
    )
    logger.debug("Writing parquet")
    pa_csv.write_csv(filtered_table, output_dir / "cfe_noahowp_attributes.csv")


def subset(
    cat_ids: List[str],
    hydrofabric: str = file_paths.conus_hydrofabric,
    output_folder_name: str = None,
) -> str:

    upstream_ids = get_upstream_ids(cat_ids)

    if not output_folder_name:
        # if the name isn't provided, use the first upstream id
        upstream_ids = sorted(list(upstream_ids))
        output_folder_name = upstream_ids[0]

    paths = file_paths(output_folder_name)
    remove_existing_output_dir(paths.subset_dir)
    create_subset_gpkg(upstream_ids, hydrofabric, paths)
    subset_parquet(upstream_ids, paths)
    move_files_to_config_dir(paths.subset_dir)
    if len(upstream_ids) > 100000:
        # don't do this slow list comprehension if there are a lot of upstreams
        num_catchments = sum(1 for x in upstream_ids if x.startswith("wb"))
        logger.info(f"Subset complete for {num_catchments} catchments")
    logger.debug(f"Subset complete for {upstream_ids} catchments")
    return str(paths.subset_dir)


def remove_existing_output_dir(subset_output_dir: Path) -> None:
    if subset_output_dir.exists():
        os.system(f"rm -rf {subset_output_dir / 'config'}")
        os.system(f"rm -rf {subset_output_dir / 'forcings'}")
    else:
        subset_output_dir.mkdir(parents=True, exist_ok=True)


def move_files_to_config_dir(subset_output_dir: str) -> None:
    config_dir = subset_output_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)

    files = [x for x in subset_output_dir.iterdir()]
    for file in files:
        if file.suffix in [".csv", ".json", ".geojson"]:
            if "partitions" in file.name:
                continue
            os.system(f"mv {file} {config_dir}")
