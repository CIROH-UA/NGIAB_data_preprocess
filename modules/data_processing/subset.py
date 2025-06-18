import logging
import os
from pathlib import Path
from typing import List, Union

from data_processing.file_paths import file_paths
from data_processing.gpkg_utils import (
    add_triggers_to_gpkg,
    create_empty_gpkg,
    subset_table,
    subset_table_by_vpu,
    update_geopackage_metadata,
)
from data_processing.graph_utils import get_upstream_ids

logger = logging.getLogger(__name__)

subset_tables = [
    "divides",
    "divide-attributes",  # requires divides
    "flowpath-attributes",
    "flowpath-attributes-ml",
    "flowpaths",
    "hydrolocations",
    "nexus",  # depends on flowpaths in some cases e.g. gage delineation
    "pois",  # requires flowpaths
    "lakes",  # requires pois
    "network",
]


def create_subset_gpkg(
    ids: Union[List[str], str], hydrofabric: Path, output_gpkg_path: Path, is_vpu: bool = False
):
    # ids is a list of nexus and wb ids, or a single vpu id
    if not isinstance(ids, list):
        ids = [ids]
    output_gpkg_path.parent.mkdir(parents=True, exist_ok=True)

    if os.path.exists(output_gpkg_path):
        os.remove(output_gpkg_path)

    create_empty_gpkg(output_gpkg_path)
    logger.info(f"Subsetting tables: {subset_tables}")
    for table in subset_tables:
        if is_vpu:
            subset_table_by_vpu(table, ids[0], hydrofabric, output_gpkg_path)
        else:
            subset_table(table, ids, hydrofabric, output_gpkg_path)

    add_triggers_to_gpkg(output_gpkg_path)
    update_geopackage_metadata(output_gpkg_path)


def subset_vpu(
    vpu_id: str, output_gpkg_path: Path, hydrofabric: Path = file_paths.conus_hydrofabric
):
    if output_gpkg_path.exists():
        os.remove(output_gpkg_path)

    create_subset_gpkg(vpu_id, hydrofabric, output_gpkg_path=output_gpkg_path, is_vpu=True)
    logger.info(f"Subset complete for VPU {vpu_id}")
    return output_gpkg_path.parent


def subset(
    cat_ids: str | List[str],
    hydrofabric: Path = file_paths.conus_hydrofabric,
    output_gpkg_path: Path = Path(),
    include_outlet: bool = True,
):
    upstream_ids = list(get_upstream_ids(cat_ids, include_outlet))

    if not output_gpkg_path:
        # if the name isn't provided, use the first upstream id
        upstream_ids = sorted(upstream_ids)
        output_folder_name = upstream_ids[0]
        paths = file_paths(output_folder_name)
        output_gpkg_path = paths.geopackage_path

    create_subset_gpkg(upstream_ids, hydrofabric, output_gpkg_path)
    logger.info(f"Subset complete for {len(upstream_ids)} features (catchments + nexuses)")
    logger.debug(f"Subset complete for {upstream_ids} catchments")
