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

def create_subset_gpkg(
    ids: Union[List[str], str], hydrofabric: Path, output_gpkg_path: Path, is_vpu: bool = False, location: str = "conus"
):
    # ids is a list of nexus and wb ids, or a single vpu id
    if not isinstance(ids, list):
        ids = [ids]
    output_gpkg_path.parent.mkdir(parents=True, exist_ok=True)

    if os.path.exists(output_gpkg_path):
        os.remove(output_gpkg_path)
    if location == "conus":
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
    elif location == "hi": # Hawaii hydrofabric has no flowpath-attributes-ml
        subset_tables = [
            "divides",
            "divide-attributes",  # requires divides
            "flowpath-attributes",
            "flowpaths",
            "hydrolocations",
            "nexus",  # depends on flowpaths in some cases e.g. gage delineation
            "pois",  # requires flowpaths
            "lakes",  # requires pois
        ]
    else:
        raise ValueError(f"Invalid location: {location}. Must be 'conus' or 'hi'.")
    create_empty_gpkg(output_gpkg_path, location)
    logger.info(f"Subsetting tables: {subset_tables}")
    if location == "conus":
        hydrofabric = file_paths.conus_hydrofabric
    elif location == "hi":
        hydrofabric = file_paths.hawaii_hydrofabric
    else:
        raise ValueError(f"Invalid location: {location}. Must be 'conus' or 'hi'.")
    for table in subset_tables:
        if is_vpu:
            subset_table_by_vpu(table, ids[0], hydrofabric, output_gpkg_path)
        else:
            subset_table(table, ids, hydrofabric, output_gpkg_path)

    add_triggers_to_gpkg(output_gpkg_path, location=location)
    update_geopackage_metadata(output_gpkg_path, hydrofabric)


def subset_vpu(
    vpu_id: str, output_gpkg_path: Path, hydrofabric: Path = file_paths.conus_hydrofabric
):
    if output_gpkg_path.exists():
        os.remove(output_gpkg_path)

    create_subset_gpkg(vpu_id, hydrofabric, output_gpkg_path=output_gpkg_path, is_vpu=True)
    logger.info(f"Subset complete for VPU {vpu_id}")
    return output_gpkg_path.parent


def subset(
    cat_ids: List[str],
    output_gpkg_path: Path = Path(),
    include_outlet: bool = True,
    location: str = "conus"
):
    if location == "conus":
        hydrofabric = file_paths.conus_hydrofabric
    elif location == "hi":
        hydrofabric = file_paths.hawaii_hydrofabric
    else:
        raise ValueError(f"Invalid location: {location}. Must be 'conus' or 'hi'.")
    
    upstream_ids = list(get_upstream_ids(cat_ids, include_outlet, location=location))

    if not output_gpkg_path:
        # if the name isn't provided, use the first upstream id
        upstream_ids = sorted(upstream_ids)
        output_folder_name = upstream_ids[0]
        paths = file_paths(output_folder_name)
        output_gpkg_path = paths.geopackage_path

    create_subset_gpkg(upstream_ids, hydrofabric, output_gpkg_path, location=location)
    logger.info(f"Subset complete for {len(upstream_ids)} features (catchments + nexuses)")
    logger.debug(f"Subset complete for {upstream_ids} catchments")
