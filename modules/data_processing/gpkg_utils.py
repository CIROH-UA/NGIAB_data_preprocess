import logging
import sqlite3
from typing import List, Tuple
from data_processing.file_paths import file_paths
from shapely.wkb import loads
from shapely.geometry import Point, Polygon
from typing import Union
import struct
import geopandas as gpd
from pathlib import Path

logger = logging.getLogger(__name__)


def verify_indices(gpkg: str = file_paths.conus_hydrofabric()) -> None:
    """
    Verify that the indices in the specified geopackage are correct.
    If they are not, create the correct indices.
    """
    new_indicies = [
        'CREATE INDEX "diid" ON "divides" ( "id" ASC );',
        'CREATE INDEX "flaid" ON "flowpath_attributes" ( "id" ASC );',
        'CREATE INDEX "flid" ON "flowpaths" ( "id" ASC );',
        'CREATE INDEX "hyid" ON "hydrolocations" ( "id" ASC );',
        'CREATE INDEX "gageid" ON "hydrolocations" ( "hl_uri" ASC );',
        #'CREATE INDEX "laid" ON "lakes" ( "id" ASC );',
        'CREATE INDEX "neid" ON "nexus" ( "id" ASC );',
        'CREATE INDEX "nid" ON "network" ( "id" ASC );',
    ]
    # check if the gpkg has the correct indices
    con = sqlite3.connect(gpkg)
    indices = con.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
    indices = [x[0] for x in indices]
    for index in new_indicies:
        if index.split('"')[1] not in indices:
            logger.info(f"Creating index {index}")
            con.execute(index)
            con.commit()
    con.close()


# whenever this is imported, check if the indices are correct
if file_paths.conus_hydrofabric().is_file():
    verify_indices()


def blob_to_geometry(blob: bytes) -> Union[Point, Polygon]:
    """
    Convert a blob to a geometry.
    from http://www.geopackage.org/spec/#gpb_format
    byte 0-2 don't need
    byte 3 bit 0 (bit 24)= 0 for little endian, 1 for big endian (used for srs id and envelope type)
    byte 3 bit 1-3 (bit 25-27)= envelope type (needed to calculate envelope size)
    byte 3 bit 4 (bit 28)= empty geometry flag
    """
    envelope_type = (blob[3] & 14) >> 1
    empty = (blob[3] & 16) >> 4
    if empty:
        return None
    envelope_sizes = [0, 32, 48, 48, 64]
    envelope_size = envelope_sizes[envelope_type]
    header_byte_length = 8 + envelope_size
    # everything after the header is the geometry
    geom = blob[header_byte_length:]
    # convert to hex
    geometry = loads(geom)
    return geometry


def blob_to_centroid(blob: bytes) -> Point:
    """
    Convert a blob to a geometry.
    from http://www.geopackage.org/spec/#gpb_format
    byte 0-2 don't need
    byte 3 bit 0 (bit 24)= 0 for little endian, 1 for big endian (used for srs id and envelope type)
    byte 3 bit 1-3 (bit 25-27)= envelope type (needed to calculate envelope size)
    byte 3 bit 4 (bit 28)= empty geometry flag
    """
    envelope_type = (blob[3] & 14) >> 1
    empty = (blob[3] & 16) >> 4
    if empty:
        return None
    envelope_sizes = [0, 32, 48, 48, 64]
    envelope_size = envelope_sizes[envelope_type]
    header_byte_length = 8 + envelope_size
    # everything after the header is the geometry
    envelope = blob[8:header_byte_length]
    if envelope_type != 1:
        logger.error(blob)
        raise Exception("Envelope type not supported")
    minx = struct.unpack("d", envelope[0:8])[0]
    maxx = struct.unpack("d", envelope[8:16])[0]
    miny = struct.unpack("d", envelope[16:24])[0]
    maxy = struct.unpack("d", envelope[24:32])[0]
    x = (minx + maxx) / 2
    y = (miny + maxy) / 2

    return Point(x, y)


def get_wbid_from_point(coords):
    """
    Retrieves the watershed boundary ID (wbid) of the watershed that contains the given point.

    Args:
        coords (dict): A dictionary containing the latitude and longitude coordinates of the point.
            Example: {"lat": 40.7128, "lng": -74.0060}

    Returns:
        int: The watershed boundary ID (wbid) of the watershed containing the point.

    Raises:
        IndexError: If no watershed boundary is found for the given point.

    """
    logger.info(f"Getting wbid for {coords}")
    q = file_paths.conus_hydrofabric()
    d = {"col1": ["point"], "geometry": [Point(coords["lng"], coords["lat"])]}
    point = gpd.GeoDataFrame(d, crs="EPSG:4326")
    df = gpd.read_file(q, format="GPKG", layer="divides", mask=point)
    return df["id"].values[0]


def copy_rTree_tables(
    table: str, ids: List[str], source_db: sqlite3.Connection, dest_db: sqlite3.Connection
) -> None:
    """
    Copy rTree tables from source database to destination database.
    This contains the spatial index for the specified table.
    Copying it saves us from having to rebuild the index.
    Args:
        table (str): The table name.
        ids (List[str]): The list of IDs.
        source_db (sqlite3.Connection): The source database connection.
        dest_db (sqlite3.Connection): The destination database connection.
    """
    rTree_tables = [f"rtree_{table}_geom{suffix}" for suffix in ["", "_rowid", "_node", "_parent"]]

    rowid_data = source_db.execute(
        f"SELECT * FROM {rTree_tables[1]} WHERE rowid in ({','.join(ids)})"
    ).fetchall()

    node_ids = [str(x[1]) for x in rowid_data]
    node_data = source_db.execute(
        f"SELECT * FROM {rTree_tables[2]} WHERE nodeno in ({','.join(node_ids)})"
    ).fetchall()

    parent_data = source_db.execute(
        f"SELECT * FROM {rTree_tables[3]} WHERE nodeno in ({','.join(node_ids)})"
    ).fetchall()

    insert_data(dest_db, rTree_tables[2], node_data)
    insert_data(dest_db, rTree_tables[1], rowid_data)
    insert_data(dest_db, rTree_tables[3], parent_data)


def insert_data(con: sqlite3.Connection, table: str, contents: List[Tuple]) -> None:
    """
    Insert data into the specified table.

    Args:
        con (sqlite3.Connection): The database connection.
        table (str): The table name.
        contents (List[Tuple]): The data to be inserted.
    """
    if len(contents) == 0:
        return

    logger.debug(f"Inserting {table}")
    placeholders = ",".join("?" * len(contents[0]))
    con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", contents)
    con.commit()


def subset_table(table: str, ids: List[str], hydrofabric: str, subset_gpkg_name: str) -> None:
    """
    Subset the specified table from the hydrofabric database and save it to the subset geopackage.

    Args:
        table (str): The table name.
        ids (List[str]): The list of IDs.
        hydrofabric (str): The path to the hydrofabric database.
        subset_gpkg_name (str): The name of the subset geopackage.
    """
    if table == "flowpath_edge_list":
        table = "network"

    logger.info(f"Subsetting {table} in {subset_gpkg_name}")
    source_db = sqlite3.connect(hydrofabric)
    dest_db = sqlite3.connect(subset_gpkg_name)

    if table == "nexus":
        sql_query = f"SELECT toid FROM divides"
        contents = dest_db.execute(sql_query).fetchall()
        ids = [str(x[0]) for x in contents]

    ids = [f"'{x}'" for x in ids]
    sql_query = f"SELECT * FROM {table} WHERE id IN ({','.join(ids)})"
    contents = source_db.execute(sql_query).fetchall()

    ids = [str(x[0]) for x in contents]

    if table in ["divides", "flowpaths", "nexus", "hydrolocations", "lakes"]:
        copy_rTree_tables(table, ids, source_db, dest_db)

    logger.debug("Inserting final data")

    if table == "network":
        table = "flowpath_edge_list"

    insert_data(dest_db, table, contents)
    dest_db.commit()
    source_db.close()
    dest_db.close()


def remove_triggers(dest_db: str) -> List[Tuple]:
    """
    Remove triggers from the specified database.
    As they break any inserts we don't remove them.
    Args:
        dest_db (str): The path to the destination database.

    Returns:
        List[(t_name, t_sql)]: The list of triggers that were removed.
    """
    con = sqlite3.connect(dest_db)
    triggers = con.execute("SELECT name, sql FROM sqlite_master WHERE type = 'trigger'").fetchall()

    for trigger in triggers:
        con.execute(f"DROP TRIGGER {trigger[0]}")

    con.commit()
    con.close()
    return triggers


def add_triggers(triggers: List[Tuple], dest_db: str) -> None:
    """
    Add triggers to the specified database.

    Args:
        triggers (List[Tuple]): The list of triggers to be added.
        dest_db (str): The path to the destination database.
    """
    con = sqlite3.connect(dest_db)

    for trigger in triggers:
        con.execute(trigger[1])

    con.commit()
    con.close()


def get_table_crs(gpkg: str, table: str) -> str:
    """
    Get the CRS of the specified table in the specified geopackage.

    Args:
        gpkg (str): The path to the geopackage.
        table (str): The table name.

    Returns:
        str: The CRS of the table.
    """
    con = sqlite3.connect(gpkg)
    sql_query = f"SELECT g.definition FROM gpkg_geometry_columns AS c JOIN gpkg_spatial_ref_sys AS g ON c.srs_id = g.srs_id WHERE c.table_name = '{table}'"
    crs = con.execute(sql_query).fetchone()[0]
    con.close()
    return crs


def get_nex_from_gage_id(gage_id: str, gpkg: Path = file_paths.conus_hydrofabric()) -> str:
    """
    Get the nexus id of associated with a gage id.

    Args:
        gage_id (str): The gage ID.

    Returns:
        str: The nexus id of the watershed containing the gage ID.

    Raises:
        IndexError: If nexus is found for the given gage ID.

    """
    gage_id = "".join([x for x in gage_id if x.isdigit()])
    logger.info(f"Getting wbid for {gage_id}, in {gpkg}")
    with sqlite3.connect(gpkg) as con:
        sql_query = f"SELECT id FROM hydrolocations WHERE hl_uri = 'Gages-{gage_id}'"
        nex_id = con.execute(sql_query).fetchone()[0]
    if nex_id is None:
        raise IndexError(f"No nexus found for gage ID {gage_id}")
    return nex_id
