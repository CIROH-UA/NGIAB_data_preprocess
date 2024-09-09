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
import pyproj
from shapely.ops import transform

logger = logging.getLogger(__name__)


def verify_indices(gpkg: str = file_paths.conus_hydrofabric) -> None:
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


def create_empty_gpkg(gpkg: str) -> None:
    """
    Create an empty geopackage with the necessary tables and indices.
    """
    with open(file_paths.template_sql) as f:
        sql_script = f.read()

    with sqlite3.connect(gpkg) as conn:
        conn.executescript(sql_script)


def add_triggers_to_gpkg(gpkg: str) -> None:
    """
    Adds geopackage triggers required to maintain spatial index integrity
    """
    with open(file_paths.triggers_sql) as f:
        triggers = f.read()
    with sqlite3.connect(gpkg) as conn:
        conn.executescript(triggers)

    logger.debug(f"Added triggers to subset gpkg {gpkg}")


# whenever this is imported, check if the indices are correct
if file_paths.conus_hydrofabric.is_file():
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


def blob_to_centre_point(blob: bytes) -> Point:
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


def convert_to_5070(shapely_geometry):
    # convert to web mercator
    if shapely_geometry.is_empty:
        return shapely_geometry
    source_crs = pyproj.CRS("EPSG:4326")
    target_crs = pyproj.CRS("EPSG:5070")
    project = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True).transform
    new_geometry = transform(project, shapely_geometry)
    logger.debug(f" new geometry: {new_geometry}")
    logger.debug(f"old geometry: {shapely_geometry}")
    return new_geometry


def get_catid_from_point(coords):
    """
    Retrieves the watershed boundary ID (catid) of the watershed that contains the given point.

    Args:
        coords (dict): A dictionary containing the latitude and longitude coordinates of the point.
            Example: {"lat": 40.7128, "lng": -74.0060}

    Returns:
        int: The watershed boundary ID (catid) of the watershed containing the point.

    Raises:
        IndexError: If no watershed boundary is found for the given point.

    """
    logger.info(f"Getting catid for {coords}")
    q = file_paths.conus_hydrofabric
    point = Point(coords["lng"], coords["lat"])
    point = convert_to_5070(point)
    with sqlite3.connect(q) as con:
        sql = f"""SELECT DISTINCT d.divide_id, d.geom
                FROM divides d
                JOIN rtree_divides_geom r ON d.fid = r.id
                WHERE r.minx <= {point.x} AND r.maxx >= {point.x}
                AND r.miny <= {point.y} AND r.maxy >= {point.y}"""
        results = con.execute(sql).fetchall()
    if len(results) == 0:
        raise IndexError(f"No watershed boundary found for {coords}")
    if len(results) > 1:
        # check the geometries to see which one contains the point
        for result in results:
            geom = blob_to_geometry(result[1])
            if geom.contains(point):
                return result[0]
    return results[0][0]


def create_rTree_table(table: str, con: sqlite3.Connection) -> None:
    """
    Create an rTree table for the specified table.

    Args:
        table (str): The table name.
        con (sqlite3.Connection): The database connection.
    """
    con.execute(
        f'CREATE VIRTUAL TABLE "rtree_{table}_geom" USING rtree("id", "minx", "maxx", "miny", "maxy")'
    )
    con.commit()


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
    rTree_table = f"rtree_{table}_geom"

    create_rTree_table(table, dest_db)

    geom_data = source_db.execute(
        f"SELECT * FROM {rTree_table} WHERE id in ({','.join(ids)})"
    ).fetchall()
    insert_data(dest_db, rTree_table, geom_data)


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


def update_geopackage_metadata(gpkg: str) -> None:
    """
    Update the contents of the gpkg_contents table in the specified geopackage.
    """
    # table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id
    tables = ["nexus", "flowpaths", "divides", "hydrolocations"]
    con = sqlite3.connect(gpkg)
    for table in tables:
        min_x = con.execute(f"SELECT MIN(minx) FROM rtree_{table}_geom").fetchone()[0]
        min_y = con.execute(f"SELECT MIN(miny) FROM rtree_{table}_geom").fetchone()[0]
        max_x = con.execute(f"SELECT MAX(maxx) FROM rtree_{table}_geom").fetchone()[0]
        max_y = con.execute(f"SELECT MAX(maxy) FROM rtree_{table}_geom").fetchone()[0]
        srs_id = con.execute(
            f"SELECT srs_id FROM gpkg_geometry_columns WHERE table_name = '{table}'"
        ).fetchone()[0]
        sql_command = f"INSERT INTO gpkg_contents (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id) VALUES ('{table}', 'features', '{table}', '', datetime('now'), {min_x}, {min_y}, {max_x}, {max_y}, {srs_id})"
        sql_command = sql_command.replace("None", "NULL")
        con.execute(sql_command)
    con.commit()

    # do some gpkg spec updating
    con.execute("PRAGMA application_id = '0x47504B47'")
    con.execute("PRAGMA user_version = 10200")
    con.commit()

    # update the gpkg_ogr_contents table with table_name and number of features
    tables.append("flowpath_attributes")
    tables.append("flowpath_edge_list")
    for table in tables:
        num_features = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        con.execute(
            f"INSERT INTO gpkg_ogr_contents (table_name, feature_count) VALUES ('{table}', {num_features})"
        )

    con.close()


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

    if table == "network":
        table = "flowpath_edge_list"

    insert_data(dest_db, table, contents)

    if table in ["divides", "flowpaths", "nexus", "hydrolocations", "lakes"]:
        copy_rTree_tables(table, ids, source_db, dest_db)

    dest_db.commit()
    source_db.close()
    dest_db.close()


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


def get_cat_from_gage_id(gage_id: str, gpkg: Path = file_paths.conus_hydrofabric) -> str:
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

    if len(gage_id) < 8:
        logger.warning(f"Gages in the hydrofabric are at least 8 digits {gage_id}")
        old_gage_id = gage_id
        gage_id = f"{int(gage_id):08d}"
        logger.warning(f"Converted {old_gage_id} to {gage_id}")

    logger.info(f"Getting catid for {gage_id}, in {gpkg}")

    # the hydrolocations table seems to have a bunch of errors in it
    # use flowpath_attributes instead
    # both have errors, cross reference them
    with sqlite3.connect(gpkg) as con:
        sql_query = f"""SELECT f.id 
                        FROM flowpaths AS f 
                        JOIN hydrolocations AS h ON f.toid = h.id
                        JOIN flowpath_attributes AS fa ON f.id = fa.id
                        WHERE h.hl_uri = 'Gages-{gage_id}' 
                        AND fa.rl_gages LIKE '%{gage_id}%'"""
        result = con.execute(sql_query).fetchall()
        if len(result) == 0:
            logger.critical(f"Gage ID {gage_id} is not associated with any waterbodies")
            raise IndexError(f"Could not find a waterbody for gage {gage_id}")
        if len(result) > 1:
            logger.critical(f"Gage ID {gage_id} is associated with multiple waterbodies")
            raise IndexError(f"Could not find a unique waterbody for gage {gage_id}")

        wb_id = result[0][0]
        cat_id = wb_id.replace("wb", "cat")

    return cat_id


def get_cat_to_nex_flowpairs(hydrofabric: Path = file_paths.conus_hydrofabric) -> List[Tuple]:
    """
    Retrieves the from and to IDs from the specified hydrofabric.

    This functions returns a list of tuples containing (catchment ID, nexus ID).
    The true network flows catchment to waterbody to nexus, this bypasses the waterbody and returns catchment to nexus.

    Args:
        hydrofabric (Path, optional): The file path to the hydrofabric. Defaults to file_paths.conus_hydrofabric.
    Returns:
        List[tuple]: A list of tuples containing the from and to IDs.
    """
    sql_query = "SELECT divide_id, toid FROM divides"
    try:
        con = sqlite3.connect(str(hydrofabric.absolute()))
        edges = con.execute(sql_query).fetchall()
        con.close()
    except sqlite3.Error as e:
        logger.error(f"SQLite error: {e}")
        raise
    unique_edges = list(set(edges))
    return unique_edges
