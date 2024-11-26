import logging
import sqlite3
import struct
from pathlib import Path
from typing import List, Tuple, Union

import pyproj
from data_processing.file_paths import file_paths
from shapely.geometry import Point, Polygon
from shapely.ops import transform
from shapely.wkb import loads

logger = logging.getLogger(__name__)


class GeoPackage:
    def __init__(self, file_name):
        self.file_name = file_name

    def __enter__(self):
        self.conn = sqlite3.connect(self.file_name)
        self.conn.enable_load_extension(True)
        self.conn.load_extension("mod_spatialite")
        return self.conn

    def __exit__(self, *args):
        self.conn.close()


def verify_indices(gpkg: str = file_paths.conus_hydrofabric) -> None:
    """
    Verify that the indices in the specified geopackage are correct.
    If they are not, create the correct indices.
    """
    logger.debug("Building database indices")
    new_indicies = [
        'CREATE INDEX "diid" ON "divides" ( "divide_id" ASC );',
        'CREATE INDEX "ditid" ON "divides" ( "toid" ASC );',
        'CREATE INDEX "diaid" ON "divide-attributes" ( "divide_id" ASC );',
        'CREATE INDEX "flaid" ON "flowpath-attributes" ( "id" ASC );',
        'CREATE INDEX "fla_gageid" ON "flowpath-attributes" ( "gage" ASC );',
        'CREATE INDEX "fla_nex_to_gageid" ON "flowpath-attributes" ( "gage_nex_id" ASC );',
        'CREATE INDEX "flamlid" ON "flowpath-attributes-ml" ( "id" ASC );',
        'CREATE INDEX "flid" ON "flowpaths" ( "id" ASC );',
        'CREATE INDEX "hlid" ON "hydrolocations" ( "id" ASC );',
        'CREATE INDEX "gageid" ON "hydrolocations" ( "hl_uri" ASC );',
        'CREATE INDEX "laid" ON "lakes" ( "poi_id" ASC );',  # flowpaths.id->pois.id->pois.poi_id->lakes.poi_id
        'CREATE INDEX "poid" ON "pois" ( "id" ASC )',
        'CREATE INDEX "neid" ON "nexus" ( "id" ASC );',
        'CREATE INDEX "nid" ON "network" ( "id" ASC );',
        'CREATE INDEX "ntid" ON "network" ( "toid" ASC );',
        # no vpu index because the 1s query execution is tiny compared to the next steps
    ]
    # check if the gpkg has the correct indices
    con = sqlite3.connect(gpkg)
    indices = con.execute("SELECT name FROM sqlite_master WHERE type = 'index'").fetchall()
    indices = [x[0] for x in indices]
    missing = [x for x in new_indicies if x.split('"')[1] not in indices]
    if len(missing) > 0:
        logger.info("Creating indices")
    for index in new_indicies:
        if index.split('"')[1] not in indices:
            logger.info(f"Creating index {index}")
            con.execute(index)
            con.commit()
        # pragma optimize after creating the indices
        con.execute("PRAGMA optimize;")
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
    con.executemany(f"INSERT INTO '{table}' VALUES ({placeholders})", contents)
    con.commit()


def update_geopackage_metadata(gpkg: str) -> None:
    """
    Update the contents of the gpkg_contents table in the specified geopackage.
    """
    # table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id
    tables = get_feature_tables(file_paths.conus_hydrofabric)
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
    for table in tables:
        num_features = con.execute(f"SELECT COUNT(*) FROM '{table}'").fetchone()[0]
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
    logger.info(f"Subsetting {table} in {subset_gpkg_name}")
    source_db = sqlite3.connect(f"file:{hydrofabric}?mode=ro", uri=True)
    dest_db = sqlite3.connect(subset_gpkg_name)

    table_keys = {"divides": "toid", "divide-attributes": "divide_id", "lakes": "poi_id"}

    if table == "lakes":
        # lakes subset we get from the pois table which was already subset by water body id
        sql_query = "SELECT poi_id FROM 'pois'"
        contents = dest_db.execute(sql_query).fetchall()
        ids = [str(x[0]) for x in contents]

    if table == "divide-attributes":
        # get the divide ids from the divides that have been subset already
        sql_query = "SELECT divide_id FROM 'divides'"
        contents = dest_db.execute(sql_query).fetchall()
        ids = [str(x[0]) for x in contents]

    ids = [f"'{x}'" for x in ids]
    key_name = "id"
    if table in table_keys:
        key_name = table_keys[table]
    sql_query = f"SELECT * FROM '{table}' WHERE {key_name} IN ({','.join(ids)})"
    contents = source_db.execute(sql_query).fetchall()
    fids = [str(x[0]) for x in contents]

    insert_data(dest_db, table, contents)

    if table in get_feature_tables(file_paths.conus_hydrofabric):
        copy_rTree_tables(table, fids, source_db, dest_db)

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

    with sqlite3.connect(gpkg) as con:
        sql_query = f"SELECT id FROM 'flowpath-attributes' WHERE gage = '{gage_id}'"
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


def get_feature_tables(gpkg: Path) -> List[str]:
    """Takes a Path to a geopackage and returns a list of tables containing geometries."""
    sql_query = "SELECT table_name FROM gpkg_contents WHERE data_type='features'"
    with sqlite3.connect(gpkg) as conn:
        tables = conn.execute(sql_query).fetchall()
    tables = [i[0] for i in tables]
    return tables


def get_available_tables(gpkg: Path) -> List[str]:
    """Takes a Path to a geopackage and returns a list of non-metadata tables. aka gpd.list_layers()"""
    sql_query = "SELECT table_name FROM gpkg_contents"
    with sqlite3.connect(gpkg) as conn:
        tables = conn.execute(sql_query).fetchall()
    tables = [i[0] for i in tables]
    return tables


def get_cat_to_nhd_feature_id(gpkg: Path = file_paths.conus_hydrofabric) -> dict:

    available_tables = get_available_tables(gpkg)
    possible_tables = ["flowpath_edge_list", "network"]

    # get the intersection, less clear than an if else, but allows for more possible_tables
    tables = set(available_tables) & set(possible_tables)
    if len(tables) > 1:
        raise IndexError(f"More than one of the possible tables exists: {possible_tables}")
    if len(tables) == 0:
        raise IndexError(
            f"No source data found for NHD ID in {available_tables}, expected one of {possible_tables}"
        )

    table_name = list(tables)[0]
    sql_query = f"SELECT divide_id, hf_id FROM {table_name} WHERE divide_id IS NOT NULL AND hf_id IS NOT NULL"

    with sqlite3.connect(gpkg) as conn:
        result = conn.execute(sql_query).fetchall()

    mapping = {}
    for cat, feature in result:
        # the ids are stored as floats this converts to int to match nwm output
        # numeric ids should be stored as strings.
        mapping[cat] = int(feature)

    return mapping
