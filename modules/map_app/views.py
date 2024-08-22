import json
import math
import re
import sqlite3
from datetime import datetime
from pathlib import Path
import logging
import geopandas as gpd
import requests
from flask import Blueprint, jsonify, render_template, request
from shapely import unary_union, to_geojson
from shapely.geometry import LineString, Point, shape
from shapely.ops import transform
import pyproj
import multiprocessing

from data_processing.gpkg_utils import (
    get_table_crs,
    blob_to_geometry,
    get_catid_from_point,
    get_cat_from_gage_id,
)
from data_processing.create_realization import create_realization
from data_processing.file_paths import file_paths
from data_processing.forcings import create_forcings
from data_processing.graph_utils import get_from_to_id_pairs, get_upstream_ids
from data_processing.subset import subset

from time import time

main = Blueprint("main", __name__)
intra_module_db = {}

logger = logging.getLogger(__name__)


@main.route("/")
def index():
    return render_template("index.html")


def get_catid_from_point(coords):
    # inpute coords are EPSG:4326
    logger.info(coords)
    # takes a point and returns the catid of the watershed it is in
    # create a geometry mask for the point
    # load the watershed boundaries
    q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg"
    d = {"col1": ["point"], "geometry": [Point(coords["lng"], coords["lat"])]}
    point = gpd.GeoDataFrame(d, crs="EPSG:4326")
    df = gpd.read_file(q, format="GPKG", layer="divides", mask=point)
    return df["divide_id"].values[0]


@main.route("/handle_map_interaction", methods=["POST"])
def handle_map_interaction():
    data = request.get_json()
    coordinates = data.get("coordinates")
    cat_id = get_catid_from_point(coordinates)
    result = {
        "status": "success",
        "message": "Received coordinates: {}".format(coordinates),
        "cat_id": cat_id,
    }
    return jsonify(result)


def convert_grid_to_coords(xmin, ymin, xmax, ymax):
    # converts tile x,y index to lat/lon EPSG:4326
    zoom = 18
    n = 2**zoom
    xmin = xmin / n * 360.0 - 180.0
    xmax = xmax / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ymin / n)))
    ymin = lat_rad * 180.0 / math.pi
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ymax / n)))
    ymax = lat_rad * 180.0 / math.pi
    return [xmin, abs(ymin), xmax, abs(ymax)]


@main.route("/get_map_data", methods=["GET"])
def get_map_data():
    # Get the URL from the query string
    url = request.args.get("url")
    if not url:
        return jsonify({"error": "Missing URL parameter"}), 400
    try:
        # Make the request to the external URL
        response = requests.get(url)
        if response.status_code == 404:
            results = re.findall(r"[0-9]+(?=,)", response.content.decode("utf-8"))
            results = results[:4]
            results = [float(x) for x in results]
            bounds = convert_grid_to_coords(*results)
        return bounds, 200

    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 500


def catids_to_geojson(cat_dict):
    for k, v in cat_dict.items():
        if v[0] != 0 and v[1] != 0:
            # assuming this was called by clicking on the map
            cat_dict[k] = Point(v[1], v[0])
        else:
            # this was called without knowing the coordinates
            # use sql to get the geometry
            conn = sqlite3.connect(file_paths.conus_hydrofabric())
            c = conn.cursor()
            c.execute(f"SELECT geom FROM divides WHERE id = '{k}'")
            result = c.fetchone()
            if result is not None:
                cat_dict[k] = convert_to_4326(blob_to_geometry(result[0])).buffer(-0.0001)

    d = {"col1": cat_dict.keys(), "geometry": cat_dict.values()}
    points = gpd.GeoDataFrame(d, crs="EPSG:4326")
    logger.debug(points)
    q = Path(__file__).parent.parent / "data_sources" / "conus.gpkg"
    df = gpd.read_file(q, format="GPKG", layer="divides", mask=points)
    # convert crs to 4326
    df = df.to_crs(epsg=4326)
    return df.to_json()


@main.route("/get_geojson_from_catids", methods=["POST"])
def get_geojson_from_catids():
    cat_dict = json.loads(request.data.decode("utf-8"))
    logger.debug(cat_dict)
    if len(cat_dict) == 0:
        return [], 204
    try:
        return catids_to_geojson(cat_dict), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_upstream_geometry(upstream_ids):
    geopackage = file_paths.conus_hydrofabric()
    sql_query = f"SELECT id, geom FROM divides WHERE id IN {tuple(upstream_ids)}"
    # remove the trailing comma from single element tuples
    sql_query = sql_query.replace(",)", ")")
    # get nexus locations
    start_time = time()
    with sqlite3.connect(geopackage) as con:
        result = con.execute(sql_query).fetchall()
    logger.info(f"sql took {time() - start_time}")
    # convert the blobs to geometries
    geometry_list = []
    logger.debug(f"sql returned at {datetime.now()}")
    for r in result:
        geometry = blob_to_geometry(r[1])
        if geometry is not None:
            geometry_list.append(geometry)
    logger.debug(f"converted blobs to geometries at {datetime.now()}")
    # split geometries into chunks and run unary_union in parallel?
    start_time = time()
    merged_geometry = unary_union(geometry_list)
    logger.info(f"unary_union took {time() - start_time}")

    return merged_geometry


def convert_to_4326(shapely_geometry):
    # convert to web mercator
    if shapely_geometry.is_empty:
        return shapely_geometry
    geopkg_crs = get_table_crs(file_paths.conus_hydrofabric(), "divides")
    source_crs = pyproj.CRS(geopkg_crs)
    logger.debug(f"source crs: {source_crs}")
    target_crs = pyproj.CRS("EPSG:4326")
    project = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True).transform
    new_geometry = transform(project, shapely_geometry)
    logger.debug(f" new geometry: {new_geometry}")
    logger.debug(f"old geometry: {shapely_geometry}")
    return new_geometry


@main.route("/get_upstream_geojson_from_catids", methods=["POST"])
def get_upstream_geojson_from_catids():
    start_time = time()
    logger.debug(f"got upstream catids at {datetime.now()}")

    cat_id = json.loads(request.data.decode("utf-8"))
    logger.debug(f"got cat_id: {cat_id} at {datetime.now()}")
    upstream_ids = get_upstream_ids(cat_id)
    logger.debug(f"got upstream ids at {datetime.now()}")
    # remove the selected cat_id from the set
    upstream_ids.remove(cat_id.replace("cat", "wb"))
    logger.debug(f"removed cat_id from upstream_ids at {datetime.now()}")
    upstream_polygon = get_upstream_geometry(upstream_ids)
    logger.debug(f"got upstream geometry at {datetime.now()}")
    upstream_polygon = convert_to_4326(upstream_polygon)
    logger.debug(f"converted to 4326 at {datetime.now()}")
    logger.debug(f"total time: {time() - start_time}")
    return to_geojson(upstream_polygon), 200


@main.route("/get_flowlines_from_catids", methods=["POST"])
def get_flowlines_from_catids():
    cat_id = json.loads(request.data.decode("utf-8"))
    upstream_ids = get_upstream_ids(cat_id)
    flow_lines = get_from_to_id_pairs(ids=upstream_ids)
    all_ids = list(set([x for y in flow_lines for x in y]))
    geopackage = file_paths.conus_hydrofabric()

    sql_query_divides = f"""SELECT d.id,
    (r.minx + r.maxx) / 2.0 AS center_x,
    (r.miny + r.maxy) / 2.0 AS center_y
    FROM divides AS d
    JOIN rtree_divides_geom AS r ON d.fid = r.id
    WHERE d.id IN {tuple(all_ids)};"""
    sql_query_nexus = f"SELECT id, geom FROM nexus WHERE id IN {tuple(all_ids)}"
    # remove the trailing comma from single element tuples
    sql_query_divides = sql_query_divides.replace(",)", ")")
    sql_query_nexus = sql_query_nexus.replace(",)", ")")
    # get nexus locations
    with sqlite3.connect(geopackage) as con:
        result_divides = con.execute(sql_query_divides).fetchall()
        result_nexus = con.execute(sql_query_nexus).fetchall()

    divide_geometries = {}
    nexus_geometries = {}
    for r in result_divides:
        divide_geometries[r[0]] = Point(r[1], r[2])
    for r in result_nexus:
        nexus_geometries[r[0]] = blob_to_geometry(r[1])

    # merge the dictionaries
    divide_geometries.update(nexus_geometries)
    logger.debug(flow_lines)

    # generate a line for each flowline
    to_nexus = []  # flow from cat to nexus
    to_cat = []  # flow from nexus to cat
    for line in flow_lines:
        if line[0].startswith("nex"):
            # only pairs beginning with nex flow to cat, tnx (terminal nexus) don't flow
            to_cat.append(LineString([divide_geometries[line[0]], divide_geometries[line[1]]]))
        else:
            to_nexus.append(LineString([divide_geometries[line[0]], divide_geometries[line[1]]]))

    to_cat = convert_to_4326(unary_union(to_cat))
    to_nexus = convert_to_4326(unary_union(to_nexus))
    if len(nexus_geometries) > 0:
        nexus_geometries = unary_union(list(nexus_geometries.values()))
        nexus_geometries = convert_to_4326(nexus_geometries)
        response = {
            "to_cat": to_geojson(to_cat),
            "to_nexus": to_geojson(to_nexus),
            "nexus": to_geojson(nexus_geometries),
        }
    else:
        response = {"to_cat": to_geojson(to_cat), "to_nexus": to_geojson(to_nexus)}

    return response, 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    cat_ids = list(json.loads(request.data.decode("utf-8")).keys())
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    subset_folder = subset(cat_ids, subset_name=subset_name)
    return subset_folder, 200


@main.route("/subset_to_file", methods=["POST"])
def subset_to_file():
    cat_ids = list(json.loads(request.data.decode("utf-8")).keys())
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    total_subset = get_upstream_ids(cat_ids)
    subset_paths = file_paths(subset_name)
    output_file = subset_paths.subset_dir() / "subset.txt"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        f.write("\n".join(total_subset))
    return str(subset_paths.subset_dir()), 200


@main.route("/forcings", methods=["POST"])
def get_forcings():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    cat_id = data.get("forcing_dir").split("/")[-1]
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    # logger.info(intra_module_db)
    app = intra_module_db["app"]
    debug_enabled = app.debug
    app.debug = False
    logger.info(f"get_forcings() disabled debug mode at {datetime.now()}")
    try:
        create_forcings(start_time, end_time, cat_id)
    except Exception as e:
        logger.info(f"get_forcings() failed with error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    app.debug = debug_enabled

    return "success", 200


@main.route("/realization", methods=["POST"])
def get_realization():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    cat_id = data.get("forcing_dir").split("/")[-1]
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    create_realization(cat_id, start_time, end_time)
    return "success", 200


@main.route("/get_catids_from_vpu", methods=["POST"])
def get_catids_from_vpu():
    vpu = json.loads(request.data.decode("utf-8"))
    vpu = shape(vpu)
    # convert to crs 5070
    vpu = gpd.GeoDataFrame({"geometry": [vpu]}, crs="EPSG:4326")
    vpu = vpu.to_crs(epsg=5070)
    cats = gpd.read_file(file_paths.data_sources() / "conus.gpkg", layer="divides", mask=vpu)
    cats = cats.to_crs(epsg=4326)
    cats = cats[cats["id"].notna()]
    # return dict[id: [lat, lon]]
    return (
        json.dumps(
            dict(
                zip(
                    cats["divide_id"],
                    zip(cats["geometry"].centroid.x, cats["geometry"].centroid.y),
                )
            )
        ),
        200,
    )


@main.route("/logs", methods=["GET"])
def get_logs():
    log_file_path = "app.log"
    try:
        with open(log_file_path, "r") as file:
            lines = file.readlines()
            reversed_lines = []
            for line in reversed(lines):
                if "werkzeug" not in line:
                    reversed_lines.append(line)
                if len(reversed_lines) > 100:
                    break
            return jsonify({"logs": reversed_lines}), 200
    except Exception as e:
        return jsonify({"error": str(e)})


@main.route("/get_catid_from_gage_id", methods=["POST"])
def get_catid_from_gage_id():
    gage_id = json.loads(request.data.decode("utf-8"))["gage_id"]
    result = get_cat_from_gage_id(gage_id)
    if result is not None:
        return jsonify({"cat_ids": result}), 200
    return jsonify({"error": "No catid found for gage id"}), 404
