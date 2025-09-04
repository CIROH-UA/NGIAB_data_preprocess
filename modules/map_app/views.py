import json
import logging
from datetime import datetime
from pathlib import Path

import geopandas as gpd
from data_processing.create_realization import create_realization
from data_processing.dataset_utils import save_and_clip_dataset
from data_processing.datasets import load_aorc_zarr, load_v3_retrospective_zarr
from data_processing.file_paths import FilePaths
from data_processing.forcings import create_forcings
from data_processing.graph_utils import get_upstream_cats, get_upstream_ids
from data_processing.subset import subset
from flask import Blueprint, jsonify, render_template, request

main = Blueprint("main", __name__)
intra_module_db = {}

logger = logging.getLogger(__name__)


@main.route("/")
def index():
    return render_template("index.html")

# this subset does not include the downstream nexus
@main.route("/get_upstream_catids", methods=["POST"])
def get_upstream_catids():
    cat_id = json.loads(request.data.decode("utf-8"))
    # give wb_id to get_upstream_cats because the graph search is 1000x faster
    wb_id = "wb-" + cat_id.split("-")[-1]
    upstream_cats = get_upstream_ids(wb_id, include_outlet=False)
    cleaned_upstreams = set()
    for id in upstream_cats:
        if id.startswith("wb-"):
            cleaned_upstreams.add("cat-" + id.split("-")[-1])
    if cat_id in cleaned_upstreams:
        cleaned_upstreams.remove(cat_id)
    return list(cleaned_upstreams), 200

# this subset includes the downstream nexus
@main.route("/get_upstream_wbids", methods=["POST"])
def get_upstream_wbids():
    cat_id = json.loads(request.data.decode("utf-8"))
    # give wb_id to get_upstream_cats because the graph search is 1000x faster
    wb_id = "wb-" + cat_id.split("-")[-1]
    upstream_cats = get_upstream_ids(wb_id)
    cleaned_upstreams = set()
    for id in upstream_cats:
        if id.startswith("wb-"):
            cleaned_upstreams.add("cat-" + id.split("-")[-1])
    if cat_id in cleaned_upstreams:
        cleaned_upstreams.remove(cat_id)
    return list(cleaned_upstreams), 200


@main.route("/subset_check", methods=["POST"])
def subset_check():
    cat_ids = list(json.loads(request.data.decode("utf-8")))
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    run_paths = FilePaths(subset_name)
    if run_paths.geopackage_path.exists():
        return str(run_paths.geopackage_path), 409
    else:
        return 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    #body: JSON.stringify({ 'cat_id': [cat_id], 'subset_type': subset_type})
    data = json.loads(request.data.decode("utf-8"))
    cat_ids = data.get("cat_id")
    subset_type = data.get("subset_type")
    logger.info(cat_ids)
    logger.info(subset_type)
    subset_name = cat_ids[0]

    run_paths = FilePaths(subset_name)
    if subset_type == "nexus":
        subset(cat_ids, output_gpkg_path=run_paths.geopackage_path, override_gpkg=True)
    else:
        subset(cat_ids, output_gpkg_path=run_paths.geopackage_path, include_outlet=False, override_gpkg=True)
    return str(run_paths.geopackage_path), 200


@main.route("/subset_to_file", methods=["POST"])
def subset_to_file():
    raise NotImplementedError
    cat_ids = list(json.loads(request.data.decode("utf-8")).keys())
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    total_subset = get_upstream_ids(cat_ids)
    subset_paths = FilePaths(subset_name)
    output_file = subset_paths.subset_dir / "subset.txt"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        f.write("\n".join(total_subset))
    return str(subset_paths.subset_dir), 200


@main.route("/forcings", methods=["POST"])
def get_forcings():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data.get("forcing_dir").split("subset to ")[-1])
    output_folder = Path(subset_gpkg.parent.parent)
    paths = FilePaths(output_dir=output_folder)

    start_time = data.get("start_time")
    end_time = data.get("end_time")

    # get the selected data source
    data_source = data.get("source")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    # logger.info(intra_module_db)
    app = intra_module_db["app"]
    debug_enabled = app.debug
    app.debug = False
    logger.debug(f"get_forcings() disabled debug mode at {datetime.now()}")
    logger.debug(f"forcing_dir: {output_folder}")
    try:
        if data_source == "aorc":
            data = load_aorc_zarr(start_time.year, end_time.year)
        elif data_source == "nwm":
            data = load_v3_retrospective_zarr()
        gdf = gpd.read_file(paths.geopackage_path, layer="divides")
        cached_data = save_and_clip_dataset(data, gdf, start_time, end_time, paths.cached_nc_file)

        create_forcings(cached_data, paths.output_dir.stem)  # type: ignore
    except Exception as e:
        logger.info(f"get_forcings() failed with error: {str(e)}")
        return jsonify({"error": str(e)}), 500
    app.debug = debug_enabled

    return "success", 200


@main.route("/realization", methods=["POST"])
def get_realization():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data.get("forcing_dir").split("subset to ")[-1])
    output_folder = subset_gpkg.parent.parent.stem
    start_time = data.get("start_time")
    end_time = data.get("end_time")
    # get the forcings
    start_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(end_time, "%Y-%m-%dT%H:%M")
    create_realization(output_folder, start_time, end_time)
    return "success", 200


@main.route("/get_catids_from_vpu", methods=["POST"])
def get_catids_from_vpu():
    raise NotImplementedError


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
    except Exception:
        return jsonify({"error": "unable to fetch logs"})
