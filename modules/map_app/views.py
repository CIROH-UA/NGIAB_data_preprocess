import json
import logging
from datetime import datetime
from pathlib import Path
import threading

import geopandas as gpd
from data_processing.create_realization import create_realization
from data_processing.dataset_utils import save_and_clip_dataset
from data_processing.datasets import load_aorc_zarr, load_v3_retrospective_zarr
from data_processing.file_paths import FilePaths
from data_processing.forcings import create_forcings
from data_processing.graph_utils import get_upstream_ids
from data_processing.subset import subset
from flask import Blueprint, jsonify, render_template, request

main = Blueprint("main", __name__)
intra_module_db = {}

logger = logging.getLogger(__name__)


@main.route("/")
def index():
    return render_template("index.html")


@main.route("/get_upstream_catids", methods=["POST"])
def get_upstream_catids():
    """Get upstream catchments (excludes downstream nexus)."""
    cat_id = json.loads(request.data.decode("utf-8"))
    wb_id = "wb-" + cat_id.split("-")[-1]
    upstream_cats = get_upstream_ids(wb_id, include_outlet=False)
    
    cleaned_upstreams = set()
    for id in upstream_cats:
        if id.startswith("wb-"):
            cleaned_upstreams.add("cat-" + id.split("-")[-1])
    
    if cat_id in cleaned_upstreams:
        cleaned_upstreams.remove(cat_id)
    
    return list(cleaned_upstreams), 200


@main.route("/get_upstream_wbids", methods=["POST"])
def get_upstream_wbids():
    """Get upstream catchments (includes downstream nexus)."""
    cat_id = json.loads(request.data.decode("utf-8"))
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
    """Check if a subset geopackage already exists."""
    cat_ids = list(json.loads(request.data.decode("utf-8")))
    logger.info(f"Checking subset for: {cat_ids}")
    
    subset_name = cat_ids[0]
    run_paths = FilePaths(subset_name)
    
    if run_paths.geopackage_path.exists():
        return str(run_paths.geopackage_path), 409
    return "no conflict", 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    """Create a subset geopackage."""
    data = json.loads(request.data.decode("utf-8"))
    cat_ids = data.get("cat_id")
    subset_type = data.get("subset_type")
    
    logger.info(f"Creating subset: {cat_ids}, type: {subset_type}")
    
    subset_name = cat_ids[0]
    run_paths = FilePaths(subset_name)
    
    if subset_type == "nexus":
        subset(cat_ids, output_gpkg_path=run_paths.geopackage_path, override_gpkg=True)
    else:
        subset(cat_ids, output_gpkg_path=run_paths.geopackage_path, 
               include_outlet=False, override_gpkg=True)
    
    return str(run_paths.geopackage_path), 200


@main.route("/make_forcings_progress_file", methods=["POST"])
def make_forcings_progress_file():
    """Create a progress tracking file for forcings generation."""
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data.split("subset to ")[-1])
    paths = FilePaths(subset_gpkg.stem.split("_")[0])
    
    paths.forcing_progress_file.parent.mkdir(parents=True, exist_ok=True)
    with open(paths.forcing_progress_file, "w") as f:
        json.dump({"total_steps": 0, "steps_completed": 0}, f)
    
    return str(paths.forcing_progress_file), 200


@main.route("/forcings_progress", methods=["POST"])
def forcings_progress_endpoint():
    """Get the current forcings generation progress."""
    progress_file = Path(json.loads(request.data.decode("utf-8")))
    
    with open(progress_file, "r") as f:
        forcings_progress = json.load(f)
    
    total = forcings_progress['total_steps']
    completed = forcings_progress['steps_completed']
    
    try:
        percent = int((completed / total) * 100)
    except ZeroDivisionError:
        percent = "NaN"
    
    return str(percent), 200


def download_forcings(data_source, start_time, end_time, paths):
    """Download forcing data from the specified source."""
    if data_source == "aorc":
        raw_data = load_aorc_zarr(start_time.year, end_time.year)
    elif data_source == "nwm":
        raw_data = load_v3_retrospective_zarr()
    else:
        raise ValueError(f"Unknown data source: {data_source}")
    
    gdf = gpd.read_file(paths.geopackage_path, layer="divides")
    cached_data = save_and_clip_dataset(raw_data, gdf, start_time, end_time, 
                                         paths.cached_nc_file)
    return cached_data


def compute_forcings(cached_data, paths):
    """Compute forcings from cached data."""
    create_forcings(cached_data, paths.output_dir.stem)


@main.route("/forcings", methods=["POST"])
def get_forcings():
    """Generate forcings for the selected catchment."""
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data.get("forcing_dir").split("subset to ")[-1])
    output_folder = Path(subset_gpkg.parent.parent)
    paths = FilePaths(output_dir=output_folder)

    start_time = datetime.strptime(data.get("start_time"), "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(data.get("end_time"), "%Y-%m-%dT%H:%M")
    data_source = data.get("source")

    logger.info(f"Generating forcings: {output_folder}, source: {data_source}")

    app = intra_module_db["app"]
    debug_enabled = app.debug
    app.debug = False
    
    cached_data = download_forcings(data_source, start_time, end_time, paths)
    
    # Run forcings computation in background thread
    thread = threading.Thread(target=compute_forcings, args=(cached_data, paths))
    thread.start()
    
    app.debug = debug_enabled
    return "started", 200


@main.route("/realization", methods=["POST"])
def get_realization():
    """Create a realization configuration."""
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data.get("forcing_dir").split("subset to ")[-1])
    output_folder = subset_gpkg.parent.parent.stem
    
    start_time = datetime.strptime(data.get("start_time"), "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(data.get("end_time"), "%Y-%m-%dT%H:%M")
    
    logger.info(f"Creating realization: {output_folder}")
    create_realization(output_folder, start_time, end_time)
    
    return "success", 200


@main.route("/logs", methods=["GET"])
def get_logs():
    """Fetch recent application logs."""
    log_file_path = "app.log"
    
    try:
        with open(log_file_path, "r") as file:
            lines = file.readlines()
            reversed_lines = []
            for line in reversed(lines):
                if "werkzeug" not in line:
                    reversed_lines.append(line.strip())
                if len(reversed_lines) > 100:
                    break
            return jsonify({"logs": reversed_lines}), 200
    except Exception:
        return jsonify({"error": "unable to fetch logs"})
