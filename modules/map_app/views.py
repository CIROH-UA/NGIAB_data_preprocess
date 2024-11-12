import json
import logging
from datetime import datetime

from data_processing.create_realization import create_realization
from data_processing.file_paths import file_paths
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


@main.route("/get_upstream_catids", methods=["POST"])
def get_upstream_catids():
    cat_id = json.loads(request.data.decode("utf-8"))
    upstream_cats = get_upstream_cats(cat_id)
    if cat_id in upstream_cats:
        upstream_cats.remove(cat_id)
    return list(upstream_cats), 200


@main.route("/get_upstream_wbids", methods=["POST"])
def get_upstream_wbids():
    cat_id = json.loads(request.data.decode("utf-8"))
    upstream_ids = get_upstream_ids(cat_id)
    # remove the selected cat_id from the set
    return [id for id in upstream_ids if id.startswith("wb")], 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    cat_ids = list(json.loads(request.data.decode("utf-8")))
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    subset_folder = subset(cat_ids, output_folder_name=subset_name)
    return subset_folder, 200


@main.route("/subset_to_file", methods=["POST"])
def subset_to_file():
    raise NotImplementedError
    cat_ids = list(json.loads(request.data.decode("utf-8")).keys())
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    total_subset = get_upstream_ids(cat_ids)
    subset_paths = file_paths(subset_name)
    output_file = subset_paths.subset_dir / "subset.txt"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        f.write("\n".join(total_subset))
    return str(subset_paths.subset_dir), 200


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
    except Exception as e:
        return jsonify({"error": str(e)})
