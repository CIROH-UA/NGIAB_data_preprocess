import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import xarray as xr
from data_processing.create_realization import create_realization
from data_processing.dataset_utils import save_and_clip_dataset
from data_processing.datasets import load_aorc_zarr, load_v3_retrospective_zarr
from data_processing.file_paths import FilePaths
from data_processing.forcings import create_forcings
from data_processing.subset import subset
from flask import Blueprint, jsonify, render_template, request
from flask_sock import Sock

main = Blueprint("main", __name__)
sock = Sock()
intra_module_db = {}

logger = logging.getLogger(__name__)

LOG_FILE = Path.home() / ".ngiab" / "app.log"


@main.route("/")
def index():
    return render_template("index.html")


@main.route("/subset_check", methods=["POST"])
def subset_check():
    cat_ids = list(json.loads(request.data.decode("utf-8")))
    logger.info(cat_ids)
    subset_name = cat_ids[0]
    run_paths = FilePaths(subset_name)
    if run_paths.geopackage_path.exists():
        return str(run_paths.geopackage_path), 409
    else:
        return "no conflict", 200


@main.route("/subset", methods=["POST"])
def subset_selection():
    # body: JSON.stringify({ 'cat_id': [cat_id], 'subset_type': subset_type})
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
        subset(
            cat_ids,
            output_gpkg_path=run_paths.geopackage_path,
            include_outlet=False,
            override_gpkg=True,
        )
    return str(run_paths.geopackage_path), 200


@main.route("/make_forcings_progress_file", methods=["POST"])
def make_forcings_progress_file():
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data.split("subset to ")[-1])
    paths = FilePaths(subset_gpkg.stem.split("_")[0])
    paths.forcing_progress_file.parent.mkdir(parents=True, exist_ok=True)
    with open(paths.forcing_progress_file, "w") as f:
        json.dump({"total_steps": 0, "steps_completed": 0}, f)
    return str(paths.forcing_progress_file), 200


# Returns the forcings completion percentage, or None while the total is unknown
# (still downloading).
def read_forcings_percent(progress_file: Path):
    with open(progress_file, "r") as f:
        forcings_progress = json.load(f)
    try:
        return int((forcings_progress["steps_completed"] / forcings_progress["total_steps"]) * 100)
    except ZeroDivisionError:
        return None


# Push forcings progress to the client instead of being polled. The client
# sends the progress file path, then receives a percentage (or "NaN" while
# downloading) a few times a second until the run completes.
@sock.route("/ws/forcings_progress")
def forcings_progress_ws(ws):
    progress_file = Path(json.loads(ws.receive()))
    last_sent = None
    idle_ticks = 0
    while True:
        try:
            percent = read_forcings_percent(progress_file)
        except (FileNotFoundError, json.JSONDecodeError):
            percent = None
        message = "NaN" if percent is None else str(percent)
        # Resend unchanged progress occasionally so a vanished client is
        # detected (send raises) and this thread exits.
        if message != last_sent or idle_ticks >= 20:
            ws.send(message)
            last_sent = message
            idle_ticks = 0
        if percent is not None and percent >= 100:
            return
        idle_ticks += 1
        time.sleep(0.25)


def download_forcings(data_source, start_time, end_time, paths):
    if data_source == "aorc":
        raw_data = load_aorc_zarr(start_time.year, end_time.year)
    elif data_source == "nwm":
        raw_data = load_v3_retrospective_zarr()
    else:
        raise ValueError(f"Unknown data source: {data_source}")
    gdf = gpd.read_file(paths.geopackage_path, layer="divides")
    cached_data = save_and_clip_dataset(raw_data, gdf, start_time, end_time, paths.cached_zarr_file)
    return cached_data


def compute_forcings(cached_data, paths):
    create_forcings(cached_data, paths.output_dir.stem)  # type: ignore


@main.route("/forcings", methods=["POST"])
def get_forcings():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data["forcing_dir"])
    paths = FilePaths(output_dir=subset_gpkg.parent.parent)

    data_source = data.get("source")
    start_time = datetime.strptime(data["start_time"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(data["end_time"], "%Y-%m-%dT%H:%M")

    cached_data = download_forcings(data_source, start_time, end_time, paths)
    # threading implemented so that main process can periodically poll progress file
    thread = threading.Thread(target=compute_forcings, args=(cached_data, paths))
    thread.start()
    return "started", 200


@main.route("/realization", methods=["POST"])
def get_realization():
    # body: JSON.stringify({'forcing_dir': forcing_dir, 'start_time': start_time, 'end_time': end_time}),
    data = json.loads(request.data.decode("utf-8"))
    subset_gpkg = Path(data["forcing_dir"])
    output_folder = subset_gpkg.parent.parent.stem
    start_time = datetime.strptime(data["start_time"], "%Y-%m-%dT%H:%M")
    end_time = datetime.strptime(data["end_time"], "%Y-%m-%dT%H:%M")
    create_realization(output_folder, start_time, end_time)
    return "success", 200


@main.route("/gage_location", methods=["POST"])
def gage_location():
    data = json.loads(request.data.decode("utf-8"))
    gage_id = str(data.get("gage_id", "")).strip().zfill(8)

    if not gage_id:
        return jsonify({"error": "Missing gage ID"}), 400

    hydrofabric_path = Path.home() / ".ngiab" / "hydrofabric" / "v2.2" / "conus_nextgen.gpkg"

    if not hydrofabric_path.exists():
        return jsonify({"error": f"Hydrofabric not found at {hydrofabric_path}"}), 404

    if "hydrolocations_gdf" not in intra_module_db:
        gdf = gpd.read_file(hydrofabric_path, layer="hydrolocations")
        if gdf.crs and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)
        intra_module_db["hydrolocations_gdf"] = gdf

    gdf = intra_module_db["hydrolocations_gdf"]

    match = None
    for col in ["hl_link", "hl_uri", "id"]:
        if col in gdf.columns:
            extracted = gdf[col].astype(str).str.extract(r"(\d{8})", expand=False)
            rows = gdf[extracted == gage_id]
            if not rows.empty:
                match = rows.iloc[0]
                break

    if match is None:
        return jsonify({"error": f"Gage {gage_id} not found"}), 404

    geom = match.geometry
    point = geom if geom.geom_type == "Point" else geom.centroid

    return jsonify(
        {
            "gage_id": gage_id,
            "lon": point.x,
            "lat": point.y,
        }
    ), 200


# Run the complete workflow by translating UI selections into the existing CLI command.
@main.route("/run_cli", methods=["POST"])
def run_cli():
    data = json.loads(request.data.decode("utf-8"))

    # Build the CLI command from the complete workflow options selected in the map app UI.
    cmd = [sys.executable, "-m", "ngiab_data_cli"]

    input_feature = data.get("input_feature")
    if input_feature:
        cmd += ["-i", input_feature]

    if data.get("input_type") == "gage":
        cmd.append("--gage")

    cmd += ["--start", data["start_time"].split("T")[0]]
    cmd += ["--end", data["end_time"].split("T")[0]]

    # Pass the selected forcing dataset through to the CLI.
    source = data.get("source")
    if source:
        cmd += ["--source", source]

    # Add the selected model flag when the user chooses a non-default realization.
    model = data.get("model")
    if model:
        cmd.append(f"--{model}")

    # Run the requested steps (the complete workflow by default).
    step_flags = {"subset": "-s", "forcings": "-f", "realization": "-r", "run": "--run"}
    steps = data.get("steps") or list(step_flags)
    if any(step not in step_flags for step in steps):
        return jsonify({"error": f"Unknown steps: {steps}"}), 400
    cmd += [step_flags[step] for step in steps]

    logger.info("Running workflow command: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:
        logger.exception("Failed to start workflow")
        return jsonify(
            {
                "status": "failed",
                "error": str(exc),
                "command": " ".join(cmd),
            }
        ), 500

    combined_output = f"{result.stdout}\n{result.stderr}".lower()
    if result.returncode != 0 or "not found" in combined_output or "error" in combined_output:
        return jsonify(
            {
                "status": "failed",
                "error": result.stderr or result.stdout,
                "command": " ".join(cmd),
            }
        ), 500

    output_name = (
        f"gage-{input_feature}"
        if data.get("input_type") == "gage" and not input_feature.startswith("gage-")
        else input_feature
    )

    output_dir = FilePaths(output_name).output_dir

    return jsonify(
        {
            "status": "completed",
            "command": " ".join(cmd),
            "output": result.stdout,
            "output_dir": str(output_dir),
        }
    ), 200


# List run directories in the configured output root that contain t-route
# output, newest first, for the results viewer's folder picker.
@main.route("/output_dirs", methods=["GET"])
def output_dirs():
    root = FilePaths.root_output_dir()
    runs = [d for d in root.glob("*/outputs/troute") if any(d.glob("*.nc"))]
    runs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    return jsonify([str(d.parent.parent) for d in runs]), 200


# WGS84 bounds of a run's subset so the map can zoom to it, or None.
def subset_bounds(output_dir: Path):
    gpkgs = sorted((output_dir / "config").glob("*.gpkg"))
    if not gpkgs:
        return None
    bounds = gpd.read_file(gpkgs[0], layer="divides").to_crs(4326).total_bounds
    return [float(b) for b in bounds]


# Serve one variable of the newest t-route output in a run's output directory,
# keyed by numeric flowpath id so the map can color flowpaths client-side.
@main.route("/troute_output", methods=["POST"])
def troute_output():
    data = json.loads(request.data.decode("utf-8"))
    output_dir = Path(os.path.expanduser(str(data.get("output_dir", "")).strip()))
    variable = data.get("variable", "flow")

    if variable not in ("flow", "velocity", "depth"):
        return jsonify({"error": f"Unknown variable: {variable}"}), 400

    troute_dir = output_dir / "outputs" / "troute"
    nc_files = sorted(troute_dir.glob("*.nc"))
    if not nc_files:
        return jsonify({"error": f"No t-route output found in {troute_dir}"}), 404
    nc_file = nc_files[-1]

    try:
        with xr.open_dataset(nc_file) as ds:
            if variable not in ds:
                return jsonify({"error": f"{nc_file.name} has no '{variable}' variable"}), 404

            da = ds[variable]
            if set(da.dims) != {"feature_id", "time"}:
                return jsonify({"error": f"Unexpected dimensions {da.dims} for '{variable}'"}), 500
            values = da.transpose("feature_id", "time").values.astype(float)

            finite = values[np.isfinite(values)]
            vmin = float(finite.min()) if finite.size else 0.0
            vmax = float(finite.max()) if finite.size else 1.0
            # JSON has no NaN; use the t-route fill value convention instead.
            values = np.where(np.isfinite(values), values.round(3), -9999.0)

            feature_ids = ds["feature_id"].values
            times = ds["time"].values
            if np.issubdtype(times.dtype, np.datetime64):
                time_list = np.datetime_as_string(times, unit="m").tolist()
            else:
                time_list = [float(t) for t in times]
    except Exception as exc:
        logger.exception("Failed to read t-route output")
        return jsonify({"error": f"Failed to read {nc_file.name}: {exc}"}), 500

    try:
        bounds = subset_bounds(output_dir)
    except Exception:
        bounds = None

    return jsonify(
        {
            "file": str(nc_file),
            "variable": variable,
            "time": time_list,
            "min": vmin,
            "max": vmax,
            "bounds": bounds,
            "values": {str(int(fid)): row.tolist() for fid, row in zip(feature_ids, values)},
        }
    ), 200


# Stream app log lines: a tail of recent history on connect, then each new
# line as it is written, so the console updates live instead of polling.
@sock.route("/ws/logs")
def logs_ws(ws):
    def keep(line):
        return "werkzeug" not in line

    with open(LOG_FILE, "r") as f:
        history = [line.rstrip("\n") for line in f.readlines() if keep(line)]
        ws.send(json.dumps({"lines": history[-100:]}))

        partial = ""
        idle_ticks = 0
        while True:
            chunk = f.readline()
            if not chunk:
                idle_ticks += 1
                # Occasional keepalive so a vanished client is detected
                # (send raises) and this thread exits.
                if idle_ticks >= 40:
                    ws.send(json.dumps({"lines": []}))
                    idle_ticks = 0
                time.sleep(0.25)
                continue
            # readline can return a partial line if the writer is mid-write;
            # hold it until the newline arrives.
            partial += chunk
            if not partial.endswith("\n"):
                continue
            line, partial = partial.rstrip("\n"), ""
            idle_ticks = 0
            if keep(line):
                ws.send(json.dumps({"lines": [line]}))
