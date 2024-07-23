## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
from data_processing.file_paths import file_paths
import os
from threading import Timer
from flask import Flask
from flask_cors import CORS
import logging
from .views import intra_module_db, main

import webbrowser

from data_sources.source_validation import download_file, decompress_gzip_tar, validate_all

validate_all()

if not file_paths.tiles_tms().is_dir():
    print("Downloading catchment boundary map")
    download_file(
        "https://www.hydroshare.org/resource/f37d4b10e28a4694825c30fc38e2a7a0/data/contents/tms8-11.tar.gz",
        "temp/tms.tar.gz",
    )
    print("Unzipping catchment boundary map")
    decompress_gzip_tar("temp/tms.tar.gz", file_paths.tiles_tms())
    os.remove("temp/tms.tar.gz")

if not file_paths.tiles_vpu().is_dir():
    print("Downloading vpu boundary map")
    download_file(
        "https://www.hydroshare.org/resource/f37d4b10e28a4694825c30fc38e2a7a0/data/contents/vpu.tar.gz",
        "temp/vpu.tar.gz",
    )
    print("Unzipping vpu boundary map")
    decompress_gzip_tar("temp/vpu.tar.gz", file_paths.tiles_vpu().parent)
    os.remove("temp/vpu.tar.gz")


with open("app.log", "w") as f:
    f.write("")
    f.write("Starting Application!\n")


app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app

CORS(app)

logging.basicConfig(
    level=logging.INFO,
    format="%(name)-12s: %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)  # Append mode
# Example: Adding a console handler to root logger (optional)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Or any other level
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger("").addHandler(console_handler)


def open_browser():
    # find the last line in the log file that contains the port number
    # * running on http://0.0.0.0:port_number
    port_number = None
    with open("app.log", "r") as f:
        lines = f.readlines()
        for line in reversed(lines):
            if "Running on http" in line:
                port_number = line.split(":")[-1].strip()
                break
    if port_number is not None:
        webbrowser.open(f"http://localhost:{port_number}")
    else:
        print("Could not find the port number in the log file. Please open the url manually.")
    set_logs_to_warning()


def set_logs_to_warning():
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


if __name__ == "__main__":

    if file_paths.dev_file().is_file():
        Timer(2, set_logs_to_warning).start()
        with open("app.log", "a") as f:
            f.write("Running in debug mode\n")
        app.run(debug=True, host="0.0.0.0", port="0")
    else:
        Timer(1, open_browser).start()
        with open("app.log", "a") as f:
            f.write("Running in production mode\n")
        app.run(host="0.0.0.0", port="0")
