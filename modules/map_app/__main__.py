## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
from data_processing.file_paths import file_paths
import os
from threading import Timer
from flask import Flask
from flask_cors import CORS
import logging
from .views import intra_module_db, main
from tqdm import tqdm
import requests
import webbrowser


def download_file(url, save_path):
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    bytes_downloaded = 0
    chunk_size = 1048576
    with open(save_path, "wb") as f:
        for data in tqdm(
            response.iter_content(chunk_size=chunk_size),
            total=total_size / chunk_size,
            unit="MB",
            unit_scale=True,
        ):
            bytes_downloaded += len(data)
            f.write(data)


hydrofabric_url = "https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v20.1/conus.gpkg"
model_attributes_url = (
    "https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v20.1/model_attributes.parquet"
)


if not file_paths.conus_hydrofabric().is_file():
    # alert the user that the hydrofabric is missing
    print("Hydrofabric is missing. Would you like to download it now? (Y/n)")
    response = input()
    if response == "" or response.lower() == "y":
        download_file(hydrofabric_url, file_paths.conus_hydrofabric())
    else:
        print("Exiting...")
        exit()

if not file_paths.model_attributes().is_file():
    # alert the user that the model attributes are missing
    print("Model attributes are missing. Would you like to download them now? (Y/n)")
    response = input()
    if response == "" or response.lower() == "y":
        download_file(model_attributes_url, file_paths.model_attributes())
    else:
        print("Exiting...")
        exit()

if not file_paths.config_file.is_file():
    # prompt the user to set the working directory
    print(
        "Output directory is not set. Would you like to set it now? Defaults to ~/ngiab_preprocess_output/ (y/N)"
    )
    response = input()
    if response.lower() == "y":
        response = input("Enter the path to the working directory: ")
    if response == "" or response.lower() == "n":
        response = "~/ngiab_preprocess_output/"
    file_paths.set_working_dir(response)


with open("app.log", "w") as f:
    f.write("")
    f.write("Starting Application!\n")

logging.getLogger("werkzeug").setLevel(logging.WARNING)

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
    webbrowser.open("http://localhost:5000")


if __name__ == "__main__":

    if file_paths.dev_file().is_file():
        with open("app.log", "a") as f:
            f.write("Running in debug mode\n")
        app.run(debug=True, host="0.0.0.0", port=5000)
    else:
        Timer(1, open_browser).start()
        with open("app.log", "a") as f:
            f.write("Running in production mode\n")
        app.run(host="0.0.0.0", port=5000)
