import tarfile
import gzip
from tqdm.rich import tqdm
from tqdm import TqdmExperimentalWarning
import os
import requests
from data_processing.file_paths import file_paths
import warnings

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)


def decompress_gzip_tar(file_path, output_dir):
    # Get the total size of the compressed file
    total_size = os.path.getsize(file_path)

    with gzip.open(file_path, "rb") as f_in:
        # Create a tqdm progress bar
        with tqdm(total=total_size, unit="MB", unit_scale=True, desc=f"Decompressing") as pbar:
            # Open the tar archive
            with tarfile.open(fileobj=f_in) as tar:
                # Extract all contents
                for member in tar:
                    tar.extract(member, path=output_dir)
                    # Update the progress bar
                    pbar.update(member.size)


def download_file(url, save_path):
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get("content-length", 0))
    bytes_downloaded = 0
    chunk_size = 1048576
    with open(save_path, "wb") as f:
        for data in tqdm(
            response.iter_content(chunk_size=chunk_size),
            total=total_size / chunk_size,
            unit="B",
            unit_scale=True,
            desc=f"Downloading",
        ):
            bytes_downloaded += len(data)
            f.write(data)


hydrofabric_url = "https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v20.1/conus.gpkg"
model_attributes_url = (
    "https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v20.1/model_attributes.parquet"
)


def validate_hydrofabric():
    if not file_paths.conus_hydrofabric.is_file():
        # alert the user that the hydrofabric is missing
        print("Hydrofabric is missing. Would you like to download it now? (Y/n)")
        response = input()
        if response == "" or response.lower() == "y":
            download_file(hydrofabric_url, file_paths.conus_hydrofabric)
        else:
            print("Exiting...")
            exit()


def validate_model_attributes():
    if not file_paths.model_attributes.is_file():
        # alert the user that the model attributes are missing
        print("Model attributes are missing. Would you like to download them now? (Y/n)")
        response = input()
        if response == "" or response.lower() == "y":
            download_file(model_attributes_url, file_paths.model_attributes)
        else:
            print("Exiting...")
            exit()


def validate_output_dir():
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


def validate_all():
    validate_hydrofabric()
    validate_model_attributes()
    validate_output_dir()
