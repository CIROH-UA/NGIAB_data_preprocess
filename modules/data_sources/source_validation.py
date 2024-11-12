import gzip
import os
import tarfile
import warnings

import requests
from data_processing.file_paths import file_paths
from tqdm import TqdmExperimentalWarning
from tqdm.rich import tqdm

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


hydrofabric_url = "https://communityhydrofabric.s3.us-east-1.amazonaws.com/conus_nextgen.gpkg"


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
    validate_output_dir()
