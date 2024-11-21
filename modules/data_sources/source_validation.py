import gzip
import os
import tarfile
import warnings
import json
from concurrent.futures import ThreadPoolExecutor
import requests
from data_processing.file_paths import file_paths
from tqdm import TqdmExperimentalWarning
from tqdm.rich import tqdm
from time import sleep

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)


def decompress_gzip_tar(file_path, output_dir):

    with tqdm(
        total=1,
        desc="Decompressing, this should take less than 30 seconds. The progress bar won't update until the end",
        bar_format="[elapsed: {elapsed}]",
    ) as pbar:
        with gzip.open(file_path, "rb") as f_in:
            with tarfile.open(fileobj=f_in) as tar:
                # Extract all contents
                for member in tar:
                    tar.extract(member, path=output_dir)
                    # Update the progress bar
                    pbar.update(member.size)


def download_chunk(url, start, end, index, save_path):
    headers = {"Range": f"bytes={start}-{end}"}
    response = requests.get(url, headers=headers, stream=True)
    chunk_path = f"{save_path}.part{index}"
    with open(chunk_path, "wb") as f_out:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f_out.write(chunk)
    return chunk_path


def download_file(url, save_path, num_threads=150):
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))

    response = requests.head(url)
    total_size = int(response.headers.get("content-length", 0))
    chunk_size = total_size // num_threads

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start = i * chunk_size
            end = start + chunk_size - 1 if i < num_threads - 1 else total_size - 1
            futures.append(executor.submit(download_chunk, url, start, end, i, save_path))

        chunk_paths = [
            future.result() for future in tqdm(futures, desc="Downloading", total=num_threads)
        ]

    with open(save_path, "wb") as f_out:
        for chunk_path in chunk_paths:
            with open(chunk_path, "rb") as f_in:
                f_out.write(f_in.read())
            os.remove(chunk_path)


hydrofabric_url = "https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/conus_nextgen.tar.gz"


def get_headers():
    # for versioning
    # Useful Headers: { 'Last-Modified': 'Wed, 20 Nov 2024 18:45:59 GMT', 'ETag': '"cc1452838886a7ab3065a61073fa991b-207"'}
    response = requests.head(hydrofabric_url)
    return response.status_code, response.headers


def download_and_update_hf():
    download_file(hydrofabric_url, file_paths.conus_hydrofabric.with_suffix(".tar.gz"))
    status, headers = get_headers()

    if status == 200:
        # write headers to a file
        with open(file_paths.hydrofabric_download_log, "w") as f:
            json.dump(dict(headers), f)

    decompress_gzip_tar(
        file_paths.conus_hydrofabric.with_suffix(".tar.gz"),
        file_paths.conus_hydrofabric.parent,
    )


def validate_hydrofabric():
    if not file_paths.conus_hydrofabric.is_file():
        print("Hydrofabric is missing. Would you like to download it now? (Y/n)")
        response = input()
        if response == "" or response.lower() == "y":
            download_and_update_hf()
        else:
            print("Exiting...")
            exit()

    if file_paths.no_update_hf.exists():
        # skip the updates
        return

    if not file_paths.hydrofabric_download_log.is_file():
        print(
            "Hydrofabric version information unavailable, Would you like to fetch the updated version?  (Y/n)"
        )
        response = input()
        if response == "" or response.lower() == "y":
            download_and_update_hf()
        else:
            print("Continuing... ")
            print(
                f"To disable this warning, create an empty file called {file_paths.no_update_hf.resolve()}"
            )
            sleep(2)
            return

    with open(file_paths.hydrofabric_download_log, "r") as f:
        content = f.read()
        headers = json.loads(content)

    status, latest_headers = get_headers()

    if status != 200:
        print("Unable to contact servers, proceeding without updating hydrofabric")
        sleep("2")

    if headers.get("ETag", "") != latest_headers.get("ETag", ""):
        print("Local and remote Hydrofabric Differ")
        print(
            f"Local last updated at {headers.get('Last-Modified', 'NA')}, remote last updated at {latest_headers.get('Last-Modified', 'NA')}"
        )
        print("Would you like to fetch the updated version?  (Y/n)")
        response = input()
        if response == "" or response.lower() == "y":
            download_and_update_hf()
        else:
            print("Continuing... ")
            print(
                f"To disable this warning, create an empty file called {file_paths.no_update_hf.resolve()}"
            )
            sleep(2)
            return


def validate_output_dir():
    if not file_paths.config_file.is_file():
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


if __name__ == "__main__":
    validate_all()
