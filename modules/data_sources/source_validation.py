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
from rich.console import Console
from rich.prompt import Prompt
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, SpinnerColumn, TimeRemainingColumn, DownloadColumn, TransferSpeedColumn
import threading
import psutil

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

console = Console()


def decompress_gzip_tar(file_path, output_dir):
    # use rich to display "decompressing" message with a progress bar that just counts down from 30s
    # actually measuring this is hard and it usually takes ~20s to decompress
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),        
    )
    task = progress.add_task("Decompressing", total=1)
    progress.start()
    with gzip.open(file_path, "rb") as f_in:
        with tarfile.open(fileobj=f_in) as tar:
            # Extract all contents
            for member in tar:
                tar.extract(member, path=output_dir)
                # Update the progress bar
    progress.update(task, completed=1)
    progress.stop()


def download_chunk(url, start, end, index, save_path):
    headers = {"Range": f"bytes={start}-{end}"}
    response = requests.get(url, headers=headers, stream=True)
    chunk_path = f"{save_path}.part{index}"
    # store the response in memory rather than streaming to disk
    # OSX has a limit of 256 open files so this is a workaround
    response_bytes = bytes()
    for chunk in response.iter_content(chunk_size=8 * 1024):
        response_bytes += chunk
    with open(chunk_path, "wb") as f_out:
        f_out.write(response_bytes)
    return chunk_path

def download_progress_estimate(progress, task, total_size):
    network_bytes_start = psutil.net_io_counters().bytes_recv
    # make a new progress bar that will be updated by a separate thread
    progress.start()
    interval = 0.5
    while not progress.finished:
        current_downloaded = psutil.net_io_counters().bytes_recv
        total_downloaded = current_downloaded - network_bytes_start
        progress.update(task, completed=total_downloaded)
        sleep(interval)
        if total_downloaded >= total_size or progress.finished:
            break
    progress.stop()


def download_file(url, save_path, num_threads=150):
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))

    response = requests.head(url)
    total_size = int(response.headers.get("content-length", 0))
    chunk_size = total_size // num_threads

    progress = Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TextColumn(" Elapsed Time:"),
            TimeElapsedColumn(),
            TextColumn(" Remaining Time:"),
            TimeRemainingColumn(),
        )
    task = progress.add_task("Downloading", total=total_size)

    download_progress_thread = threading.Thread(target=download_progress_estimate, args=(progress, task ,total_size))
    download_progress_thread.start()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i in range(num_threads):
            start = i * chunk_size
            end = start + chunk_size - 1 if i < num_threads - 1 else total_size - 1
            futures.append(executor.submit(download_chunk, url, start, end, i, save_path))

        chunk_paths = [
            future.result() for future in futures
        ]

    with open(save_path, "wb") as f_out:
        for chunk_path in chunk_paths:
            with open(chunk_path, "rb") as f_in:
                f_out.write(f_in.read())
            os.remove(chunk_path)

    progress.update(task, completed=total_size)
    download_progress_thread.join()


hydrofabric_url = "https://communityhydrofabric.s3.us-east-1.amazonaws.com/hydrofabrics/community/conus_nextgen.tar.gz"


def get_headers():
    # for versioning
    # Useful Headers: { 'Last-Modified': 'Wed, 20 Nov 2024 18:45:59 GMT', 'ETag': '"cc1452838886a7ab3065a61073fa991b-207"'}
    try:
        response = requests.head(hydrofabric_url)
    except requests.exceptions.ConnectionError:
        return 500, {}
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
        response = Prompt.ask(
            "Hydrofabric is missing. Would you like to download it now?",
            default="y",
            choices=["y", "n"],
        )
        if response == "y":
            download_and_update_hf()
        else:
            console.print("Exiting...", style="bold red")
            exit()

    if file_paths.no_update_hf.exists():
        # skip the updates
        return

    if not file_paths.hydrofabric_download_log.is_file():
        response = Prompt.ask(
            "Hydrofabric version information unavailable, Would you like to fetch the updated version?",
            default="y",
            choices=["y", "n"],
        )
        if response == "y":
            download_and_update_hf()
        else:
            console.print("Continuing... ", style="bold yellow")
            console.print(
                f"To disable this warning, create an empty file called {file_paths.no_update_hf.resolve()}",
                style="bold yellow",
            )
            sleep(2)
            return

    with open(file_paths.hydrofabric_download_log, "r") as f:
        content = f.read()
        headers = json.loads(content)

    status, latest_headers = get_headers()

    if status != 200:
        console.print(
            "Unable to contact servers, proceeding without updating hydrofabric", style="bold red"
        )
        sleep(2)

    if headers.get("ETag", "") != latest_headers.get("ETag", ""):
        console.print("Local and remote Hydrofabric Differ", style="bold yellow")
        console.print(
            f"Local last updated at {headers.get('Last-Modified', 'NA')}, remote last updated at {latest_headers.get('Last-Modified', 'NA')}",
            style="bold yellow",
        )
        response = Prompt.ask(
            "Would you like to fetch the updated version?",
            default="y",
            choices=["y", "n"],
        )
        if response == "y":
            download_and_update_hf()
        else:
            console.print("Continuing... ", style="bold yellow")
            console.print(
                f"To disable this warning, create an empty file called {file_paths.no_update_hf.resolve()}",
                style="bold yellow",
            )
            sleep(2)
            return


def validate_output_dir():
    if not file_paths.config_file.is_file():
        response = Prompt.ask(
            "Output directory is not set. Would you like to use the default? ~/ngiab_preprocess_output/",
            default="y",
            choices=["y", "n"],
        )
        if response.lower() == "n":
            response = Prompt.ask("Enter the path to the working directory")
        if response == "" or response.lower() == "y":
            response = "~/ngiab_preprocess_output/"
        file_paths.set_working_dir(response)


def validate_all():
    validate_hydrofabric()
    validate_output_dir()


if __name__ == "__main__":
    validate_all()
