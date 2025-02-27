import gzip
import os
import tarfile
import warnings
import json
import requests
from data_processing.file_paths import file_paths
from tqdm import TqdmExperimentalWarning
from time import sleep
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from rich.console import Console
from rich.prompt import Prompt
from rich.progress import Progress, TextColumn, TimeElapsedColumn, SpinnerColumn
import psutil

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

console = Console()
S3_BUCKET = "communityhydrofabric"
S3_KEY = "hydrofabrics/community/conus_nextgen.tar.gz"
S3_REGION = "us-east-1"
hydrofabric_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{S3_KEY}"

def decompress_gzip_tar(file_path, output_dir):
    # use rich to display "decompressing" message with a progress bar that just counts down from 30s
    # actually measuring this is hard and it usually takes ~20s to decompress
    console.print("Decompressing Hydrofabric...", style="bold green")
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


def download_from_s3(save_path, bucket=S3_BUCKET, key=S3_KEY, region=S3_REGION):
    """Download file from S3 with optimal multipart configuration"""
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))

    # Check if file already exists
    if os.path.exists(save_path):
        console.print(f"File already exists: {save_path}", style="bold yellow")
        os.remove(save_path)

    # Initialize S3 client
    s3_client = boto3.client(
        "s3", aws_access_key_id="", aws_secret_access_key="", region_name=region
    )
    # Disable request signing for public buckets
    s3_client._request_signer.sign = lambda *args, **kwargs: None

    # Get object size
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        total_size = int(response.get("ContentLength", 0))
    except ClientError as e:
        console.print(f"Error getting object info: {e}", style="bold red")
        return False

    # Configure transfer settings for maximum speed
    # Use more CPU cores for parallel processing
    cpu_count = os.cpu_count() or 8
    max_threads = cpu_count * 4

    # Optimize chunk size based on file size and available memory
    memory = psutil.virtual_memory()
    available_mem_mb = memory.available / (1024 * 1024)

    # Calculate optimal chunk size (min 8MB, max 100MB)
    # Larger files get larger chunks for better throughput
    optimal_chunk_mb = min(max(8, total_size / (50 * 1024 * 1024)), 100)
    # Ensure we don't use too much memory
    optimal_chunk_mb = min(optimal_chunk_mb, available_mem_mb / (max_threads * 2))

    # Create transfer config
    config = TransferConfig(
        # multipart_threshold=8 * 1024 * 1024,  # 8MB
        max_concurrency=max_threads,
        multipart_chunksize=int(optimal_chunk_mb * 1024 * 1024),
        use_threads=True,
    )

    console.print(f"Downloading {key} to {save_path}...", style="bold green")
    console.print(
        f"The file downloads faster with no progress indicator, this should take around 30s",
        style="bold yellow",
    )
    console.print(
        f"Please use network monitoring on your computer if you wish to track the download",
        style="green",
    )

    try:
        # Download file using optimized transfer config
        s3_client.download_file(Bucket=bucket, Key=key, Filename=save_path, Config=config)
        return True
    except Exception as e:
        console.print(f"Error downloading file: {e}", style="bold red")
        return False


def get_headers():
    # for versioning
    # Useful Headers: { 'Last-Modified': 'Wed, 20 Nov 2024 18:45:59 GMT', 'ETag': '"cc1452838886a7ab3065a61073fa991b-207"'}
    try:
        response = requests.head(hydrofabric_url)
    except requests.exceptions.ConnectionError:
        return 500, {}
    return response.status_code, response.headers


def download_and_update_hf():
    download_from_s3(
        file_paths.conus_hydrofabric.with_suffix(".tar.gz"),
        bucket="communityhydrofabric",
        key="hydrofabrics/community/conus_nextgen.tar.gz",
    )
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
