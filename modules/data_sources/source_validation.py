import os
import gzip
import tarfile
import json
import warnings
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import psutil
from time import sleep
from rich.console import Console
from rich.prompt import Prompt
from rich.progress import Progress, TextColumn, TimeElapsedColumn, SpinnerColumn,
from tqdm import TqdmExperimentalWarning
from data_processing.file_paths import file_paths

warnings.filterwarnings("ignore", category=TqdmExperimentalWarning)

console = Console()

# S3 bucket and object details
S3_BUCKET = "communityhydrofabric"
S3_KEY = "hydrofabrics/community/conus_nextgen.tar.gz"

def decompress_gzip_tar(file_path, output_dir):
    """Decompress a gzipped tarfile with progress indicator"""
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),        
    )
    task = progress.add_task("Decompressing", total=1)
    progress.start()

    try:
        with gzip.open(file_path, "rb") as f_in:
            with tarfile.open(fileobj=f_in) as tar:
                for member in tar:
                    tar.extract(member, path=output_dir)
        progress.update(task, completed=1)
    finally:
        progress.stop()


def download_progress_monitor(progress, task, file_path):
    """Monitor download progress based on file size"""
    progress.start()
    interval = 1
    try:
        while not progress.finished:
            sleep(interval)
            if os.path.exists(file_path):
                break

    finally:
        progress.stop()


def download_from_s3(save_path, bucket=S3_BUCKET, key=S3_KEY, region="us-east-1"):
    """Download file from S3 with optimal multipart configuration"""
    if not os.path.exists(os.path.dirname(save_path)):
        os.makedirs(os.path.dirname(save_path))

    # Check if file already exists
    if os.path.exists(save_path):
        console.print(f"File already exists: {save_path}", style="bold yellow")
        os.remove(save_path)

    # Initialize S3 client
    s3_client = boto3.client("s3", region_name=region)

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


def get_s3_headers(bucket=S3_BUCKET, key=S3_KEY, region="us-east-1"):
    """Get metadata headers for the S3 object"""
    try:
        s3_client = boto3.client("s3", region_name=region)
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return 200, response
    except ClientError as e:
        console.print(f"Error getting object headers: {e}", style="bold red")
        return 500, {}


def download_and_update_hf():
    """Download and update hydrofabric from S3"""
    save_path = file_paths.conus_hydrofabric.with_suffix(".tar.gz")
    success = download_from_s3(save_path)

    if success:
        status, headers = get_s3_headers()

        if status == 200:
            # Write headers to a file
            with open(file_paths.hydrofabric_download_log, "w") as f:
                json.dump(dict(headers), f)

        decompress_gzip_tar(save_path, file_paths.conus_hydrofabric.parent)

def validate_hydrofabric():
    """Validate hydrofabric file exists and is up to date"""
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
        # Skip the updates
        return

    if not file_paths.hydrofabric_download_log.is_file():
        response = Prompt.ask(
            "Hydrofabric version information unavailable. Would you like to fetch the updated version?",
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

    # Check if update needed
    try:
        with open(file_paths.hydrofabric_download_log, "r") as f:
            content = f.read()
            local_headers = json.loads(content)

        status, remote_headers = get_s3_headers()

        if status != 200:
            console.print(
                "Unable to contact S3, proceeding without updating hydrofabric", style="bold red"
            )
            sleep(2)
            return

        local_etag = local_headers.get("ETag", "").strip('"')
        remote_etag = remote_headers.get("ETag", "").strip('"')

        if local_etag != remote_etag:
            console.print("Local and remote Hydrofabric differ", style="bold yellow")
            console.print(
                f"Local last modified: {local_headers.get('LastModified', 'N/A')}, "
                f"Remote last modified: {remote_headers.get('LastModified', 'N/A')}",
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
    except Exception as e:
        console.print(f"Error checking for updates: {e}", style="bold red")

def validate_output_dir():
    """Validate output directory exists"""
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
    """Run all validation checks"""
    validate_hydrofabric()
    validate_output_dir()

if __name__ == "__main__":
    # For testing just the download
    download_from_s3(file_paths.conus_hydrofabric.with_suffix(".tar.gz"))

    # Or run the full validation process
    # validate_all()
