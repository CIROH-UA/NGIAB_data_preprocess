# NGIAB Data Preprocess

This repository contains tools for preparing data to run a [next gen](https://github.com/NOAA-OWP/ngen) simulation using [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra). The tools allow you to select a catchment of interest on an interactive map, choose a date range, and prepare the data with just a few clicks!

![map screenshot](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/map_app/static/resources/screenshot.png)

## Table of Contents

1. [What does this tool do?](#what-does-this-tool-do)
2. [Requirements](#requirements)
3. [Installation and Running](#installation-and-running)
4. [Development Installation](#development-installation)
5. [Usage](#usage)
6. [CLI Documentation](#cli-documentation)
   - [Arguments](#arguments)
   - [Examples](#examples)
   - [File Formats](#file-formats)
   - [Output](#output)

## What does this tool do?

This tool prepares data to run a next gen simulation by creating a run package that can be used with NGIAB. It picks default data sources, the [v20.1 hydrofabric](https://www.lynker-spatial.com/data?path=hydrofabric%2Fv20.1%2F) and [nwm retrospective v3 forcing](https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/index.html#CONUS/zarr/forcing/) data.

## Requirements

* This tool is officially supported on macOS or Ubuntu (tested on 22.04 & 24.04). To use it on Windows, please install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install).
* GDAL needs to be installed.
* The 'ogr2ogr' command needs to work in your terminal.
`sudo apt install gdal-bin` will install gdal and ogr2ogr on ubuntu / wsl

## Installation and Running

```bash
# optional but encouraged: create a virtual environment
python3 -m venv env
source env/bin/activate
# installing and running the tool
pip install ngiab_data_preprocess[plot] # [plot] needed to install the evaluation and plotting module
python -m map_app
# CLI instructions at the bottom of the README
```

The first time you run this command, it will download the hydrofabric and model parameter files from Lynker Spatial. If you already have them, place `conus.gpkg` and `model_attributes.parquet` into `modules/data_sources/`.

## Development Installation

<details>
  <summary>Click to expand installation steps</summary>

To install and run the tool, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/CIROH-UA/NGIAB_data_preprocess
   cd NGIAB_data_preprocess
   ```
2. Create a virtual environment and activate it:
   ```bash
   python3 -m venv env
   source env/bin/activate
   ```
3. Install the tool:
   ```bash
   pip install -e .
   ```
4. Run the map app:
   ```bash
   python -m map_app
   ```
</details>

## Usage

Running the command `python -m map_app` will open the app in a new browser tab. Alternatively, you can manually open it by going to [http://localhost:5000](http://localhost:5000) with the app running.

To use the tool:
1. Select the catchment you're interested in on the map.
2. Pick the time period you want to simulate.
3. Click the following buttons in order:
    1) Create subset gpkg
    2) Create Forcing from Zarrs
    3) Create Realization

Once all the steps are finished, you can run NGIAB on the folder shown underneath the subset button.

**Note:** When using the tool, the output will be stored in the `./output/<your-input-feature>/` folder. There is no overwrite protection on the folders.

# CLI Documentation

<details>
<summary>Click to expand CLI documentation</summary>

## Arguments

- `-h`, `--help`: Show the help message and exit.
- `-i INPUT_FEATURE`, `--input_feature INPUT_FEATURE`: ID of feature to subset. Providing a prefix will automatically convert to catid, e.g., cat-5173 or gage-01646500 or wb-1234.
- `-l`, `--latlon`: Use latitude and longitude instead of catid. Expects comma-separated values via the CLI, e.g., `python -m ngiab_data_cli -i 54.33,-69.4 -l -s`.
- `-g`, `--gage`: Use gage ID instead of catid. Expects a single gage ID via the CLI, e.g., `python -m ngiab_data_cli -i 01646500 -g -s`.
- `-s`, `--subset`: Subset the hydrofabric to the given feature.
- `-f`, `--forcings`: Generate forcings for the given feature.
- `-r`, `--realization`: Create a realization for the given feature.
- `--start_date START_DATE`, `--start START_DATE`: Start date for forcings/realization (format YYYY-MM-DD).
- `--end_date END_DATE`, `--end END_DATE`: End date for forcings/realization (format YYYY-MM-DD).
- `-o OUTPUT_NAME`, `--output_name OUTPUT_NAME`: Name of the output folder.
- `-D`, `--debug`: Enable debug logging.
- `--run`: Automatically run Next Gen against the output folder.
- `--validate`: Run every missing step required to run ngiab.
- `--eval`: Evaluate performance of the model after running and plot streamflow at USGS gages.
- `-a`, `--all`: Run all operations: subset, forcings, realization, run Next Gen, and evaluate.

## Usage Notes

- If your input has a prefix of `gage-`, you do not need to pass `-g`.
- The `-l`, `-g`, `-s`, `-f`, `-r` flags can be combined like normal CLI flags. For example, to subset, generate forcings, and create a realization, you can use `-sfr` or `-s -f -r`.
- When using the `--all` flag, it automatically sets `subset`, `forcings`, `realization`, `run`, and `eval` to `True`.
- Using the `--run` flag automatically sets the `--validate` flag.

## Examples

1. Subset hydrofabric using catchment ID:
   ```bash
   python -m ngiab_data_cli -i cat-7080 -s
   ```

2. Generate forcings using a single catchment ID:
   ```bash
   python -m ngiab_data_cli -i cat-5173 -f --start 2022-01-01 --end 2022-02-28
   ```

3. Create realization using a lat/lon pair and output to a named folder:
   ```bash
   python -m ngiab_data_cli -i 54.33,-69.4 -l -r --start 2022-01-01 --end 2022-02-28 -o custom_output
   ```

4. Perform all operations using a lat/lon pair:
   ```bash
   python -m ngiab_data_cli -i 54.33,-69.4 -l -s -f -r --start 2022-01-01 --end 2022-02-28
   ```

5. Subset hydrofabric using gage ID:
   ```bash
   python -m ngiab_data_cli -i 10154200 -g -s
   # or
   python -m ngiab_data_cli -i gage-10154200 -s
   ```

6. Generate forcings using a single gage ID:
   ```bash
   python -m ngiab_data_cli -i 01646500 -g -f --start 2022-01-01 --end 2022-02-28
   ```

7. Run all operations, including Next Gen and evaluation/plotting:
   ```bash
   python -m ngiab_data_cli -i cat-5173 -a --start 2022-01-01 --end 2022-02-28
   ```

## Output

The script creates an output folder named after the first catchment ID in the input file, the provided output name, or derived from the first lat/lon pair or gage ID. This folder will contain the results of the subsetting, forcings generation, realization creation, Next Gen run (if applicable), and evaluation (if applicable) operations.

</details>