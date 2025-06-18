# NGIAB Data Preprocess

This repository contains tools for preparing data to run a [next gen](https://github.com/NOAA-OWP/ngen) simulation using [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra). The tools allow you to select a catchment of interest on an interactive map, choose a date range, and prepare the data with just a few clicks!

![map screenshot](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/map_app/static/resources/screenshot.jpg)

## Table of Contents

1. [What does this tool do?](#what-does-this-tool-do)
2. [What does it not do?](#what-does-it-not-do)
   - [Evaluation](#evaluation)
   - [Visualisation](#visualisation)
3. [Requirements](#requirements)
4. [Installation and Running](#installation-and-running)
   - [Running without install](#running-without-install)
5. [For legacy pip installation](#for-legacy-pip-installation)
6. [Development Installation](#development-installation)
7. [Usage](#usage)
8. [CLI Documentation](#cli-documentation)
   - [Arguments](#arguments)
   - [Usage Notes](#usage-notes)
   - [Examples](#examples)

## What does this tool do?

This tool prepares data to run a next gen simulation by creating a run package that can be used with NGIAB.  
It uses geometry and model attributes from the [v2.2 hydrofabric](https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v2.2/conus/conus_nextgen.gpkg) more information on [all data sources here](https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v2.2/hfv2.2-data_model.html).  
The raw forcing data is [nwm retrospective v3 forcing](https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/index.html#CONUS/zarr/forcing/) data or the [AORC 1km gridded data](https://noaa-nws-aorc-v1-1-1km.s3.amazonaws.com/index.html) depending on user input

1. **Subset** (delineate) everything upstream of your point of interest (catchment, gage, flowpath etc). Outputs as a geopackage.  
2. **Calculates** Forcings as a weighted mean of the gridded AORC forcings. Weights are calculated using [exact extract](https://isciences.github.io/exactextract/) and computed with numpy. 
3. Creates **configuration files** needed to run nextgen.
    -  realization.json  - ngen model configuration
    -  troute.yaml - routing configuration.
    -  **per catchment** model configuration
4. Optionally Runs a non-interactive [Next gen in a box](https://github.com/CIROH-UA/NGIAB-CloudInfra).

## What does it not do?

### Evaluation
For automatic evaluation using [Teehr](https://github.com/RTIInternational/teehr), please run [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra) interactively using the `guide.sh` script.

### Visualisation
For automatic interactive visualisation, please run [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra) interactively using the `guide.sh` script

## Requirements

* This tool is officially supported on macOS or Ubuntu (tested on 22.04 & 24.04). To use it on Windows, please install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install).

## Installation and Running
It is highly recommended to use [Astral UV](https://docs.astral.sh/uv/) to install and run this tool. It works similarly to pip and conda, and I would also recommend you use it for other python projects as it is so useful.

```bash
# Install UV
curl -LsSf https://astral.sh/uv/install.sh | sh
# It can be installed via pip if that fails
# pip install uv

# Create a virtual environment in the current directory
uv venv

# Install the tool in the virtual environment 
uv pip install ngiab_data_preprocess

# To run the cli
uv run cli --help

# To run the map 
uv run map_app
```

UV automatically detects any virtual environments in the current directory and will use them when you use `uv run`.

### Running without install
This package supports pipx and uvx which means you can run the tool without installing it. No virtual environment needed, just UV.
```bash
# run this from anywhere 
uvx --from ngiab_data_preprocess cli --help
# for the map
uvx --from ngiab_data_preprocess map_app
```

## For legacy pip installation
<details>
  <summary>Click here to expand</summary>

```bash
# If you're installing this on jupyterhub / 2i2c you HAVE TO DEACTIVATE THE CONDA ENV
(notebook) jovyan@jupyter-user:~$ conda deactivate
jovyan@jupyter-user:~$
# The interactive map won't work on 2i2c
```    

```bash
# This tool is likely to not work without a virtual environment
python3 -m venv .venv
source .venv/bin/activate
# installing and running the tool
pip install 'ngiab_data_preprocess'
python -m map_app
# CLI instructions at the bottom of the README
```
</details>

## Development Installation

<details>
  <summary>Click to expand installation steps</summary>

To install and run the tool, follow these steps:

1. Clone the repository:
   ```bash
   git clone https://github.com/CIROH-UA/NGIAB_data_preprocess
   cd NGIAB_data_preprocess
   ```
2. Create a virtual environment:
   ```bash
   uv venv
   ```
3. Install the tool:
   ```bash
   uv pip install -e .
   ```
4. Run the map app:
   ```bash
   uv run map_app
   ```
</details>

## Usage

Running the command `uv run map_app` will open the app in a new browser tab.

To use the tool:
1. Select the catchment you're interested in on the map.
2. Pick the time period you want to simulate.
3. Click the following buttons in order:
    1) Create subset gpkg
    2) Create Forcing from Zarrs
    3) Create Realization

Once all the steps are finished, you can run NGIAB on the folder shown underneath the subset button.

**Note:** When using the tool, the default output will be stored in the `~/ngiab_preprocess_output/<your-input-feature>/` folder. There is no overwrite protection on the folders.

# CLI Documentation

## Arguments

- `-h`, `--help`: Show the help message and exit.
- `-i INPUT_FEATURE`, `--input_feature INPUT_FEATURE`: ID of feature to subset. Providing a prefix will automatically convert to catid, e.g., cat-5173 or gage-01646500 or wb-1234.
- `--vpu VPU_ID` : The id of the vpu to subset e.g 01. 10 = 10L + 10U and 03 = 03N + 03S + 03W. `--help` will display all the options.
- `-l`, `--latlon`: Use latitude and longitude instead of catid. Expects comma-separated values via the CLI, e.g., `python -m ngiab_data_cli -i 54.33,-69.4 -l -s`.
- `-g`, `--gage`: Use gage ID instead of catid. Expects a single gage ID via the CLI, e.g., `python -m ngiab_data_cli -i 01646500 -g -s`.
- `-s`, `--subset`: Subset the hydrofabric to the given feature.
- `-f`, `--forcings`: Generate forcings for the given feature.
- `-r`, `--realization`: Create a realization for the given feature.
- `--start_date START_DATE`, `--start START_DATE`: Start date for forcings/realization (format YYYY-MM-DD).
- `--end_date END_DATE`, `--end END_DATE`: End date for forcings/realization (format YYYY-MM-DD).
- `-o OUTPUT_NAME`, `--output_name OUTPUT_NAME`: Name of the output folder.
- `--source` : The datasource you want to use, either `nwm` for retrospective v3 or `aorc`. Default is `nwm`
- `-D`, `--debug`: Enable debug logging.
- `--run`: Automatically run Next Gen against the output folder.
- `--validate`: Run every missing step required to run ngiab.
- `-a`, `--all`: Run all operations: subset, forcings, realization, run Next Gen

## Usage Notes
- If your input has a prefix of `gage-`, you do not need to pass `-g`.
- The `-l`, `-g`, `-s`, `-f`, `-r` flags can be combined like normal CLI flags. For example, to subset, generate forcings, and create a realization, you can use `-sfr` or `-s -f -r`.
- When using the `--all` flag, it automatically sets `subset`, `forcings`, `realization`, and `run` to `True`.
- Using the `--run` flag automatically sets the `--validate` flag.

## Examples

0. Prepare everything for a nextgen run at a given gage:
   ```bash
   python -m ngiab_data_cli -i gage-10154200 -sfr --start 2022-01-01 --end 2022-02-28 
   #         add --run or replace -sfr with --all to run nextgen in a box too
   # to name the folder, add -o folder_name
   ```

1. Subset hydrofabric using catchment ID or VPU:
   ```bash
   python -m ngiab_data_cli -i cat-7080 -s
   python -m ngiab_data_cli --vpu 01 -s
   ```

2. Generate forcings using a single catchment ID:
   ```bash
   python -m ngiab_data_cli -i cat-5173 -f --start 2022-01-01 --end 2022-02-28
   ```

3. Create realization using a lat/lon pair and output to a named folder:
   ```bash
   python -m ngiab_data_cli -i 33.22,-87.54 -l -r --start 2022-01-01 --end 2022-02-28 -o custom_output
   ```

4. Perform all operations using a lat/lon pair:
   ```bash
   python -m ngiab_data_cli -i 33.22,-87.54 -l -s -f -r --start 2022-01-01 --end 2022-02-28
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



