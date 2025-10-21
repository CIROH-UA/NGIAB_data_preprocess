# NGIAB Data Preprocess

This repository contains tools for preparing data to run a [NextGen](https://github.com/NOAA-OWP/ngen)-based simulation using [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra). The tools allow you to select a catchment of interest on an interactive map, choose a date range, and prepare the data with just a few clicks!

![map screenshot](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/map_app/static/resources/screenshot.jpg)

| | |
| --- | --- |
| ![CIROH Logo](./ciroh-bgsafe.png) | Funding for this project was provided by the National Oceanic & Atmospheric Administration (NOAA), awarded to the Cooperative Institute for Research to Operations in Hydrology (CIROH) through the NOAA Cooperative Agreement with The University of Alabama (NA22NWS4320003). |

## Table of Contents

1. [What does this tool do?](#what-does-this-tool-do)
2. [Limitations](#limitations)
   - [Custom realizations](#custom-realizations)
   - [Calibration](#calibration)
   - [Evaluation](#evaluation)
   - [Visualisation](#visualisation)
3. [Requirements](#requirements)
4. [Installation and running](#installation-and-running)
   - [Running without install](#running-without-install)
   - [For uv installation](#for-uv-installation)
   - [For legacy pip installation](#for-legacy-pip-installation)
   - [Development installation](#development-installation)
5. [Map interface documentation](#map-interface-documentation)
   - [Running the map interface app](#running-the-map-interface-app)
   - [Using the map interace](#using-the-map-interface)
6. [CLI documentation](#cli-documentation)
   - [Running the CLI](#running-the-cli)
   - [Arguments](#arguments)
   - [Usage notes](#usage-notes)
   - [Examples](#examples)
7. [Realization information](#realization-information)
   - [NOAH + CFE](#noah--cfe)

## What does this tool do?

This tool prepares data to run a NextGen-based simulation by creating a run package that can be used with NGIAB.  
It uses geometry and model attributes from the [v2.2 hydrofabric](https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v2.2/conus/conus_nextgen.gpkg) more information on [all data sources here](https://lynker-spatial.s3-us-west-2.amazonaws.com/hydrofabric/v2.2/hfv2.2-data_model.html).  
The raw forcing data is [nwm retrospective v3 forcing](https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/index.html#CONUS/zarr/forcing/) data or the [AORC 1km gridded data](https://noaa-nws-aorc-v1-1-1km.s3.amazonaws.com/index.html) depending on user input

1. **Subsets** (delineates) everything upstream of your point of interest (catchment, gage, flowpath etc) from the hydrofabric. This subset is output as a geopackage (.gpkg). 
2. Calculates **forcings** as a weighted mean of the gridded NWM or AORC forcings. Weights are calculated using [exact extract](https://isciences.github.io/exactextract/) and computed with numpy. 
3. Creates **configuration files** for a default NGIAB model run. 
    -  realization.json  - ngen model configuration
    -  troute.yaml - routing configuration.
    -  **per catchment** model configuration
4. Optionally performs a non-interactive [Docker-based NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra) run.

## Limitations
This tool cannot do the following:

### Custom realizations
This tool currently only outputs a single, default realization, which is described in "[Realization information](#realization-information)". Support for additional model configurations is planned, but not currently available.

### Calibration
If available, this repository will download [calibrated parameters](https://communityhydrofabric.s3.us-east-1.amazonaws.com/index.html#hydrofabrics/community/gage_parameters/) from the [Community Hydrofabric](https://github.com/CIROH-UA/community_hf_patcher) AWS S3 bucket.
However, many gages and catchments will not have such parameters available. In these cases, Data Preprocess will output realizations with default values.

For automatic calibration, please see [ngiab-cal](https://github.com/CIROH-UA/ngiab-cal), which is under active development.

### Evaluation
For automatic evaluation using [TEEHR](https://github.com/RTIInternational/teehr), please run [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra) interactively using the `guide.sh` script.

### Visualisation
For automatic interactive visualisation, please run [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra) interactively using the `guide.sh` script

# Requirements

This tool is **officially supported** on **macOS** and **Ubuntu** (tested on 22.04 & 24.04). To use it on Windows, please install [**WSL**](https://learn.microsoft.com/en-us/windows/wsl/install).

It is also **highly recommended** to use [Astral UV](https://docs.astral.sh/uv/) to install and run this tool. Installing the project via `pip` without the use of a virtual environment creates a **severe risk** of dependency conflicts.

# Installation and running

### Running without install
This package supports pipx and uvx, which means you can run the tool without installing it. No virtual environment needed, just UV.
```bash
# Run these from anywhere!
uvx --from ngiab-data-preprocess cli --help  # Running the CLI
uvx ngiab-prep --help                        # Alias for the CLI
uvx --from ngiab-data-preprocess map_app     # Running the map interface
```

### For uv installation

<details>
  <summary>Click here to expand</summary>

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

</details>

### For legacy pip installation

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

### Development installation

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

# Map interface documentation

## Running the map interface app

Running the `map_app` tool will open the app in a new browser tab.

Install-free: `uvx --from ngiab-data-preprocess map_app`  
Installed with uv: `uv run map_app`

## Using the map interface

1. Select the catchment you're interested in on the map.
2. Pick the time period you want to simulate.
3. Click the following buttons in order:
    1) Create subset gpkg
    2) Create Forcing from Zarrs
    3) Create Realization

Once all the steps are finished, you can run NGIAB on the folder shown underneath the subset button.

**Note:** When using the tool, the default output will be stored in the `~/ngiab_preprocess_output/<your-input-feature>/` folder. There is no overwrite protection on the folders.

# CLI documentation

## Running the CLI

Install-free: `uvx ngiab-prep`  
Installed with uv: `uv run cli`

## Arguments

- `-h`, `--help`: Show the help message and exit.
- `-i INPUT_FEATURE`, `--input_feature INPUT_FEATURE`: ID of feature to subset. Providing a prefix will automatically convert to catid, e.g., cat-5173 or gage-01646500 or wb-1234.
- `--vpu VPU_ID` : The id of the vpu to subset e.g 01. 10 = 10L + 10U and 03 = 03N + 03S + 03W. `--help` will display all the options.
- `-l`, `--latlon`: Use latitude and longitude instead of catid. Expects comma-separated values via the CLI, e.g., `python -m ngiab_data_cli -i 54.33,-69.4 -l -s`.
- `-g`, `--gage`: Use gage ID instead of catid. Expects a single gage ID via the CLI, e.g., `python -m ngiab_data_cli -i 01646500 -g -s`.
- `-s`, `--subset`: Subset the hydrofabric to the given feature.
- `-f`, `--forcings`: Generate forcings for the given feature.
- `-r`, `--realization`: Create a realization for the given feature.
- `--lstm`: Configures the data for the [python lstm](https://github.com/ciroh-ua/lstm/).
- `--lstm_rust`: Configures the data for the [rust lstm](https://github.com/ciroh-ua/rust-lstm-1025/).
- `--start_date START_DATE`, `--start START_DATE`: Start date for forcings/realization (format YYYY-MM-DD).
- `--end_date END_DATE`, `--end END_DATE`: End date for forcings/realization (format YYYY-MM-DD).
- `-o OUTPUT_NAME`, `--output_name OUTPUT_NAME`: Name of the output folder.
- `--source` : The datasource you want to use, either `nwm` for retrospective v3 or `aorc`. Default is `nwm`
- `-D`, `--debug`: Enable debug logging.
- `--run`: Automatically run [NGIAB's docker distribution](https://github.com/CIROH-UA/NGIAB-CloudInfra) against the output folder.
- `--validate`: Run every missing step required to run NGIAB.
- `-a`, `--all`: Run all operations. Equivalent to `-sfr` and `--run`.

## Usage notes
- If your input has a prefix of `gage-`, you do not need to pass `-g`.
- The `-l`, `-g`, `-s`, `-f`, `-r` flags can be combined like normal CLI flags. For example, to subset, generate forcings, and create a realization, you can use `-sfr` or `-s -f -r`.
- When using the `--all` flag, it automatically sets `subset`, `forcings`, `realization`, and `run` to `True`.
- Using the `--run` flag automatically sets the `--validate` flag.

## Examples

1. Prepare everything for an NGIAB run at a given gage:
   ```bash
   uvx ngiab-prep -i gage-10154200 -sfr --start 2022-01-01 --end 2022-02-28 
   #         add --run or replace -sfr with --all to run NGIAB, too
   # to name the folder, add -o folder_name
   ```

2. Subset the hydrofabric using a catchment ID or VPU:
   ```bash
   uvx ngiab-prep -i cat-7080 -s
   uvx ngiab-prep --vpu 01 -s
   ```

3. Generate forcings using a single catchment ID:
   ```bash
   uvx ngiab-prep -i cat-5173 -f --start 2022-01-01 --end 2022-02-28
   ```

4. Create realization using a latitude/longitude pair and output to a named folder:
   ```bash
   uvx ngiab-prep -i 33.22,-87.54 -l -r --start 2022-01-01 --end 2022-02-28 -o custom_output
   ```

5. Perform all operations using a latitude/longitude pair:
   ```bash
   uvx ngiab-prep -i 33.22,-87.54 -l -s -f -r --start 2022-01-01 --end 2022-02-28
   ```

6. Subset the hydrofabric using a gage ID:
   ```bash
   uvx ngiab-prep -i 10154200 -g -s
   # or
   uvx ngiab-prep -i gage-10154200 -s
   ```

7. Generate forcings using a single gage ID:
   ```bash
   uvx ngiab-prep -i 01646500 -g -f --start 2022-01-01 --end 2022-02-28
   ```

# Realization information

This tool currently offers three realizations.

## NOAH + CFE (Default)

[This realization](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/data_sources/cfe-nowpm-realization-template.json) is intended to be roughly comparable to earlier versions of the National Water Model.
- [NOAH-OWP-Modular](https://github.com/NOAA-OWP/NOAH-OWP-Modular): A refactoring of Noah-MP, a land-surface model. Used to model groundwater properties.
- [Conceptual Functional Equivalent (CFE)](https://github.com/NOAA-OWP/CFE): A simplified conceptual approximation of versions 1.2, 2.0, and 2.1 of the National Water Model. Used to model precipitation and evaporation.
- [SLoTH](https://github.com/NOAA-OWP/SLoTH): A module used to feed through unchanged values. In this default configuration, it simply forces certain soil moisture and ice fraction properties to zero.

## LSTM (Python)
[This realization](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/data_sources/lstm-realization-template.json) will run the [python lstm](https://github.com/ciroh-ua/lstm/). It's designed to work with ngiab using [these example weights](https://github.com/CIROH-UA/lstm/tree/example_weights/trained_neuralhydrology_models) generously contributed by [jmframe/lstm](https://github.com/jmframe/lstm)

## LSTM (Rust)
[This realization](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/data_sources/lstm-rust-realization-template.json) will run the [rust port](https://github.com/CIROH-UA/rust-lstm-1025/tree/main) of the python lstm above. It's an experimental drop in replacement that should produce identical results with a ~2-5x speedup depending on your setup.
