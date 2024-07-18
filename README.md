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
pip install ngiab_data_preprocess
python -m map_app
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

**Note:** When using the tool, the output will be stored in the `./output/<your-first-catchment>/` folder. There is no overwrite protection on the folders.

# CLI Documentation

<details>
  <summary>Click to expand CLI documentation</summary>


## Arguments

- `-h`, `--help`: Show the help message and exit.
- `-i INPUT_FILE`, `--input_file INPUT_FILE`: Path to a CSV or TXT file containing a list of waterbody IDs or lat/lon pairs, or a single waterbody ID (e.g., `wb-5173`), or a single lat/lon pair.
- `-l`, `--latlon`: Use latitude and longitude instead of waterbody IDs. When used with `-i`, the file should contain lat/lon pairs.
- `-s`, `--subset`: Subset the hydrofabric to the given waterbody IDs or locations.
- `-f`, `--forcings`: Generate forcings for the given waterbody IDs or locations.
- `-r`, `--realization`: Create a realization for the given waterbody IDs or locations.
- `--start_date START_DATE`: Start date for forcings/realization (format YYYY-MM-DD).
- `--end_date END_DATE`: End date for forcings/realization (format YYYY-MM-DD).
- `-o OUTPUT_NAME`, `--output_name OUTPUT_NAME`: Name of the subset to be created (default is the first waterbody ID in the input file).

## Examples

`-l -s -f -r` can be combinded like normal cli flags, e.g. to subset, generate forcings and a realization, you can add `-sfr` or `-s -f -r` 

1. Subset hydrofabric using waterbody IDs:
   ```
   python -m ngiab_data_cli -i waterbody_ids.txt -s
   ```

2. Generate forcings using a single waterbody ID:
   ```
   python -m ngiab_data_cli -i wb-5173 -f --start_date 2023-01-01 --end_date 2023-12-31
   ```

3. Create realization using lat/lon pairs from a CSV file:
   ```
   python -m ngiab_data_cli -i locations.csv -l -r --start_date 2023-01-01 --end_date 2023-12-31 -o custom_output
   ```

4. Perform all operations using a single lat/lon pair:
   ```
   python -m ngiab_data_cli -i 54.33,-69.4 -l -s -f -r --start_date 2023-01-01 --end_date 2023-12-31
   ```

## File Formats

### 1. Waterbody ID input:
- CSV file: A single column of waterbody IDs, or a column named 'wb_id', 'waterbody_id', or 'divide_id'.
- TXT file: One waterbody ID per line.

Example CSV (waterbody_ids.csv):
```
wb_id,soil_type
wb-5173,some
wb-5174,data
wb-5175,here
```
Or:
```
wb-5173
wb-5174
wb-5175
```

### 2. Lat/Lon input:
- CSV file: Two columns named 'lat' and 'lon', or two unnamed columns in that order.
- Single pair: Comma-separated values passed directly to the `-i` argument.

Example CSV (locations.csv):
```
lat,lon
54.33,-69.4
55.12,-68.9
53.98,-70.1
```

Or:
```
54.33,-69.4
55.12,-68.9
53.98,-70.1
```

## Output

The script creates an output folder named after the first waterbody ID in the input file, the provided output name, or derived from the first lat/lon pair. This folder will contain the results of the subsetting, forcings generation, and realization creation operations.

</details>
