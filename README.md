This repository contains tools for preparing data to run a [next gen](https://github.com/NOAA-OWP/ngen) simulation using [NGIAB](https://github.com/CIROH-UA/NGIAB-CloudInfra). The tools allow you to select a catchment of interest on an interactive map, choose a date range, and prepare the data with just a few clicks!

![map screenshot](https://github.com/CIROH-UA/NGIAB_data_preprocess/blob/main/modules/map_app/static/resources/screenshot.png)


## What does this tool do?

This tool prepares data to run a next gen simulation by creating a run package that can be used with NGIAB. It picks default data sources, including the [v20.1 hydrofabric](https://www.lynker-spatial.com/data?path=hydrofabric%2Fv20.1%2F) and [nwm retrospective v3 forcing](https://noaa-nwm-retrospective-3-0-pds.s3.amazonaws.com/index.html#CONUS/zarr/forcing/) data.

## Requirements

* This tool is officially supported on macOS or Ubuntu. To use it on Windows, please install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install).
* GDAL needs to be installed.
* The 'ogr2ogr' command needs to work in your terminal.

`sudo apt install gdal-bin` will install gdal and ogr2ogr on ubuntu / wsl

## Installation and running

```bash
#optional but encouraged: create a virtual environment
python3 -m venv env
source env/bin/activate

# installing and running the tool
pip install ngiab_data_preprocess
python -m map_app
```

The first time you run this command, it will download the hydrofabric and model parameter files from Lynker Spatial. If you already have them, place `conus.gpkg` and `model_attributes.parquet` into `modules/data_sources/`.

## Development Installation
<details>
  <summary>Installation steps</summary>
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
