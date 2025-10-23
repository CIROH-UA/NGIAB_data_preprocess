from datetime import datetime
from pathlib import Path
from typing import Optional


class FilePaths:
    """
    This class contains all of the file paths used in the data processing
    workflow.
    """

    config_file = Path("~/.ngiab/preprocessor").expanduser()
    hydrofabric_dir = Path("~/.ngiab/hydrofabric/v2.2").expanduser()
    hydrofabric_download_log = Path("~/.ngiab/hydrofabric/v2.2/download_log.json").expanduser()
    no_update_hf = Path("~/.ngiab/hydrofabric/v2.2/no_update").expanduser()
    output_dir = None
    data_sources = Path(__file__).parent.parent / "data_sources"
    map_app_static = Path(__file__).parent.parent / "map_app" / "static"
    tiles_tms = map_app_static / "tiles" / "tms"
    tiles_vpu = map_app_static / "tiles" / "vpu"
    template_gpkg = data_sources / "template.gpkg"
    template_sql = data_sources / "template.sql"
    triggers_sql = data_sources / "triggers.sql"
    conus_hydrofabric = hydrofabric_dir / "conus_nextgen.gpkg"
    dhbv_attributes = hydrofabric_dir / "dhbv_attrs.parquet"
    hydrofabric_graph = hydrofabric_dir / "conus_igraph_network.gpickle"
    template_nc = data_sources / "forcing_template.nc"
    dev_file = Path(__file__).parent.parent.parent / ".dev"
    template_troute_config = data_sources / "ngen-routing-template.yaml"
    template_cfe_nowpm_realization_config = data_sources / "cfe-nowpm-realization-template.json"
    template_lstm_realization_config = data_sources / "lstm-realization-template.json"
    template_lstm_rust_realization_config = data_sources / "lstm-rust-realization-template.json"
    template_noahowp_config = data_sources / "noah-owp-modular-init.namelist.input"
    template_cfe_config = data_sources / "cfe-template.ini"
    template_lstm_config = data_sources / "lstm-catchment-template.yml"
    template_dhbv2_realization_config = data_sources / "dhbv2-realization-template.json"
    template_dhbv2_config = data_sources / "dhbv2-catchment-template.yaml"

    def __init__(self, folder_name: Optional[str] = None, output_dir: Optional[Path] = None):
        """
        Initialize the FilePaths class with a the name of the output subfolder.
        OR the path to the output folder you want to use.
        use one or the other, not both

        Args:
            folder_name (str): Water body ID.
            output_dir (Path): Path to the folder you want to output to
        """
        if (not folder_name and not output_dir) or (folder_name and output_dir):
            raise ValueError("please pass either folder_name or output_dir")
        if folder_name:
            self.folder_name = folder_name
            self.output_dir = self.root_output_dir() / folder_name
        if output_dir:
            self.output_dir = Path(output_dir)
            self.folder_name = self.output_dir.stem

    @classmethod
    def get_working_dir(cls) -> Path | None:
        try:
            with open(cls.config_file, "r") as f:
                return Path(f.readline().strip()).expanduser()
        except FileNotFoundError:
            return None

    @classmethod
    def set_working_dir(cls, working_dir: Path) -> None:
        with open(cls.config_file, "w") as f:
            f.write(str(working_dir))

    @classmethod
    def root_output_dir(cls) -> Path:
        return cls.get_working_dir() or Path(__file__).parent.parent.parent / "output"

    @property
    def subset_dir(self) -> Path:
        if self.output_dir:
            return self.output_dir
        else:
            self.output_dir = self.root_output_dir() / self.folder_name
            return self.output_dir

    @property
    def config_dir(self) -> Path:
        return self.subset_dir / "config"

    @property
    def forcings_dir(self) -> Path:
        return self.subset_dir / "forcings"

    @property
    def metadata_dir(self) -> Path:
        meta_dir = self.subset_dir / "metadata"
        meta_dir.mkdir(parents=True, exist_ok=True)
        return meta_dir

    @property
    def forcing_progress_file(self) -> Path:
        return self.metadata_dir / "forcing_progress.json"

    @property
    def geopackage_path(self) -> Path:
        return self.config_dir / f"{self.folder_name}_subset.gpkg"

    @property
    def cached_nc_file(self) -> Path:
        return self.forcings_dir / "raw_gridded_data.nc"

    def append_cli_command(self, command: list[str]) -> None:
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        command_string = " ".join(command)
        history_file = self.metadata_dir / "cli_commands_history.txt"
        if not history_file.parent.exists():
            history_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.metadata_dir / "cli_commands_history.txt", "a") as f:
            f.write(f"{current_time}| {command_string}\n")

    def setup_run_folders(self) -> None:
        folders = [
            "outputs",
            "outputs/ngen",
            "outputs/troute",
            "metadata",
        ]
        for folder in folders:
            Path(self.subset_dir / folder).mkdir(parents=True, exist_ok=True)
