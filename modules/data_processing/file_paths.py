from pathlib import Path


class file_paths:
    """
    This class contains all of the file paths used in the NWM data processing
    workflow. The file paths are organized into static methods and instance
    methods. Static methods do not require a water body ID, while instance
    methods do.
    """

    config_file = Path("~/.NGIAB_data_preprocess").expanduser()

    def __init__(self, wb_id: str):
        """
        Initialize the file_paths class with a water body ID.
        The following functions require a water body ID:
        config_dir, forcings_dir, geopackage_path, cached_nc_file
        Args:
            wb_id (str): Water body ID.
        """
        self.wb_id = wb_id

    @staticmethod
    def get_working_dir() -> Path:
        try:
            with open(file_paths.config_file, "r") as f:
                return Path(f.readline().strip()).expanduser()
        except FileNotFoundError:
            return None

    @staticmethod
    def set_working_dir(working_dir: Path) -> None:
        with open(file_paths.config_file, "w") as f:
            f.write(str(working_dir))

    @staticmethod
    def data_sources() -> Path:
        return Path(__file__).parent.parent / "data_sources"

    @staticmethod
    def root_output_dir() -> Path:
        if file_paths.get_working_dir() is not None:
            return file_paths.get_working_dir()
        return Path(__file__).parent.parent.parent / "output"

    @staticmethod
    def template_gpkg() -> Path:
        return file_paths.data_sources() / "template.gpkg"

    @staticmethod
    def model_attributes() -> Path:
        return file_paths.data_sources() / "model_attributes.parquet"

    @staticmethod
    def conus_hydrofabric() -> Path:
        return file_paths.data_sources() / "conus.gpkg"

    @staticmethod
    def hydrofabric_graph() -> Path:
        return file_paths.conus_hydrofabric().with_suffix(".gpickle")

    @staticmethod
    def template_nc() -> Path:
        return file_paths.data_sources() / "template.nc"

    @staticmethod
    def dev_file() -> Path:
        return Path(__file__).parent.parent.parent / ".dev"

    @staticmethod
    def template_troute_config() -> Path:
        return file_paths.data_sources() / "ngen-routing-template.yaml"

    @staticmethod
    def template_realization_config() -> Path:
        return file_paths.data_sources() / "ngen-realization-template.json"

    @staticmethod
    def template_noahowp_config() -> Path:
        return file_paths.data_sources() / "noah-owp-modular-init.namelist.input"

    def subset_dir(self) -> Path:
        return file_paths.root_output_dir() / self.wb_id

    def config_dir(self) -> Path:
        return file_paths.subset_dir(self) / "config"

    def forcings_dir(self) -> Path:
        return file_paths.subset_dir(self) / "forcings"

    def geopackage_path(self) -> Path:
        return self.config_dir() / f"{self.wb_id}_subset.gpkg"

    def cached_nc_file(self) -> Path:
        return file_paths.subset_dir(self) / "merged_data.nc"

    def setup_run_folders(self) -> None:
        Path(self.subset_dir() / "restart").mkdir(parents=True, exist_ok=True)
        Path(self.subset_dir() / "lakeout").mkdir(parents=True, exist_ok=True)
        Path(self.subset_dir() / "outputs").mkdir(parents=True, exist_ok=True)
        Path(self.subset_dir() / "outputs" / "ngen").mkdir(parents=True, exist_ok=True)
        Path(self.subset_dir() / "outputs" / "parquet").mkdir(parents=True, exist_ok=True)
        Path(self.subset_dir() / "outputs" / "troute").mkdir(parents=True, exist_ok=True)
