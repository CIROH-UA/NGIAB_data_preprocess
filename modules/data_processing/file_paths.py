from pathlib import Path

class file_paths:
    """
    This class contains all of the file paths used in the data processing
    workflow.
    """
    config_file = Path("~/.NGIAB_data_preprocess").expanduser()
    data_sources = Path(__file__).parent.parent / "data_sources"
    map_app_static = Path(__file__).parent.parent / "map_app" / "static"
    tiles_tms = map_app_static / "tiles" / "tms"
    tiles_vpu = map_app_static / "tiles" / "vpu"
    template_gpkg = data_sources / "template.gpkg"
    template_sql = data_sources / "template.sql"
    triggers_sql = data_sources / "triggers.sql"
    model_attributes = data_sources / "model_attributes.parquet"
    conus_hydrofabric = data_sources / "conus.gpkg"
    hydrofabric_graph = conus_hydrofabric.with_suffix(".gpickle")
    template_nc = data_sources / "template.nc"
    dev_file = Path(__file__).parent.parent.parent / ".dev"
    template_troute_config = data_sources / "ngen-routing-template.yaml"
    template_realization_config = data_sources / "ngen-realization-template.json"
    template_noahowp_config = data_sources / "noah-owp-modular-init.namelist.input"

    def __init__(self, folder_name: str):
        """
        Initialize the file_paths class with a the name of the output subfolder.

        Args:
            folder_name (str): Water body ID.
        """
        self.cat_id = folder_name

    @classmethod
    def get_working_dir(cls) -> Path:
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
        if cls.get_working_dir() is not None:
            return cls.get_working_dir()
        return Path(__file__).parent.parent.parent / "output"

    @property
    def subset_dir(self) -> Path:
        return self.root_output_dir() / self.cat_id

    @property
    def config_dir(self) -> Path:
        return self.subset_dir / "config"

    @property
    def forcings_dir(self) -> Path:
        return self.subset_dir / "forcings"

    @property
    def metadata_dir(self) -> Path:
        return self.subset_dir / "metadata"

    @property
    def geopackage_path(self) -> Path:
        return self.config_dir / f"{self.cat_id}_subset.gpkg"

    @property
    def cached_nc_file(self) -> Path:
        return self.subset_dir / "merged_data.nc"

    def setup_run_folders(self) -> None:
        folders = [
            "restart",
            "lakeout",
            "outputs",
            "outputs/ngen",
            "outputs/parquet",
            "outputs/troute",
            "metadata",
        ]
        for folder in folders:
            Path(self.subset_dir / folder).mkdir(parents=True, exist_ok=True)
