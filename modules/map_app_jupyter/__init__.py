## Registers the map app with jupyter-server-proxy so it can be launched from the
## JupyterLab launcher / JupyterHub "New" menu.
## This is kept separate from map_app because importing map_app runs the startup checks,
## and this module gets imported by the jupyter server itself.
##
## Two ways to use it:
##   1. pip install 'ngiab_data_preprocess[jupyter]' into the jupyter server environment,
##      setup_map_app is picked up by the jupyter_serverproxy_servers entry point.
##   2. uvx --from ngiab-data-preprocess map_app_jupyter_install
##      writes a jupyter config file that launches the app with uvx, so nothing needs
##      to be installed into the jupyter server environment except jupyter-server-proxy.
import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

PROXY_NAME = "map_app"
ICON_PATH = Path(__file__).parent / "map_app.svg"
CONFIG_DIR = Path.home() / ".jupyter"
# jupyter_server_config.d/*.json is only used for enabling extensions, so this has to go in
# the main config file, which may already contain user settings
CONFIG_FILE = CONFIG_DIR / "jupyter_server_config.json"
INSTALLED_ICON = CONFIG_DIR / "ngiab_map_app.svg"


def _server_config(launch_command, icon_path):
    return {
        "command": launch_command
        + ["--host", "127.0.0.1", "--port", "{port}", "--no-browser", "--behind-proxy"],
        # the prefix is stripped before reaching flask, this header lets url_for add it back
        "request_headers_override": {"X-Forwarded-Prefix": "{base_url}" + PROXY_NAME},
        # skip the terminal prompts, the first launch may download the hydrofabric (~GBs)
        "environment": {"NGIAB_NONINTERACTIVE": "1"},
        "timeout": 900,
        "absolute_url": False,
        "new_browser_tab": False,
        "launcher_entry": {
            "enabled": True,
            "title": "NGIAB Map App",
            "icon_path": str(icon_path),
        },
    }


def setup_map_app():
    return _server_config([sys.executable, "-m", "map_app"], ICON_PATH)


def _check_server_proxy():
    # this runs in the uvx environment, so ask the jupyter on PATH instead of importing
    jupyter = shutil.which("jupyter")
    if jupyter is None:
        print("Warning: couldn't find jupyter on PATH to check for jupyter-server-proxy")
        return
    result = subprocess.run(
        [jupyter, "server", "extension", "list"], capture_output=True, text=True
    )
    if "jupyter_server_proxy" not in result.stdout + result.stderr:
        print(
            "Warning: jupyter-server-proxy doesn't appear to be installed in the jupyter "
            "server environment.\nInstall it there with: pip install jupyter-server-proxy"
        )


def _read_config():
    if CONFIG_FILE.is_file():
        return json.loads(CONFIG_FILE.read_text() or "{}")
    return {}


def install(package_spec, uvx_path):
    uvx = uvx_path or shutil.which("uvx")
    if uvx is None:
        sys.exit("Couldn't find uvx, pass its location with --uvx")
    # use an absolute path, the jupyter server's PATH may not include ~/.local/bin
    uvx = str(Path(uvx).resolve())

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy(ICON_PATH, INSTALLED_ICON)
    command = [uvx, "--from", package_spec, "map_app"]
    config = _read_config()
    servers = config.setdefault("ServerProxy", {}).setdefault("servers", {})
    servers[PROXY_NAME] = _server_config(command, INSTALLED_ICON)
    CONFIG_FILE.write_text(json.dumps(config, indent=2))

    print(f"Added the map app to {CONFIG_FILE}")
    print(f"The map app will be launched with: {' '.join(command)}")
    _check_server_proxy()
    print(
        "Restart your jupyter server to see the NGIAB Map App launcher "
        "(JupyterHub: File > Hub Control Panel > Stop My Server)"
    )


def uninstall():
    config = _read_config()
    proxy = config.get("ServerProxy", {})
    if proxy.get("servers", {}).pop(PROXY_NAME, None) is not None:
        if not proxy["servers"]:
            del proxy["servers"]
        if not proxy:
            del config["ServerProxy"]
        CONFIG_FILE.write_text(json.dumps(config, indent=2))
        print(f"Removed the map app from {CONFIG_FILE}")
    if INSTALLED_ICON.exists():
        INSTALLED_ICON.unlink()
    print("Restart your jupyter server for the change to take effect")


def main():
    parser = argparse.ArgumentParser(
        description="Add an NGIAB Map App launcher to Jupyter that runs the app with uvx"
    )
    parser.add_argument(
        "--from",
        dest="package_spec",
        default="ngiab-data-preprocess",
        help="Package spec for uvx --from, e.g. a pinned version or git url "
        "(default: ngiab-data-preprocess)",
    )
    parser.add_argument("--uvx", help="Path to the uvx executable (default: found on PATH)")
    parser.add_argument("--uninstall", action="store_true", help="Remove the launcher")
    args = parser.parse_args()

    if args.uninstall:
        uninstall()
    else:
        install(args.package_spec, args.uvx)


if __name__ == "__main__":
    main()
