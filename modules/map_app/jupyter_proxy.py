## Registers the map app with jupyter-server-proxy so it can be launched from the
## JupyterLab launcher / JupyterHub "New" menu. See the jupyter_serverproxy_servers
## entry point in pyproject.toml.
import sys
from pathlib import Path

PROXY_NAME = "map_app"


def setup_map_app():
    return {
        "command": [
            sys.executable,
            "-m",
            "map_app",
            "--host",
            "127.0.0.1",
            "--port",
            "{port}",
            "--no-browser",
            "--behind-proxy",
        ],
        # the prefix is stripped before reaching flask, this header lets url_for add it back
        "request_headers_override": {"X-Forwarded-Prefix": "{base_url}" + PROXY_NAME},
        # skip the terminal prompts, the first launch may download the hydrofabric (~GBs)
        "environment": {"NGIAB_NONINTERACTIVE": "1"},
        "timeout": 900,
        "absolute_url": False,
        "new_browser_tab": True,
        "launcher_entry": {
            "enabled": True,
            "title": "NGIAB Map App",
            "icon_path": str(Path(__file__).parent / "static" / "resources" / "map_app.svg"),
        },
    }
