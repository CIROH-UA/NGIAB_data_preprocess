## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
import argparse
import logging
import webbrowser
from pathlib import Path
from threading import Timer

from data_processing.file_paths import FilePaths
from data_processing.graph_utils import get_graph

from map_app import app, console_handler

LOG_PATH = Path.home() / ".ngiab" / "app.log"


def open_browser():
    # find the last line in the log file that contains the port number
    # * running on http://0.0.0.0:port_number
    port_number = None
    with open(LOG_PATH, "r") as f:
        lines = f.readlines()
        for line in reversed(lines):
            if "Running on http" in line:
                port_number = line.split(":")[-1].strip()
                break
    if port_number is not None:
        webbrowser.open(f"http://localhost:{port_number}")
    else:
        print("Could not find the port number in the log file. Please open the url manually.")
    set_logs_to_warning()


def set_logs_to_warning():
    logging.getLogger("werkzeug").setLevel(logging.WARNING)
    console_handler.setLevel(logging.DEBUG)


def parse_args():
    parser = argparse.ArgumentParser(description="Run the NGIAB data preprocess map app")
    parser.add_argument("--host", default="0.0.0.0", help="Interface to bind to")
    parser.add_argument("--port", type=int, help="Port to listen on (default: random free port)")
    parser.add_argument(
        "--no-browser", action="store_true", help="Don't open the app in a web browser"
    )
    parser.add_argument(
        "--behind-proxy",
        action="store_true",
        help="Trust the X-Forwarded-Prefix header, e.g. when served by jupyter-server-proxy",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # call this once to cache the graph
    Timer(1, get_graph).start()

    if args.behind_proxy:
        from werkzeug.middleware.proxy_fix import ProxyFix

        # only trust the prefix header so url_for generates urls under the proxy path
        app.wsgi_app = ProxyFix(app.wsgi_app, x_for=0, x_proto=0, x_host=0, x_prefix=1)

    if args.port is not None or args.no_browser:
        if not args.no_browser:
            Timer(1, open_browser).start()
        else:
            Timer(2, set_logs_to_warning).start()
        app.run(host=args.host, port=args.port or 0)
    elif FilePaths.dev_file.is_file():
        Timer(2, set_logs_to_warning).start()
        with open(LOG_PATH, "a") as f:
            f.write("Running in debug mode\n")
        app.run(debug=True, host="0.0.0.0", port="8080")  # type: ignore
    else:
        Timer(1, open_browser).start()
        with open(LOG_PATH, "a") as f:
            f.write("Running in production mode\n")
        app.run(host="0.0.0.0", port="0")  # type: ignore


if __name__ == "__main__":
    main()
