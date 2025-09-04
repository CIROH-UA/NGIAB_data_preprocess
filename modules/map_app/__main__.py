## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
import logging
import webbrowser
from threading import Timer

from data_processing.file_paths import FilePaths
from data_processing.graph_utils import get_graph

from map_app import app, console_handler


def open_browser():
    # find the last line in the log file that contains the port number
    # * running on http://0.0.0.0:port_number
    port_number = None
    with open("app.log", "r") as f:
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


def main():
    # call this once to cache the graph
    Timer(1, get_graph).start()

    if FilePaths.dev_file.is_file():
        Timer(2, set_logs_to_warning).start()
        with open("app.log", "a") as f:
            f.write("Running in debug mode\n")
        app.run(debug=True, host="0.0.0.0", port="8080")  # type: ignore
    else:
        Timer(1, open_browser).start()
        with open("app.log", "a") as f:
            f.write("Running in production mode\n")
        app.run(host="0.0.0.0", port="0")  # type: ignore


if __name__ == "__main__":
    main()
