## This file is run when the python -m map_app command is run
## It is the entry point for the application and is equivalent to run.sh
import logging
import webbrowser
from threading import Timer

from data_processing.file_paths import file_paths
from data_sources.source_validation import validate_all
from data_processing.graph_utils import get_graph
from flask import Flask

from .views import intra_module_db, main

validate_all()

with open("app.log", "w") as f:
    f.write("")
    f.write("Starting Application!\n")


app = Flask(__name__)
app.register_blueprint(main)

intra_module_db["app"] = app

logging.basicConfig(
    level=logging.INFO,
    format="%(name)-12s: %(levelname)s - %(message)s",
    filename="app.log",
    filemode="a",
)  # Append mode
# Example: Adding a console handler to root logger (optional)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Or any other level
formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
console_handler.setFormatter(formatter)
logging.getLogger("").addHandler(console_handler)


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


if __name__ == "__main__":

    # call this once to cache the graph
    Timer(1, get_graph).start()

    if file_paths.dev_file.is_file():
        Timer(2, set_logs_to_warning).start()
        with open("app.log", "a") as f:
            f.write("Running in debug mode\n")
        app.run(debug=True, host="0.0.0.0", port="8080")
    else:
        Timer(1, open_browser).start()
        with open("app.log", "a") as f:
            f.write("Running in production mode\n")
        app.run(host="0.0.0.0", port="0")
