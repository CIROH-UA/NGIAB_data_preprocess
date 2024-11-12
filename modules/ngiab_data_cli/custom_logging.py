import logging

from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    def format(self, record):
        message = super().format(record)
        if record.levelno == logging.DEBUG:
            return f"{Fore.BLUE}{message}{Style.RESET_ALL}"
        if record.levelno == logging.WARNING:
            return f"{Fore.YELLOW}{message}{Style.RESET_ALL}"
        if record.levelno == logging.CRITICAL or record.levelno == logging.ERROR:
            return f"{Fore.RED}{message}{Style.RESET_ALL}"
        if record.name == "root":  # Only color info messages from this script green
            return f"{Fore.GREEN}{message}{Style.RESET_ALL}"
        return message


def setup_logging() -> None:
    """Set up logging configuration with green formatting."""
    handler = logging.StreamHandler()
    handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])


def set_logging_to_critical_only() -> None:
    """Set logging to CRITICAL level only."""
    logging.getLogger().setLevel(logging.CRITICAL)
    # Explicitly set Dask's logger to CRITICAL level
    logging.getLogger("distributed").setLevel(logging.CRITICAL)
