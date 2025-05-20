import logging

from dask.distributed import Client

logger = logging.getLogger(__name__)


def shutdown_cluster():
    try:
        client = Client.current()
        client.shutdown()
    except ValueError:
        logger.debug("No cluster found to shutdown")


def no_cluster(func):
    """
    Decorator that ensures the wrapped function runs with no active Dask cluster.

    This decorator attempts to shut down any existing Dask cluster before
    executing the wrapped function. If no cluster is found, it logs a debug message
    and continues execution.

    Parameters:
        func: The function to be executed without a Dask cluster

    Returns:
        wrapper: The wrapped function that will be executed without a Dask cluster
    """

    def wrapper(*args, **kwargs):
        shutdown_cluster()
        result = func(*args, **kwargs)
        return result

    return wrapper


def use_cluster(func):
    """
    Decorator that ensures the wrapped function has access to a Dask cluster.

    If a Dask cluster is already running, it uses the existing one.
    If no cluster is available, it creates a new one before executing the function.
    The cluster remains active after the function completes.

    Parameters:
        func: The function to be executed with a Dask cluster

    Returns:
        wrapper: The wrapped function with access to a Dask cluster
    """

    def wrapper(*args, **kwargs):
        try:
            client = Client.current()
        except ValueError:
            client = Client()
        result = func(*args, **kwargs)
        return result

    return wrapper


def temp_cluster(func):
    """
    Decorator that provides a temporary Dask cluster for the wrapped function.

    If a Dask cluster is already running, it uses the existing one and leaves it running.
    If no cluster exists, it creates a temporary one and shuts it down after
    the function completes.

    Parameters:
        func: The function to be executed with a Dask cluster

    Returns:
        wrapper: The wrapped function with access to a Dask cluster
    """

    def wrapper(*args, **kwargs):
        cluster_was_running = True
        try:
            client = Client.current()
        except ValueError:
            cluster_was_running = False
            client = Client()
        result = func(*args, **kwargs)
        if not cluster_was_running:
            client.shutdown()
        return result

    return wrapper
