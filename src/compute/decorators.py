"""
This module contains decorators for defining compute tasks and configuring Spark sessions.

Decorators:
- compute: Defines a compute task with specified inputs and outputs.
- cluster_conf: Configures the Spark session and provides it to the wrapped function.
"""
from __future__ import annotations

import uuid
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from compute._logger import run_logger
from compute.data_compute import Compute
from compute.input import Input
from compute.output import Output
from compute.session import session
from compute.toolbox import filter_kwargs

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def compute(**compute_dict: Input | Output) -> Callable[..., Any]:
    """
    This decorator is used to define a compute task with inputs and outputs.

    :param compute_dict: (dict), Dictionary of input and output objects.

    :returns: (Callable), Decorator function.
    """
    def wrapper(compute_func: Callable) -> Callable[..., Any]:
        @wraps(wrapped=compute_func)  # Preserve original function metadata
        def wrapped_func(*_: Any, **f_kwargs: Any) -> Any:
            filtered_inputs: dict[str, Input] = filter_kwargs(kwargs=compute_dict, type=Input)
            filtered_outputs: dict[str, Output] = filter_kwargs(kwargs=compute_dict, type=Output)
            compute_instance = Compute(
                compute_func=compute_func,
                inputs=filtered_inputs,
                outputs=filtered_outputs,
                params=f_kwargs,
            )
            run_logger.info(msg=f"Spark app name is {compute_instance.app_name}")
            return compute_instance()
        return wrapped_func
    return wrapper


def cluster_conf(app_name : str | None = None, conf: dict | None = None) -> Callable[..., Any]:
    """
    This decorator is used to configure the Spark session and provide it to the wrapped function.

    :param app_name: (str), Name of the Spark application.
    :param conf: (dict), Configuration options for the Spark session.

    :returns: (Callable), Decorator function.
    """
    if app_name is None:
        app_name = f"master_{uuid.uuid4()!s}"

    def wrapper(func: Callable) -> Callable[..., Any]:
        @wraps(func)  # Preserve original function metadata
        def wrapped_func(*args: Any, **kwargs: Any) -> Any:
            # Initialize the Spark session
            spark: SparkSession = session(app_name=app_name, conf=conf)
            spark.sparkContext.setLogLevel(logLevel="WARN")
            kwargs["spark"] = session(app_name=app_name, conf=conf)
            # Call the wrapped function with the Spark session
            result: Any = func(*args, **kwargs)
            return result
        return wrapped_func
    return wrapper
