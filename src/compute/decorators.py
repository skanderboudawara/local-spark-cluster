from __future__ import annotations

import uuid
from typing import Any, Callable

from compute._compute import Compute
from compute._dataset import Input, Output
from compute._logger import logger
from compute._utils import filter_kwargs, spark_session


def compute(**compute_dict: dict[str, Input | Output | Any]) -> Callable:
    """
    This decorator is used to define a compute task with inputs and outputs.

    :param compute_dict: (dict), Dictionary of input and output objects.

    :returns: (Callable), Decorator function.
    """
    def wrapper(compute_func):
        def wrapped_func(*f_args, **f_kwargs):
                filtered_inputs = filter_kwargs(compute_dict, Input)
                filtered_outputs = filter_kwargs(compute_dict, Output)
                logger.info("Inputs and Outputs loaded")
                compute_instance = Compute(compute_func, inputs=filtered_inputs, outputs=filtered_outputs, params=f_kwargs)
                logger.info(f"Decorator returns: {type(compute_instance)}")
                logger.info(f"App Name is {compute_instance.app_name}")
                return compute_instance()

        return wrapped_func
    return wrapper


def cluster_conf(app_name : str | None = None, conf: dict | None = None) -> Callable:
    """
    This decorator is used to configure the Spark session and provide it to the wrapped function.

    :param app_name: (str), Name of the Spark application.
    :param conf: (dict), Configuration options for the Spark session.

    :returns: (Callable), Decorator function.
    """
    if app_name is None:
        app_name = f"master_{uuid.uuid4()!s}"

    def wrapper(func):
        def wrapped_func(*args, **kwargs):
            # Initialize the Spark session
            spark = spark_session(app_name, conf)
            spark.sparkContext.setLogLevel("WARN")
            kwargs["spark"] = spark_session(app_name, conf)
            # Call the wrapped function with the Spark session
            result = func(*args, **kwargs)
            return result
        return wrapped_func
    return wrapper
