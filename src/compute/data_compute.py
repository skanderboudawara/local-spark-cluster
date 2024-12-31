"""
This module defines the Compute class used to define a compute task with inputs and outputs.
"""

import inspect
import uuid
from typing import Any, Callable, Optional

from compute._logger import run_logger
from compute.input import Input
from compute.output import Output
from compute.session import session


class Compute:
    """
    This class defines a compute task with inputs and outputs.
    """
    def __init__(
        self,
        compute_func: Callable[..., Any],
        inputs: Optional[dict[str, Input]] = None,
        outputs: Optional[dict[str, Output]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        This class is used to define a compute task with inputs and outputs.

        :param compute_func: (Callable), Function to be executed.
        :param inputs: (dict), Dictionary of input objects.
        :param outputs: (dict), Dictionary of output objects.

        :returns: None
        """
        params = {} if params is None else params
        self.compute_func: Callable[..., Any] = compute_func
        self.spark = params.get("spark", session(app_name=f"master_{uuid.uuid4()!s}"))
        self.inputs: dict[str, Input] = inputs or {}
        run_logger.info("Inputs loaded via Compute class")
        self.outputs: dict[str, Output] = outputs or {}
        run_logger.info(msg="Outputs loaded via Compute class")

        # Extract metadata from the compute function
        self._initialize_function(compute_func)

    @property
    def app_name(self) -> Any:
        """
        This property returns the name of the Spark application.

        :param: None

        :returns: (str), Name of the Spark application.
        """
        return self.spark.sparkContext.appName

    def _initialize_function(self, func: Callable[..., Any]) -> None:
        """
        Extracts and stores metadata from the compute function.

        :param func: (Callable), Function to be executed.

        :returns: None
        """
        self.__name__: str = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self._arguments: inspect.FullArgSpec = inspect.getfullargspec(func=func)
        self._use_context: bool = "spark" in self._arguments.args

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        This method is called when the object is called as a function.

        :param args: (Any), Positional arguments.
        :param kwargs: (Any), Keyword arguments.

        :returns: Any
        """
        run_logger.info(msg=f"{self.compute_func.__name__} is being computed")
        if self._use_context:
            kwargs["spark"] = self.spark
        kwargs.update(self.inputs)
        kwargs.update(self.outputs)
        result: Any = self.compute_func(*args, **kwargs)
        run_logger.info(msg=f"{self.compute_func.__name__} completed")
        self.spark.stop()
        return result
