import inspect
import uuid
from typing import Any, Callable, Optional

from compute._dataset import Input, Output
from compute._logger import logger
from compute._utils import spark_session


class Compute:
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
        self.compute_func = compute_func
        self.spark = params.get("spark", spark_session(f"master_{uuid.uuid4()!s}"))
        self.inputs = inputs or {}
        # self.inputs = {k: v.read for k, v in inputs.items()} if inputs else {}
        logger.info("Inputs loaded via Compute class")
        self.outputs = outputs or {}
        logger.info("Outputs loaded via Compute class")

        # Extract metadata from the compute function
        self._initialize_function(compute_func)

    @property
    def app_name(self):
        return self.spark.sparkContext.appName

    def _initialize_function(self, func: Callable[..., Any]) -> None:
        """Extracts and stores metadata from the compute function."""
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self._arguments = inspect.getfullargspec(func)
        self._use_context = "spark" in self._arguments.args

    def __call__(self, *args, **kwargs):
        logger.info("Compute.__call__ triggered")
        if self._use_context:
            kwargs["spark"] = self.spark
        kwargs.update(self.inputs)
        kwargs.update(self.outputs)
        result = self.compute_func(*args, **kwargs)
        self.spark.stop()
        return result
