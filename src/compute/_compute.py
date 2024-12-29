import inspect
import uuid
from typing import Any, Callable, Optional

from compute._dataset import Input, Output
from compute._logger import run_logger
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
        run_logger.info("Inputs loaded via Compute class")
        self.outputs = outputs or {}
        run_logger.info("Outputs loaded via Compute class")

        # Extract metadata from the compute function
        self._initialize_function(compute_func)

    @property
    def app_name(self) -> str:
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
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self._arguments = inspect.getfullargspec(func)
        self._use_context = "spark" in self._arguments.args

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        This method is called when the object is called as a function.

        :param args: (Any), Positional arguments.
        :param kwargs: (Any), Keyword arguments.

        :returns: Any
        """
        run_logger.info(f"{self.compute_func.__name__} is being computed")
        if self._use_context:
            kwargs["spark"] = self.spark
        kwargs.update(self.inputs)
        kwargs.update(self.outputs)
        result = self.compute_func(*args, **kwargs)
        run_logger.info(f"{self.compute_func.__name__} completed")
        self.spark.stop()
        return result
