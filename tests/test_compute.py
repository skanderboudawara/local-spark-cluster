

from typing import Literal
import compute.data_compute as C
from compute import Input, Output
from pyspark.sql.session import SparkSession

def dummy_compute_func(spark, input1, input2, output1) -> Literal[3]:
    print(input1)
    print(input2)
    print(output1)
    return 3


def test_compute_initialization(spark_session: SparkSession) -> None:
    inputs: dict[str, Input] = {"input1": Input(path="1"), "input2": Input(path="2")}
    outputs: dict[str, Output] = {"output1": Output(path="3")}
    params: dict[str, SparkSession] = {"spark": spark_session}

    compute = C.Compute(compute_func=dummy_compute_func, inputs=inputs, outputs=outputs, params=params)

    assert compute.compute_func == dummy_compute_func
    assert compute.inputs == inputs
    assert compute.outputs == outputs
    assert compute.spark == spark_session
    assert compute.app_name.startswith("test")
    compute.spark.stop()


def test_compute_call(spark_session: SparkSession) -> None:
    inputs: dict[str, Input] = {"input1": Input(path="1"), "input2": Input(path="2")}
    outputs: dict[str, Output] = {"output1": Output(path="3")}
    params: dict[str, SparkSession] = {"spark": spark_session}

    compute = C.Compute(compute_func=dummy_compute_func, inputs=inputs, outputs=outputs, params=params)

    result: Literal[3] = compute()
    assert result == 3
