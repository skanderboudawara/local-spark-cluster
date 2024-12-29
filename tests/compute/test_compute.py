

import src.compute._compute as C

def dummy_compute_func(spark, input1, input2, output1):
    return input1 + input2


def test_compute_initialization(spark_session):
    inputs = {"input1": 1, "input2": 2}
    outputs = {"output1": 3}
    params = {"spark": spark_session}

    compute = C.Compute(compute_func=dummy_compute_func, inputs=inputs, outputs=outputs, params=params)

    assert compute.compute_func == dummy_compute_func
    assert compute.inputs == inputs
    assert compute.outputs == outputs
    assert compute.spark == spark_session
    assert compute.app_name.startswith("test")


def test_compute_call(spark_session):
    inputs = {"input1": 1, "input2": 2}
    outputs = {"output1": 3}
    params = {"spark": spark_session}

    compute = C.Compute(compute_func=dummy_compute_func, inputs=inputs, outputs=outputs, params=params)

    result = compute()
    assert result == 3
