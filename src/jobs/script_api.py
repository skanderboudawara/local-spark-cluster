"""
This transformation script reads a CSV file, selects a column, and writes the result to a new CSV file.
"""
import sys

sys.path.append("src/")

from pyspark.sql import DataFrame

from compute import cluster_conf, compute, Input, Output


@cluster_conf(app_name="select_col")
@compute(
    random=Input("data/input/random_data.csv", header=True, inferSchema=True),
    output=Output("data/output/selected_col2.csv"),
)
def compute_random(random: DataFrame, output: Output) -> None:
    """
    This function reads a CSV file, selects a column, and writes the result to a new CSV file.

    :param random: (DataFrame), DataFrame read from the input CSV file.
    :param output: (Output), Output instance to write the DataFrame.

    :return: None
    """
    random = random.dataframe()

    random.show()

    output.write(random)


if __name__ == "__main__":
    compute_random()
