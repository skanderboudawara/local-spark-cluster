"""
This transformation script reads a CSV file, selects a column, writes the result to a new CSV file
"""
from typing import TYPE_CHECKING

from compute import Input, Output, cluster_conf, compute

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@cluster_conf(app_name="select_col")
@compute(
    random_data=Input(path="data/input/random_data.csv", header=True, inferSchema=True),
    output=Output(path="data/output/selected_col2.csv"),
)
def compute_random(random_data: Input, output: Output) -> None:
    """
    This function reads a CSV file, selects a column, and writes the result to a new CSV file.

    :param random: (DataFrame), DataFrame read from the input CSV file.
    :param output: (Output), Output instance to write the DataFrame.

    :return: None
    """
    random_data_imported: DataFrame = random_data.dataframe()

    random_data_imported.show()

    output.write(df=random_data_imported)


if __name__ == "__main__":
    compute_random()
