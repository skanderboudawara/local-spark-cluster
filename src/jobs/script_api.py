"""
This transformation script reads a CSV file, selects a column, and writes the result to a new CSV file.
"""
from compute.decorators import compute, cluster_conf
from compute._dataset import Input, Output
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.types import IntegerType

@cluster_conf(app_name="select_col")
@compute(
    random=Input("input/random_data.csv", header=True, inferSchema=True),
    output=Output("output/selected_col2.csv")
)
def compute_random(random: DataFrame, output: Output) -> None:
    """
    This function reads a CSV file, selects a column, and writes the result to a new CSV file.
    
    :param random: (DataFrame), DataFrame read from the input CSV file.
    :param output: (Output), Output instance to write the DataFrame.
    
    :return: None
    """
    random = random.dataframe()
    
    random = random.select(
        (col("ID").cast(IntegerType())*lit(2)).alias("ID2")
    )
    random.show()
    
    output.write(random)
    

if __name__ == "__main__":
    compute_random()