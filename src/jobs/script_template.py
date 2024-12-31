"""
This script is a template for creating a Spark job using PySpark. It demonstrates how to:

1. Initialize a Spark session.
2. Create a DataFrame with random data.
3. Save the DataFrame to a CSV file.
4. Load the DataFrame from the CSV file.
5. Display the first three rows of the DataFrame.
6. Stop the Spark session.
"""
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import rand
from pyspark.sql.session import SparkSession

from utils import get_spark_session

if __name__ == "__main__":
    app_name = "YourSparkApplicationName"
    spark_session: SparkSession = get_spark_session(app_name=app_name)

    # Create a random DataFrame
    df: DataFrame = spark_session.range(start=0, end=100).withColumn(colName="random", col=rand())
    # Show the DataFrame
    df.show()

    # Save the DataFrame to a CSV file
    df.write_dataframe(format="csv")

    # Load the dataframe
    processed_df: DataFrame = spark_session.read_dataframe(
        "data/output/YourSparkApplicationName",
        file_extension="csv",
        header=True,
        inferSchema=True,
    )

    # Show the first three rows of the DataFrame
    processed_df.limit(3).show()

    # Stop the Spark session
    spark_session.stop()
