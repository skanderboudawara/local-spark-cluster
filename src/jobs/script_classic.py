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

from compute import session

if __name__ == "__main__":  # pragma: no cover
    app_name = "YourSparkApplicationName"
    spark_session: SparkSession = session(app_name=app_name)

    # Create a random DataFrame
    df: DataFrame = spark_session.range(start=0, end=100).withColumn(colName="random", col=rand())
    # Show the DataFrame
    df.show()

    # Save the DataFrame to a CSV file
    df.write.mode("overwrite") \
        .format("csv") \
        .option(key="header", value="true") \
        .save("data/output/YourSparkApplicationName")

    # Load the dataframe
    processed_df: DataFrame = spark_session.read.csv(
        "data/output/YourSparkApplicationName", header=True, inferSchema=True
    )

    # Show the first three rows of the DataFrame
    processed_df.limit(3).show()

    # Stop the Spark session
    spark_session.stop()
