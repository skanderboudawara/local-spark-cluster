"""
This module contains functions to create a fake DataFrame and perform various window operations.

Functions:
    fake_dataframe(spark: SparkSession) -> DataFrame:
        Creates a fake DataFrame with predefined schema and data.

    do_exercice(df: DataFrame) -> DataFrame:
        Performs window operations on the given DataFrame, including:
        1. Calculating the rate change (difference between current and previous quarters' revenue).
        2. Calculating the cumulative average within each year.
        3. Calculating the moving average over the last two quarters (current and previous quarter).

Usage:
    1. Create a Spark session using `get_spark_session`.
    2. Generate a fake DataFrame using `fake_dataframe`.
    3. Write the fake DataFrame to a CSV file.
    4. Read the DataFrame from the CSV file.
    5. Apply the `do_exercice` function to perform window operations.
    6. Display the resulting DataFrame.
    7. Write the resulting DataFrame to a Parquet file.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, lag
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType
from pyspark.sql.window import Window

from utils import get_spark_session


def fake_dataframe(spark: SparkSession) -> DataFrame:
    """
    This method is used to create a fake DataFrame with sample data.

    :param spark: (SparkSession), object used to create the DataFrame.

    :return: (DataFrame), A DataFrame containing sample data with columns 'year', 'quarter',
        and 'revenue_rate'.
    """
    # Define schema
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("revenue_rate", FloatType(), True),
    ])

    # Create data
    data = [
        (2020, 1, 100.0),
        (2020, 2, 150.0),
        (2020, 3, 200.0),
        (2020, 4, 250.0),
        (2021, 1, 110.0),
        (2021, 2, 160.0),
        (2021, 3, 210.0),
        (2021, 4, 260.0),
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    return df


def do_exercice(df: DataFrame) -> DataFrame:
    """
    Performs window operations on the given DataFrame, including:

    1. Calculating the rate change (difference between current and previous quarters' revenue).
    2. Calculating the cumulative average within each year.
    3. Calculating the moving average over the last two quarters (current and previous quarter).

    :param df: (DataFrame), The input DataFrame containing columns 'year', 'quarter',
        and 'revenue_rate'.

    :return: (DataFrame), A DataFrame with additional columns 'rate_change', 'cumulative_avg',
        and 'moving_avg'.
    """
    # Define a window specification
    # The window is partitioned by year and ordered by quarter
    w = Window.partitionBy("year").orderBy("quarter")

    # 1. Calculate the rate change (difference between current and previous quarters revenue)
    df = df.withColumn(
        "rate_change",
        col("revenue_rate") - lag("revenue_rate", 1).over(w),
    )

    # 2. Calculate the cumulative average within each year
    df = df.withColumn(
        "cumulative_avg",
        avg("revenue_rate").over(w.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
    )

    # 3. Calculate the moving average over the last two quarters (current and previous quarter)
    df = df.withColumn(
        "moving_avg",
        avg("revenue_rate").over(w.rowsBetween(-1, 0)),
    )

    return df


# Create Spark session
spark_session = get_spark_session("PysparkWindow")

fake_dataframe(spark_session).write_dataframe(format="csv", custom_name="fake_data")
# Get inputs
df: DataFrame = spark_session.read_dataframe(
    "data/output/PysparkWindow/fake_data",
    file_extension="csv",
    header=True,
    inferSchema=True,
)

# Apply exercice
df = do_exercice(df)

# Display result
df.show()

# Store output
df.write_dataframe(format="parquet", custom_name="output")
