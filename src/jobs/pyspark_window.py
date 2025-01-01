"""
This module contains functions to create a fake DataFrame and perform various window operations.

Functions:
    fake_dataframe(spark: SparkSession) -> DataFrame:
        Creates a fake DataFrame with predefined schema and data.

    do_exercise(df: DataFrame) -> DataFrame:
        Performs window operations on the given DataFrame, including:
        1. Calculating the rate change (difference between current and previous quarters' revenue).
        2. Calculating the cumulative average within each year.
        3. Calculating the moving average over the last two quarters (current and previous quarter).

Usage:
    1. Create a Spark session using `session`.
    2. Generate a fake DataFrame using `fake_dataframe`.
    3. Write the fake DataFrame to a CSV file.
    4. Read the DataFrame from the CSV file.
    5. Apply the `do_exercise` function to perform window operations.
    6. Display the resulting DataFrame.
    7. Write the resulting DataFrame to a Parquet file
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, lag
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType
from pyspark.sql.window import Window, WindowSpec

from compute import session


def fake_dataframe(spark: SparkSession) -> DataFrame:  # pragma: no cover
    """
    This method is used to create a fake DataFrame with sample data.

    :param spark: (SparkSession), object used to create the DataFrame.

    :return: (DataFrame), A DataFrame containing sample data with columns 'year', 'quarter',
        and 'revenue_rate'.
    """
    # Define schema
    schema = StructType(fields=[
        StructField(name="year", dataType=IntegerType(), nullable=True),
        StructField(name="quarter", dataType=IntegerType(), nullable=True),
        StructField(name="revenue_rate", dataType=FloatType(), nullable=True),
    ])

    # Create data
    data: list[tuple[int, int, float]] = [
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
    df: DataFrame = spark.createDataFrame(data=data, schema=schema)

    return df


def do_exercise(df: DataFrame) -> DataFrame:  # pragma: no cover
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
    w: WindowSpec = Window.partitionBy("year").orderBy("quarter")

    # 1. Calculate the rate change (difference between current and previous quarters revenue)
    df = df.withColumn(
        colName="rate_change",
        col=col(col="revenue_rate") - lag(col="revenue_rate", offset=1).over(window=w),
    )

    # 2. Calculate the cumulative average within each year
    df = df.withColumn(
        colName="cumulative_avg",
        col=avg(col="revenue_rate").over(
            window=w.rowsBetween(start=Window.unboundedPreceding, end=Window.currentRow),
        ),
    )

    # 3. Calculate the moving average over the last two quarters (current and previous quarter)
    df = df.withColumn(
        colName="moving_avg",
        col=avg(col="revenue_rate").over(window=w.rowsBetween(start=-1, end=0)),
    )

    return df


if __name__ == "__main__":  # pragma: no cover
    # Create Spark session
    spark_session: SparkSession = session(app_name="PysparkWindow")

    fake_dataframe: DataFrame = fake_dataframe(spark=spark_session)

    fake_dataframe.write.mode("overwrite") \
        .format("csv") \
        .option(key="header", value="true") \
        .save("data/output/PysparkWindow/fake_data")

    df: DataFrame = spark_session.read.csv(
        "data/output/PysparkWindow/fake_data",
        header=True,
        inferSchema=True
    )

    # Apply exercise
    df = do_exercise(df)

    # Display result
    df.show()

    df.write.mode("overwrite") \
        .format("parquet") \
        .save("data/output/PysparkWindow/final")

    spark_session.stop()
