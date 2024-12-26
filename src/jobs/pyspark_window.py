import sys

sys.path.append("src/")

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lag
from pyspark.sql.window import Window

from spark_session import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


def fake_dataframe(spark: SparkSession) -> DataFrame:
    # Define schema
    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("quarter", IntegerType(), True),
        StructField("revenue_rate", FloatType(), True)
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
        (2021, 4, 260.0)
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)

    return df

def do_exercice(df: DataFrame) -> DataFrame:
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
    inferSchema=True
)

# Apply exercice
df = do_exercice(df)

# Display result
df.show()

# Store output
df.write_dataframe(format="parquet", custom_name="output")
