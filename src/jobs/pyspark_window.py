from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lag
from pyspark.sql.window import Window

from spark_session import get_spark_session


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

# Get inputs
df: DataFrame = spark_session.read_dataframe("PysparkWindow/data.csv", header=True, inferSchema=True)

# Apply exercice
df = do_exercice(df)

# Display result
df.show()

# Store output
df.write_dataframe(format="parquet")
