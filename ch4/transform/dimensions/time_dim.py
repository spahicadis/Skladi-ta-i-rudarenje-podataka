from pyspark.sql.functions import col, hour, lit, minute, row_number, when
from pyspark.sql.window import Window
from spark_session import get_spark_session


def _times(df, ts_column):
    return (
        df
        .select(hour(col(ts_column)).alias("hour"), minute(col(ts_column)).alias("minute"))
        .dropna()
        .dropDuplicates()
    )


def transform_time_dim(motion_df, csv_df=None):
    get_spark_session()

    postgres_df = _times(motion_df, "ts")

    if csv_df:
        combined_df = postgres_df.unionByName(_times(csv_df, "ts")).dropDuplicates(["hour", "minute"])
    else:
        combined_df = postgres_df

    combined_df = combined_df.withColumn(
        "part_of_day",
        when(col("hour") < 6, lit("night"))
        .when(col("hour") < 12, lit("morning"))
        .when(col("hour") < 18, lit("afternoon"))
        .otherwise(lit("evening")),
    )

    window = Window.orderBy("hour", "minute")
    combined_df = combined_df.withColumn("time_tk", row_number().over(window))

    return combined_df.select("time_tk", "hour", "minute", "part_of_day").orderBy("hour", "minute")
