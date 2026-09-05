from pyspark.sql.functions import col, dayofmonth, dayofweek, date_format, month, quarter, row_number, to_date, when, year
from pyspark.sql.window import Window
from spark_session import get_spark_session


def _dates(df, ts_column):
    return (
        df
        .select(to_date(col(ts_column)).alias("full_date"))
        .dropna()
        .dropDuplicates()
    )


def transform_date_dim(motion_df, csv_df=None):
    get_spark_session()

    postgres_df = _dates(motion_df, "ts")

    if csv_df:
        combined_df = postgres_df.unionByName(_dates(csv_df, "ts")).dropDuplicates(["full_date"])
    else:
        combined_df = postgres_df

    combined_df = (
        combined_df
        .withColumn("day", dayofmonth("full_date"))
        .withColumn("day_of_week", date_format("full_date", "EEEE"))
        .withColumn("is_weekend", when(dayofweek("full_date").isin(1, 7), True).otherwise(False))
        .withColumn("month", month("full_date"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("year", year("full_date"))
    )

    window = Window.orderBy("full_date")
    combined_df = combined_df.withColumn("date_tk", row_number().over(window))

    return combined_df.select(
        "date_tk", "full_date", "day", "day_of_week", "is_weekend", "month", "quarter", "year"
    ).orderBy("full_date")
