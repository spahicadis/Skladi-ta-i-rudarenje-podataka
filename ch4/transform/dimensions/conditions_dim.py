from itertools import chain
from pyspark.sql.functions import col, create_map, lit, row_number, when
from pyspark.sql.window import Window
from spark_session import get_spark_session

WEATHER_CATEGORY = {
    0: "clear",
    1: "cloudy",
    2: "cloudy",
    3: "cloudy",
    51: "drizzle",
    53: "drizzle",
    55: "drizzle",
    61: "rain",
    63: "rain",
}

COLUMNS = ["weather_category", "temperature_bucket", "precipitation_bucket", "lighting"]


def categorize_conditions(df):
    mapping = create_map([lit(x) for x in chain(*WEATHER_CATEGORY.items())])

    return (
        df
        .withColumn("weather_category", mapping[col("weather_code").cast("int")])
        .withColumn(
            "temperature_bucket",
            when(col("temperature") < 20, lit("cool"))
            .when(col("temperature") < 25, lit("mild"))
            .when(col("temperature") < 30, lit("warm"))
            .otherwise(lit("hot")),
        )
        .withColumn(
            "precipitation_bucket",
            when(col("precipitation_mm") <= 0, lit("none"))
            .when(col("precipitation_mm") <= 0.5, lit("light"))
            .when(col("precipitation_mm") <= 2.5, lit("moderate"))
            .otherwise(lit("heavy")),
        )
    )


def transform_conditions_dim(weather_df, motion_df, csv_df=None):
    get_spark_session()

    postgres_df = (
        motion_df.alias("m")
        .join(weather_df.alias("w"), col("m.weather_observation_fk") == col("w.id"))
        .select(col("w.weather_code"), col("w.temperature"), col("w.precipitation_mm"), col("m.lighting"))
    )

    if csv_df:
        csv_source = csv_df.select(
            col("weather_code"),
            col("temp_c").alias("temperature"),
            col("precip_mm").alias("precipitation_mm"),
            col("lighting"),
        )
        combined_df = postgres_df.unionByName(csv_source)
    else:
        combined_df = postgres_df

    combined_df = categorize_conditions(combined_df).select(*COLUMNS).dropDuplicates(COLUMNS)

    window = Window.orderBy(*COLUMNS)
    combined_df = combined_df.withColumn("conditions_tk", row_number().over(window))

    return combined_df.select("conditions_tk", *COLUMNS).orderBy("conditions_tk")
