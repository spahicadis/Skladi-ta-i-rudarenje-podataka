from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session

COLUMNS = [
    "truncated", "plate_visible", "headlights_on", "commercial_markings",
    "cause", "direction", "special_use", "road_condition",
]


def _from_postgres(vehicle_df, inference_df, motion_df):
    return (
        vehicle_df.alias("v")
        .join(inference_df.alias("i"), col("v.inference_fk") == col("i.id"))
        .join(motion_df.alias("m"), col("i.motion_fk") == col("m.id"))
        .select(
            col("v.truncated"), col("v.plate_visible"), col("v.headlights_on"),
            col("v.commercial_markings"), col("m.cause"), col("v.direction"),
            col("v.special_use"), col("i.road_condition"),
        )
    )


def transform_observation_dim(vehicle_df, inference_df, motion_df, csv_df=None):
    get_spark_session()

    postgres_df = _from_postgres(vehicle_df, inference_df, motion_df)

    if csv_df:
        combined_df = postgres_df.unionByName(csv_df.select(*COLUMNS))
    else:
        combined_df = postgres_df

    combined_df = combined_df.dropDuplicates(COLUMNS)

    window = Window.orderBy(*COLUMNS)
    combined_df = combined_df.withColumn("observation_tk", row_number().over(window))

    return combined_df.select("observation_tk", *COLUMNS).orderBy("observation_tk")
