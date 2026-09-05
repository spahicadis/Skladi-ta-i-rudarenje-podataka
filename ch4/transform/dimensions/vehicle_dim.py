from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session

COLUMNS = ["vehicle_class", "brand", "model", "color", "color_group", "body", "era"]


def _from_postgres(vehicle_df, class_df, brand_df, color_df):
    return (
        vehicle_df.alias("v")
        .join(class_df.alias("k"), col("v.class_fk") == col("k.id"), "left")
        .join(brand_df.alias("b"), col("v.brand_fk") == col("b.id"), "left")
        .join(color_df.alias("o"), col("v.color_fk") == col("o.id"), "left")
        .select(
            col("k.name").alias("vehicle_class"),
            col("b.name").alias("brand"),
            col("v.model").alias("model"),
            col("o.name").alias("color"),
            col("o.color_group").alias("color_group"),
            col("v.body").alias("body"),
            col("v.era").alias("era"),
        )
    )


def _from_csv(csv_df, color_df):
    return (
        csv_df.alias("s")
        .join(color_df.alias("o"), col("s.color") == col("o.name"), "left")
        .select(
            col("s.vehicle_class").alias("vehicle_class"),
            col("s.make").alias("brand"),
            col("s.model").alias("model"),
            col("s.color").alias("color"),
            col("o.color_group").alias("color_group"),
            col("s.body").alias("body"),
            col("s.era").alias("era"),
        )
    )


def transform_vehicle_dim(vehicle_df, class_df, brand_df, color_df, csv_df=None):
    get_spark_session()

    postgres_df = _from_postgres(vehicle_df, class_df, brand_df, color_df)

    if csv_df:
        combined_df = postgres_df.unionByName(_from_csv(csv_df, color_df))
    else:
        combined_df = postgres_df

    combined_df = combined_df.dropDuplicates(COLUMNS)

    window = Window.orderBy(*COLUMNS)
    combined_df = combined_df.withColumn("vehicle_tk", row_number().over(window))

    return combined_df.select("vehicle_tk", *COLUMNS).orderBy("vehicle_tk")
