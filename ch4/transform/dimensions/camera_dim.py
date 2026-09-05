from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session


def transform_camera_dim(camera_df, location_df):
    get_spark_session()

    combined_df = (
        camera_df.alias("c")
        .join(location_df.alias("l"), col("c.location_fk") == col("l.id"), "left")
        .select(
            col("c.code").alias("code"),
            col("c.name").alias("name"),
            col("c.device").alias("device"),
            col("l.street").alias("street"),
            col("l.house_number").alias("house_number"),
            col("l.postal_code").alias("postal_code"),
            col("l.city").alias("city"),
            col("l.county").alias("county"),
            col("l.country").alias("country"),
            col("c.lat").alias("lat"),
            col("c.lon").alias("lon"),
        )
        .dropDuplicates(["code"])
    )

    window = Window.orderBy("code")
    combined_df = combined_df.withColumn("camera_tk", row_number().over(window))

    return combined_df.select(
        "camera_tk", "code", "name", "device", "street", "house_number",
        "postal_code", "city", "county", "country", "lat", "lon",
    ).orderBy("camera_tk")
