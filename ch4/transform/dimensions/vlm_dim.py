from pyspark.sql.functions import col, dense_rank, lead, row_number
from pyspark.sql.window import Window
from spark_session import get_spark_session


def transform_vlm_dim(vlm_df):
    get_spark_session()

    entity_window = Window.orderBy("model")
    version_window = Window.partitionBy("model").orderBy("valid_from")

    combined_df = (
        vlm_df
        .select("model", "schema_v", "prompt_v", "valid_from")
        .dropDuplicates(["model", "schema_v"])
        .withColumn("vlm_id", dense_rank().over(entity_window))
        .withColumn("version", row_number().over(version_window))
        .withColumn("date_from", col("valid_from"))
        .withColumn("date_to", lead("valid_from").over(version_window))
    )

    window = Window.orderBy("model", "valid_from")
    combined_df = combined_df.withColumn("vlm_tk", row_number().over(window))

    return combined_df.select(
        "vlm_tk", "version", "date_from", "date_to", "vlm_id", "model", "schema_v", "prompt_v"
    ).orderBy("vlm_tk")
