from functools import reduce
from pyspark.sql.functions import col, count, hour, minute, row_number, to_date
from pyspark.sql.window import Window
from spark_session import get_spark_session
from transform.dimensions.conditions_dim import categorize_conditions

FLAT_COLUMNS = [
    "motion_id", "ts", "camera_id", "cause", "peak_mass", "dur_ms", "schema_v",
    "weather_code", "temperature", "precipitation_mm", "lighting", "road_condition",
    "vehicle_class", "brand", "model", "color", "body", "era", "direction",
    "headlights_on", "commercial_markings", "special_use", "truncated", "plate_visible",
]


# Spajanje otporno na NULL. Obična jednakost nad NULL vrijednostima ne daje
# istinu, pa bi retci s nepoznatim atributima ostali bez surogat ključa.
def _null_safe_join(left, right, columns):
    condition = reduce(lambda a, b: a & b, [left[c].eqNullSafe(right[c]) for c in columns])
    return left.join(right, condition, "left")


def _from_postgres(tables):
    return (
        tables["vehicle"].alias("v")
        .join(tables["inference"].alias("i"), col("v.inference_fk") == col("i.id"))
        .join(tables["motion"].alias("m"), col("i.motion_fk") == col("m.id"))
        .join(tables["weather_observation"].alias("w"), col("m.weather_observation_fk") == col("w.id"))
        .join(tables["camera"].alias("c"), col("m.camera_fk") == col("c.id"))
        .join(tables["vlm"].alias("n"), col("i.vlm_fk") == col("n.id"))
        .join(tables["class"].alias("k"), col("v.class_fk") == col("k.id"), "left")
        .join(tables["brand"].alias("b"), col("v.brand_fk") == col("b.id"), "left")
        .join(tables["color"].alias("o"), col("v.color_fk") == col("o.id"), "left")
        .select(
            col("m.id").alias("motion_id"),
            col("m.ts").alias("ts"),
            col("c.code").alias("camera_id"),
            col("m.cause").alias("cause"),
            col("m.peak_mass").alias("peak_mass"),
            col("m.dur_ms").alias("dur_ms"),
            col("n.schema_v").alias("schema_v"),
            col("w.weather_code").alias("weather_code"),
            col("w.temperature").alias("temperature"),
            col("w.precipitation_mm").alias("precipitation_mm"),
            col("m.lighting").alias("lighting"),
            col("i.road_condition").alias("road_condition"),
            col("k.name").alias("vehicle_class"),
            col("b.name").alias("brand"),
            col("v.model").alias("model"),
            col("o.name").alias("color"),
            col("v.body").alias("body"),
            col("v.era").alias("era"),
            col("v.direction").alias("direction"),
            col("v.headlights_on").alias("headlights_on"),
            col("v.commercial_markings").alias("commercial_markings"),
            col("v.special_use").alias("special_use"),
            col("v.truncated").alias("truncated"),
            col("v.plate_visible").alias("plate_visible"),
        )
    )


def _from_csv(csv_df):
    return csv_df.select(
        col("motion_id"), col("ts"), col("camera_id"), col("cause"),
        col("peak_mass"), col("dur_ms"), col("schema_v"), col("weather_code"),
        col("temp_c").alias("temperature"),
        col("precip_mm").alias("precipitation_mm"),
        col("lighting"), col("road_condition"), col("vehicle_class"),
        col("make").alias("brand"),
        col("model"), col("color"), col("body"), col("era"), col("direction"),
        col("headlights_on"), col("commercial_markings"), col("special_use"),
        col("truncated"), col("plate_visible"),
    )


def transform_vehicle_detection_fact(tables, date_dim, time_dim, camera_dim,
                                     vehicle_dim, conditions_dim, vlm_dim,
                                     observation_dim, csv_df=None):
    get_spark_session()

    # Oba izvora najprije se svode na isti spljošteni oblik i tek se onda
    # spajaju; dimenzije se traže nad ujedinjenim skupom.
    flat_df = _from_postgres(tables).select(*FLAT_COLUMNS)

    if csv_df:
        flat_df = flat_df.unionByName(_from_csv(csv_df).select(*FLAT_COLUMNS))

    flat_df = (
        flat_df
        .withColumn("full_date", to_date(col("ts")))
        .withColumn("hour", hour(col("ts")))
        .withColumn("minute", minute(col("ts")))
        .withColumn("objects_in_event", count("*").over(Window.partitionBy("motion_id")))
    )

    flat_df = categorize_conditions(flat_df)

    joined = (
        flat_df
        .join(date_dim, ["full_date"], "left")
        .join(time_dim, ["hour", "minute"], "left")
        .join(camera_dim.select("camera_tk", col("code").alias("camera_id")), ["camera_id"], "left")
        .join(vlm_dim.select("vlm_tk", "schema_v"), ["schema_v"], "left")
    )

    joined = _null_safe_join(
        joined,
        vehicle_dim.select("vehicle_tk", "vehicle_class", "brand", "model", "color", "body", "era"),
        ["vehicle_class", "brand", "model", "color", "body", "era"],
    )
    joined = _null_safe_join(
        joined,
        conditions_dim,
        ["weather_category", "temperature_bucket", "precipitation_bucket", "lighting"],
    )
    joined = _null_safe_join(
        joined,
        observation_dim,
        ["truncated", "plate_visible", "headlights_on", "commercial_markings",
         "cause", "direction", "special_use", "road_condition"],
    )

    window = Window.orderBy("motion_id", "vehicle_tk")
    joined = joined.withColumn("fact_vehicle_detection_tk", row_number().over(window))

    return joined.select(
        "fact_vehicle_detection_tk", "date_tk", "time_tk", "camera_tk", "vehicle_tk",
        "conditions_tk", "vlm_tk", "observation_tk", "motion_id",
        "peak_mass", "dur_ms", "objects_in_event",
    ).orderBy("fact_vehicle_detection_tk")
