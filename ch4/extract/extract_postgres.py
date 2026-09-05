from spark_session import get_spark_session

JDBC_URL = "jdbc:postgresql://localhost:5432/srp-traffic-analysis"
CONNECTION_PROPERTIES = {
    "user": "adisspahic",
    "driver": "org.postgresql.Driver",
}


def extract_table(table_name):
    spark = get_spark_session("ETL_App")

    df = spark.read.jdbc(
        url=JDBC_URL,
        table=f"public.{table_name}",
        properties=CONNECTION_PROPERTIES,
    )
    return df


# Potpuno izvlačenje: sve tablice izvora čitaju se odjednom u pripremno
# područje, bez uvjeta i bez pisanja natrag u izvorišni sustav.
def extract_all_tables():
    return {
        "location": extract_table("location"),
        "camera": extract_table("camera"),
        "vlm": extract_table("vlm"),
        "weather_observation": extract_table("weather_observation"),
        "motion": extract_table("motion"),
        "inference": extract_table("inference"),
        "class": extract_table("class"),
        "brand": extract_table("brand"),
        "color": extract_table("color"),
        "vehicle": extract_table("vehicle"),
    }
