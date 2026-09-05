from pyspark.sql import DataFrame

JDBC_URL = "jdbc:postgresql://localhost:5432/srp-traffic-analysis"
CONNECTION_PROPERTIES = {
    "user": "adisspahic",
    "driver": "org.postgresql.Driver",
}


# Potpuno punjenje: tablica se upisuje odjednom, a ne redak po redak.
def write_spark_df_to_postgres(spark_df: DataFrame, table_name: str, mode: str = "append"):
    print(f"Writing to table `{table_name}` with mode `{mode}`...")

    spark_df.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode=mode,
        properties=CONNECTION_PROPERTIES,
    )
    print(f"Done writing to `{table_name}`.")
