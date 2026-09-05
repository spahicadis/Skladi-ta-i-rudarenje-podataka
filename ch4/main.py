from extract.extract_csv import extract_from_csv
from extract.extract_postgres import extract_all_tables
from load.run_loading import write_spark_df_to_postgres
from spark_session import get_spark_session
from transform.pipeline import run_transformations

CSV_FILE_PATH = "../ch2/processed/Traffic_Analysis_Novigrad_2026-08-20_2026-08-31_PROCESSED_20.csv"


# ETL u tri faze. Oba izvora, baza i CSV, spajaju se u jedan rječnik prije
# transformacije, pa dalje kroz proces putuju kao jedan skup.
def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    spark.catalog.clearCache()

    print("Starting data extraction")
    postgres_df = extract_all_tables()
    csv_df = {"csv_detections": extract_from_csv(CSV_FILE_PATH)}
    merged_df = {**postgres_df, **csv_df}
    print("Data extraction completed")

    print("Starting data transformation")
    load_ready_dict = run_transformations(merged_df)
    print("Data transformation completed")

    print("Starting data loading")
    for table_name, df in load_ready_dict.items():
        write_spark_df_to_postgres(df, table_name)
    print("Data loading completed")


if __name__ == "__main__":
    main()
