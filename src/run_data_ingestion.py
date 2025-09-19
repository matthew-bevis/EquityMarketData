from ingestion import get_spark_session
from parsers.csv_parser import parse_csv
from parsers.json_parser import parse_json
from schema import common_event_schema
from dotenv import load_dotenv
from tracker import Tracker
from eod_load import process_partition
import os, sys

def run_ingestion(spark, account):
    raw_csv = spark.sparkContext.textFile(
        f"wasbs://spring-capital@{account}.blob.core.windows.net/csv/*/*"
    )
    parsed_csv = raw_csv.map(parse_csv)

    raw_json = spark.sparkContext.textFile(
        f"wasbs://spring-capital@{account}.blob.core.windows.net/json/*/*"
    )
    parsed_json = raw_json.map(parse_json)

    all_data = parsed_csv.union(parsed_json)
    df = spark.createDataFrame(all_data, schema=common_event_schema)

    # Write to local staging dir
    df.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

    # Process partitions (T, Q) and upload to Azure blob
    print("Processing EOD load...")
    for partition_type in ["T", "Q"]:
        process_partition(spark, partition_type)

def main(config):
    load_dotenv()
    spark = get_spark_session()
    account = os.getenv("AZURE_STORAGE_ACCOUNT")

    tracker = Tracker("data_ingestion", config)

    if tracker.is_job_running():
        print("Ingestion job already running. Exiting.")
        sys.exit(1)

    tracker.update_job_status("running")
    try:
        run_ingestion(spark, account)
        tracker.update_job_status("success")
        print("Ingestion completed successfully.")
    except Exception as e:
        print("Ingestion failed:", e)
        tracker.update_job_status("failed")
        sys.exit(1)
    finally:
        if "spark" in locals():
            spark.stop()

if __name__ == "__main__":
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read("config/config.ini")
    main(cfg)

