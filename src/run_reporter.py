from ingestion import get_spark_session
from eod_load import process_partition
from dotenv import load_dotenv
from tracker import Tracker
import os, sys

def run_analytics(spark):
    for partition_type in ["T", "Q"]:
        process_partition(spark, partition_type)

def main(config):
    load_dotenv()
    spark = get_spark_session()

    tracker = Tracker("analytical_etl", config)

    if tracker.is_job_running():
        print("Analytical ETL already running. Exiting.")
        sys.exit(1)

    # Dependency check: ingestion must be successful
    ingestion_status = tracker.get_job_status("data_ingestion")
    if ingestion_status != "success":
        print("Cannot run analytics: ingestion did not succeed.")
        sys.exit(1)

    tracker.update_job_status("running")
    try:
        run_analytics(spark)
        tracker.update_job_status("success")
        print("Analytical ETL completed successfully.")
    except Exception as e:
        print("Analytical ETL failed:", e)
        tracker.update_job_status("failed")
        sys.exit(1)

if __name__ == "__main__":
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read("config/config.ini")
    main(cfg)
