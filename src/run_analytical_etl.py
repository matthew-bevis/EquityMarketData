from ingestion import get_spark_session
from analytical_etl import run_analytical_etl
from dotenv import load_dotenv
from tracker import Tracker
import os, sys, configparser, datetime

def main(trade_date: str):
    load_dotenv()

    # Load config.ini
    cfg = configparser.ConfigParser()
    cfg.read("config/config.ini")

    # Create tracker for analytical_etl
    tracker = Tracker("analytical_etl", cfg)

    # Prevent parallel runs
    if tracker.is_job_running():
        print("Analytical ETL already running, aborting.")
        sys.exit(1)

    # Dependency check: ingestion must be successful first
    ingestion_status = tracker.get_job_status("data_ingestion")
    if ingestion_status != "success":
        print("Cannot run analytics: ingestion did not succeed.")
        sys.exit(1)

    # Mark this job as running
    tracker.update_job_status("running")

    try:
        spark = get_spark_session()
        account = os.getenv("AZURE_STORAGE_ACCOUNT")
        base_path = f"wasbs://spring-capital@{account}.blob.core.windows.net"

        run_analytical_etl(spark, trade_date, base_path)

        tracker.update_job_status("success")
        print("Analytical ETL completed successfully.")
    except Exception as e:
        print("Analytical ETL failed:", e)
        tracker.update_job_status("failed")
        sys.exit(1)
    finally:
        if "spark" in locals():
            spark.stop()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        trade_date = sys.argv[1]
    else:
        trade_date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    main(trade_date)


