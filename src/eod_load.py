import os
import shutil
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from azure.storage.blob import BlobServiceClient, ContentSettings

def apply_latest(df, unique_keys):
    window_spec = Window.partitionBy(*unique_keys).orderBy(F.desc("arrival_tm"))
    return df.withColumn("rn", F.row_number().over(window_spec)) \
             .filter(F.col("rn") == 1) \
             .drop("rn")

def upload_to_blob(local_dir, container_name, blob_prefix):
    load_dotenv()
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    container = blob_service.get_container_client(container_name)

    for root, _, files in os.walk(local_dir):
        for file in files:
            file_path = os.path.join(root, file)
            blob_path = os.path.join(blob_prefix, file).replace("\\", "/")
            print(f"Uploading: {file_path} -> {blob_path}")
            with open(file_path, "rb") as data:
                container.upload_blob(
                    name=blob_path,
                    data=data,
                    overwrite=True,
                    content_settings=ContentSettings(content_type="application/octet-stream")
                )

def process_partition(spark, partition_type: str, output_container: str = "spring-capital"):
    load_dotenv()
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    input_path = f"output_dir/partition={partition_type}"
    print(f"Processing partition: {partition_type} from {input_path}")

    df = spark.read.parquet(input_path)

    if partition_type == "T":
        selected = df.select("trade_dt", "symbol", "exchange", "event_tm",
                             "event_seq_nb", "arrival_tm", "trade_pr")
        base_dir = "trade"
    elif partition_type == "Q":
        selected = df.select("trade_dt", "symbol", "exchange", "event_tm",
                             "event_seq_nb", "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size")
        base_dir = "quote"
    else:
        print(f"Skipping unsupported partition type: {partition_type}")
        return

    unique_keys = ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb"]
    deduped_df = apply_latest(selected, unique_keys)

    trade_dates = [row["trade_dt"] for row in deduped_df.select("trade_dt").distinct().collect()]
    print(f"Detected trade_dates: {trade_dates}")

    for date in trade_dates:
        local_output_dir = f"/tmp/eod_data/{partition_type}/{date}"
        if os.path.exists(local_output_dir):
            shutil.rmtree(local_output_dir)

        filtered = deduped_df.filter(F.col("trade_dt") == date).coalesce(1)
        print(f"Writing {partition_type} data for {date} locally to: {local_output_dir}")
        filtered.write.mode("overwrite").parquet(local_output_dir)

        blob_prefix = f"{base_dir}/trade_dt={date}"
        print(f"Uploading local data to Azure Blob: {blob_prefix}")
        upload_to_blob(local_output_dir, output_container, blob_prefix)

        shutil.rmtree(local_output_dir)  # clean up after upload
