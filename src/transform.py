from src.parsers.csv_parser import parse_csv
from src.parsers.json_parser import parse_json
from src.schema import common_event_schema
from dotenv import load_dotenv
import os

def transform_data(spark, storage_account_name):
    load_dotenv()

    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    base_path = f"wasbs://spring-capital@{account}.blob.core.windows.net"

    raw_csv = spark.read.text(f"{base_path}/csv/*/*").rdd.map(lambda row: row[0])
    parsed_csv = raw_csv.map(parse_csv)

    raw_json = spark.read.text(f"{base_path}/json/*/*").rdd.map(lambda row: row[0])
    parsed_json = raw_json.map(parse_json)

    all_data = parsed_csv.union(parsed_json)
    df = spark.createDataFrame(all_data, schema=common_event_schema)
    return df


