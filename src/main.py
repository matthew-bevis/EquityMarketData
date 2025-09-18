from src.ingestion import get_spark_session
from src.parsers.csv_parser import parse_csv
from src.parsers.json_parser import parse_json
from src.schema import common_event_schema
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    spark = get_spark_session()
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    print("Loaded key:", spark._jsc.hadoopConfiguration().get("fs.azure.account.key.{account}.blob.core.windows.net"))
    raw_csv = spark.sparkContext.textFile(
        "wasbs://spring-capital@{account}.blob.core.windows.net/csv/*/*"
    )
    parsed_csv = raw_csv.map(parse_csv)

    raw_json = spark.sparkContext.textFile(
        "wasbs://spring-capital@{account}.blob.core.windows.net/json/*/*"
    )
    parsed_json = raw_json.map(parse_json)

    all_data = parsed_csv.union(parsed_json)
    df = spark.createDataFrame(all_data, schema=common_event_schema)

    df.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

if __name__ == "__main__":
    main()
