import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

def get_spark_session():
    load_dotenv()

    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")

    if not account or not key:
        raise ValueError("Missing AZURE_STORAGE_ACCOUNT or AZURE_STORAGE_KEY")

    # Path to your JARs inside the project
    jars = [
        os.path.join("jars", "hadoop-azure-3.3.0.jar"),
        os.path.join("jars", "azure-storage-8.6.5.jar"),
    ]

    jars_str = ",".join(jars)

    builder = (
        SparkSession.builder
        .appName("EquityMarketData")
        # load Azure + Hadoop JARs into Spark
        .config("spark.jars", jars_str)
        # Azure storage credentials
        .config(
            f"spark.hadoop.fs.azure.account.key.{account}.blob.core.windows.net",
            key
        )
    )

    spark = builder.getOrCreate()
    return spark





