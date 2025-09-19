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
        .config("spark.jars", jars_str)
        .config("spark.hadoop.fs.azure.account.key.{}.blob.core.windows.net".format(account), key)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.fs.azure.committer.name", "directory")
        .config("spark.hadoop.fs.azure.createRemoteFileSystemDuringInitialization", "true")
        .config("spark.speculation", "false")
    )

    spark = builder.getOrCreate()
    return spark