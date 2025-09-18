from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, DecimalType
)

common_event_schema = StructType([
    StructField("trade_dt", StringType(), True),
    StructField("rec_type", StringType(), True),   # T = Trade, Q = Quote, B = Bad
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("event_tm", TimestampType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("arrival_tm", TimestampType(), True),
    StructField("trade_pr", DecimalType(18,4), True),
    StructField("bid_pr", DecimalType(18,4), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", DecimalType(18,4), True),
    StructField("ask_size", IntegerType(), True),
    StructField("partition", StringType(), True)   # Partition key
])
