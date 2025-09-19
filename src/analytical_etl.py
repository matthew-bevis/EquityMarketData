import os, shutil, datetime
from pyspark.sql import functions as F, Window
from eod_load import upload_to_blob

def run_analytical_etl(spark, trade_date: str, base_path: str, output_container="spring-capital"):
    trades = spark.read.parquet(f"{base_path}/trade/trade_dt={trade_date}")
    quotes = spark.read.parquet(f"{base_path}/quote/trade_dt={trade_date}")

    trades_mv = trades.select("symbol", "exchange", "event_tm", "event_seq_nb", "trade_pr")
    window_spec = (
        Window.partitionBy("symbol")
              .orderBy(F.col("event_tm").cast("long"))
              .rangeBetween(-1800, 0)   # 30 min window
    )
    trades_mv = trades_mv.withColumn("mov_avg_pr", F.avg("trade_pr").over(window_spec))

    prev_date = (
        datetime.datetime.strptime(trade_date, "%Y-%m-%d")
        - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")

    trades_prev_last = None
    prev_path = f"{base_path}/trade/trade_dt={prev_date}"
    try:
        trades_prev = spark.read.parquet(prev_path)
        window_last = Window.partitionBy("symbol", "exchange").orderBy(F.col("event_tm").desc())
        trades_prev_last = (
            trades_prev.withColumn("rn", F.row_number().over(window_last))
                       .filter("rn = 1")
                       .drop("rn", "event_seq_nb", "event_tm")
                       .withColumnRenamed("trade_pr", "close_pr")
                       .select("symbol", "exchange", "close_pr")
        )
    except Exception:
        print(f"No prior-day data found at {prev_path}, skipping close price join")

    quote_union = quotes.selectExpr(
        "trade_dt", "'Q' as rec_type", "symbol", "exchange", "event_tm",
        "event_seq_nb", "bid_pr", "bid_size", "ask_pr", "ask_size",
        "NULL as trade_pr", "NULL as mov_avg_pr"
    ).union(
        trades_mv.selectExpr(
            f"'{trade_date}' as trade_dt", "'T' as rec_type",
            "symbol", "exchange", "event_tm", "event_seq_nb",
            "NULL as bid_pr", "NULL as bid_size", "NULL as ask_pr", "NULL as ask_size",
            "trade_pr", "mov_avg_pr"
        )
    )

    # Carry forward last trade_pr + mov_avg_pr
    window_ffill = (
        Window.partitionBy("symbol", "exchange")
              .orderBy("event_tm", "event_seq_nb")
              .rowsBetween(Window.unboundedPreceding, 0)
    )
    quote_union_update = (
        quote_union.withColumn("last_trade_pr", F.last("trade_pr", True).over(window_ffill))
                   .withColumn("last_mov_avg_pr", F.last("mov_avg_pr", True).over(window_ffill))
    )

    # Keep only quotes
    quote_update = quote_union_update.filter("rec_type = 'Q'").select(
        "trade_dt", "symbol", "event_tm", "event_seq_nb", "exchange",
        "bid_pr", "bid_size", "ask_pr", "ask_size",
        "last_trade_pr", "last_mov_avg_pr"
    )

    # Join with prior-day close if available
    if trades_prev_last:
        quote_final = (
            quote_update.join(
                F.broadcast(trades_prev_last),
                on=["symbol", "exchange"],
                how="left"
            )
            .withColumn("bid_pr_mv", F.col("bid_pr") - F.col("close_pr"))
            .withColumn("ask_pr_mv", F.col("ask_pr") - F.col("close_pr"))
        )
    else:
        quote_final = (
            quote_update.withColumn("bid_pr_mv", F.lit(None))
                        .withColumn("ask_pr_mv", F.lit(None))
        )

    unique_keys = ["trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb"]
    window_spec_dedup = Window.partitionBy(*unique_keys).orderBy(F.desc("event_tm"))
    deduped_df = quote_final.withColumn("rn", F.row_number().over(window_spec_dedup)) \
                            .filter(F.col("rn") == 1).drop("rn")

    trade_dates = [row["trade_dt"] for row in deduped_df.select("trade_dt").distinct().collect()]
    print(f"Detected trade_dates in analytical ETL: {trade_dates}")

    for date in trade_dates:
        local_output_dir = f"/tmp/analytical_data/{date}"
        if os.path.exists(local_output_dir):
            shutil.rmtree(local_output_dir)

        filtered = deduped_df.filter(F.col("trade_dt") == date).coalesce(1)
        print(f"Writing analytical quotes for {date} locally to: {local_output_dir}")
        filtered.write.mode("overwrite").parquet(local_output_dir)

        blob_prefix = f"quote-trade-analytical/trade_dt={date}"
        print(f"Uploading analytical data to Azure Blob: {blob_prefix}")
        upload_to_blob(local_output_dir, output_container, blob_prefix)

        shutil.rmtree(local_output_dir)  # cleanup

    return deduped_df
