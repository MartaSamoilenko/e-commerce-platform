import json, os, uuid, logging
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType)

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

spark = (
    SparkSession.builder
    .appName("sales-stream-agg")
    .config("spark.sql.shuffle.partitions", "1")         
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = (
    StructType()
    .add("sale_id",  StringType())   # UUID as str
    .add("product_id", StringType())
    .add("quantity", IntegerType())
    .add("ts", StringType())
    .add("user_id", StringType())
    .add("store_id", StringType())
)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
PUSHGW            = os.getenv("PUSHGATEWAY", "pushgateway:9091")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "sales")
    .option("startingOffsets", "earliest")
    .load()
)

sales = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("event_time",
                    F.to_timestamp("ts").cast(TimestampType()))
)

windowed = (
    sales
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        F.window("event_time", "1 minute").alias("w"),
        "product_id",
        "store_id",
    )
    .agg(
        F.count("*").alias("sales_count"),
        F.sum("quantity").alias("total_qty")
    )
)


def push_metrics(batch_df, batch_id):
    """
    Called for every micro-batch (~5 s). Converts the current 1-min window
    aggregations to Prometheus metrics and pushes them to the gateway.
    """
    registry = CollectorRegistry()

    g_sales = Gauge(
        "ecommerce_sales_count_window",
        "Sales count per product & store (1-min window)",
        ["product_id", "store_id"],
        registry=registry)
    g_qty = Gauge(
        "ecommerce_quantity_window",
        "Quantity per product & store (1-min window)",
        ["product_id", "store_id"],
        registry=registry)

    rows = (
        batch_df
        .select("product_id", "store_id", "sales_count", "total_qty")
        .collect()
    )

    for r in rows:
        g_sales.labels(r.product_id, r.store_id).set(r.sales_count)
        g_qty.labels(r.product_id, r.store_id).set(r.total_qty)

    group_key = datetime.utcnow().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())[:8]

    push_to_gateway(PUSHGW, job="spark_sales_stream", grouping_key={"window": group_key}, registry=registry)
    print(f"Pushed {len(rows)} window rows to Pushgateway")


query = (
    windowed.writeStream
    .outputMode("update")                 #
    .trigger(processingTime="5 seconds")
    .foreachBatch(push_metrics)
    .start()
)

query.awaitTermination()
