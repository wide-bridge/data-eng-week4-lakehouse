# scripts/etl_bad.py — 일부러 중복을 안 줄인 버전
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("orders-etl-bad").getOrCreate()

raw = spark.read.option("header", "true").csv("s3a://raw/orders/")
clean = raw.select(
    F.col("order_id").cast("bigint").alias("order_id"),
    F.col("customer"),
    F.col("amount").cast("double").alias("amount"),
    F.to_date("order_date").alias("order_date"),
    F.col("channel"),
)
clean.createOrReplaceTempView("updates")

spark.sql("""
    MERGE INTO demo.lakehouse.orders AS t
    USING updates AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET customer = s.customer, amount = s.amount, order_date = s.order_date, channel = s.channel
    WHEN NOT MATCHED THEN INSERT (order_id, customer, amount, order_date, channel) VALUES (s.order_id, s.customer, s.amount, s.order_date, s.channel)
""")
spark.stop()
