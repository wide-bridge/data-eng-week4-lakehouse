# scripts/etl_orders.py — 랜딩 존 → 정제 → MERGE 적재
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("orders-etl").getOrCreate()

# 1) 랜딩 존(raw 버킷)에서 원천 읽기 (s3a)
raw = spark.read.option("header", "true").csv("s3a://raw/orders/")
print("원천 행:", raw.count())

# 2) 형 변환
typed = raw.select(
    F.col("order_id").cast("bigint").alias("order_id"),
    F.col("customer"),
    F.col("amount").cast("double").alias("amount"),
    F.to_date("order_date").alias("order_date"),
    F.col("channel"),
)

# 3) 정제: 불량 제거 + 최신 날짜 우선. 같은 날짜는 나머지 값으로 안정적으로 선택.
w = Window.partitionBy("order_id").orderBy(F.col("order_date").desc(), F.col("customer").asc_nulls_last(), F.col("amount").desc(), F.col("channel").asc_nulls_last())
clean = (
    typed
    .filter("order_id IS NOT NULL AND order_date IS NOT NULL AND amount >= 0")
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)
print("정제 후 행:", clean.count())
clean.createOrReplaceTempView("updates")

# 4) 적재: 키가 있으면 갱신, 없으면 삽입 (멱등)
spark.sql("""
    MERGE INTO demo.lakehouse.orders AS t
    USING updates AS s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN UPDATE SET customer = s.customer, amount = s.amount, order_date = s.order_date, channel = s.channel
    WHEN NOT MATCHED THEN INSERT (order_id, customer, amount, order_date, channel) VALUES (s.order_id, s.customer, s.amount, s.order_date, s.channel)
""")

print("total rows:", spark.table("demo.lakehouse.orders").count())
spark.stop()
