import argparse
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BUCKET = 'de-course'
INT_PATH = f"s3a://{BUCKET}/intermediate/int_orders_with_payments"
FCT_ORDERS_PATH = f"s3a://{BUCKET}/marts/fct_orders"


spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # перезапись только затронутых партиций
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

parser = argparse.ArgumentParser()
parser.add_argument("--load-date", required=True, help="Дата запуска загрузки, ГГГГ-ММ-ДД")
args = parser.parse_args()

load_date = datetime.strptime(args.load_date, "%Y-%m-%d").date()
start_date = load_date - timedelta(days=3)

df_int = (
    spark.read.parquet(INT_PATH)
    .filter((F.col("order_date") >= start_date) & (F.col("order_date") < load_date))
)

df_fct_orders = (
    df_int
    .filter(~F.col("is_test_order"))
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("order_date"),
        F.col("ordered_at"),
        F.col("order_status"),
        F.col("order_amount"),
        F.col("paid_amount"),
        F.col("refunded_amount"),
        F.col("net_paid_amount"),
        F.col("successful_payments_count"),
        F.col("first_paid_at"),
        F.col("is_confirmed"),
    )
)

# dynamic overwrite перезапишет только партиции текущего окна - идемпотентно
(
    df_fct_orders.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("order_date")
    .save(FCT_ORDERS_PATH)
)

print(f"Сохранено: {FCT_ORDERS_PATH}")
