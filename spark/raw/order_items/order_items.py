# Задача 1.5.
#
# Инкрементальная загрузка по окну невозможна, так как в таблице нет
# колонки с датой. Окно [LOAD_DATE - 3 дня; LOAD_DATE) строится по дате, а здесь
# её нет, поэтому нельзя понять, какие строки новые или изменённые. Поэтому
# таблицу каждый раз перечитываем целиком и перезаписываем без partitionBy.


import os

from pyspark.sql import SparkSession


PG = dict(
    host='postgres',
    port=5432,
    dbname=os.environ.get('POSTGRES_DB'),
    user=os.environ.get('POSTGRES_USER'),
    password=os.environ.get('POSTGRES_PASSWORD'),
)

BUCKET = 'de-course'
ORDER_ITEMS_PATH = f"s3a://{BUCKET}/raw/order_items"

spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get('MINIO_ENDPOINT'))
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get('MINIO_ROOT_USER'))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get('MINIO_ROOT_PASSWORD'))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

jdbc = dict(
    url=f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}",
    user=PG['user'],
    password=PG['password'],
    driver='org.postgresql.Driver',
)

# Полный снимок справочника
query = """
    SELECT order_id
         , product_id
         , quantity
         , unit_price
         , discount_amount
    FROM public.order_items
"""

df_order_items = (
    spark.read.format("jdbc")
    .option("query", query)
    .options(**jdbc)
    .load()
)

# Полная перезапись: overwrite без partitionBy
(
    df_order_items.write
    .format("parquet")
    .mode("overwrite")
    .save(ORDER_ITEMS_PATH)
)

print(f"Сохранено: {ORDER_ITEMS_PATH}, строк: {df_order_items.count()}")
