"""
Convierte los datos de entrenamiento (JSONL) a tabla Iceberg almacenada en MinIO.
Ejecutar UNA sola vez con:
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 create_iceberg_table.py
"""
import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin"
BASE_PATH      = "/app"

spark = SparkSession.builder \
    .appName("CreateIcebergTable") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.host", "spark-master") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.minio_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_catalog.warehouse",
            "s3a://flight-data/iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint",        MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",      MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",      MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ArrDelay",   DoubleType(),    True),
    StructField("CRSArrTime", TimestampType(), True),
    StructField("CRSDepTime", TimestampType(), True),
    StructField("Carrier",    StringType(),    True),
    StructField("DayOfMonth", IntegerType(),   True),
    StructField("DayOfWeek",  IntegerType(),   True),
    StructField("DayOfYear",  IntegerType(),   True),
    StructField("DepDelay",   DoubleType(),    True),
    StructField("Dest",       StringType(),    True),
    StructField("Distance",   DoubleType(),    True),
    StructField("FlightDate", DateType(),      True),
    StructField("FlightNum",  StringType(),    True),
    StructField("Origin",     StringType(),    True),
])

print("Leyendo datos locales...")
input_path = "{}/data/simple_flight_delay_features.jsonl.bz2".format(BASE_PATH)
df = spark.read.json(input_path, schema=schema)
count = df.count()
print(f"Filas leidas: {count}")

print("Escribiendo tabla Iceberg en MinIO...")
df.writeTo("minio_catalog.flight_features") \
  .tableProperty("write.format.default", "parquet") \
  .createOrReplace()

print("Verificando tabla Iceberg...")
result = spark.sql("SELECT COUNT(*) as total FROM minio_catalog.flight_features")
result.show()

spark.sql("SELECT Carrier, Origin, Dest, DepDelay, ArrDelay FROM minio_catalog.flight_features LIMIT 5").show()

print(f"\nOK - Tabla Iceberg creada en s3a://flight-data/iceberg/flight_features")
print(f"     {count} filas almacenadas en formato Parquet/Iceberg")
spark.stop()
