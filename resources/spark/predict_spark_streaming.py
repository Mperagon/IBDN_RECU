"""
Spark Structured Streaming — predicción de retrasos de vuelos.
Lee de Kafka topic 'flight-delay-ml-request', predice con el modelo sklearn
almacenado en Iceberg (minio_catalog.models) y publica en 'spark-flight-predictions'.
"""
import os
import io
import joblib

KAFKA_BROKER   = os.environ.get('KAFKA_BROKER',          'kafka:9092')
MINIO_ENDPOINT = os.environ.get('MLFLOW_S3_ENDPOINT_URL', 'http://minio:9000')
MINIO_ACCESS   = os.environ.get('AWS_ACCESS_KEY_ID',      'minioadmin')
MINIO_SECRET   = os.environ.get('AWS_SECRET_ACCESS_KEY',  'minioadmin')
INPUT_TOPIC    = 'flight-delay-ml-request'
OUTPUT_TOPIC   = 'spark-flight-predictions'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, udf
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

spark = SparkSession.builder \
    .appName("SparkStreamingPredict") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.minio_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_catalog.warehouse",
            "s3a://flight-data/iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint",            MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key",          MINIO_ACCESS) \
    .config("spark.hadoop.fs.s3a.secret.key",          MINIO_SECRET) \
    .config("spark.hadoop.fs.s3a.path.style.access",   "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Leer path del modelo desde Iceberg y descargar el fichero desde MinIO con boto3
print("Leyendo path del modelo desde minio_catalog.models...")
_rows = (
    spark.read.format("iceberg")
    .table("minio_catalog.models")
    .filter("model_name = 'sklearn_random_forest'")
    .orderBy("trained_at", ascending=False)
    .limit(1)
    .collect()
)
if not _rows:
    raise RuntimeError("No se encontró modelo sklearn en minio_catalog.models. "
                       "Ejecuta primero el DAG de entrenamiento.")

import boto3 as _boto3
_s3c = _boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
)
_buf = io.BytesIO()
_s3c.download_fileobj("flight-data", "models/sklearn_random_forest.joblib", _buf)
_buf.seek(0)
model = joblib.load(_buf)
print("Modelo sklearn cargado desde MinIO ({}) OK".format(_rows[0]["model_path"]))

model_bc = spark.sparkContext.broadcast(model)

msg_schema = StructType([
    StructField("UUID",       StringType(),  True),
    StructField("Carrier",    StringType(),  True),
    StructField("Origin",     StringType(),  True),
    StructField("Dest",       StringType(),  True),
    StructField("DepDelay",   DoubleType(),  True),
    StructField("Distance",   DoubleType(),  True),
    StructField("DayOfMonth", IntegerType(), True),
    StructField("DayOfWeek",  IntegerType(), True),
    StructField("DayOfYear",  IntegerType(), True),
    StructField("Timestamp",  StringType(),  True),
])

@udf(returnType=IntegerType())
def predict_udf(carrier, origin, dest, dep_delay, distance,
                day_of_month, day_of_week, day_of_year):
    import pandas as pd
    m = model_bc.value
    carrier    = carrier    or "AA"
    origin     = origin     or "LAX"
    dest       = dest       or "JFK"
    dep_delay  = dep_delay  if dep_delay  is not None else 0.0
    distance   = distance   if distance   is not None else 500.0
    day_of_month = day_of_month if day_of_month is not None else 1
    day_of_week  = day_of_week  if day_of_week  is not None else 1
    day_of_year  = day_of_year  if day_of_year  is not None else 1
    X = pd.DataFrame([{
        "Carrier":    carrier,
        "Origin":     origin,
        "Dest":       dest,
        "Route":      f"{origin}-{dest}",
        "DepDelay":   dep_delay,
        "Distance":   distance,
        "DayOfMonth": day_of_month,
        "DayOfWeek":  day_of_week,
        "DayOfYear":  day_of_year,
    }])
    return int(m.predict(X)[0])

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw.select(
    from_json(col("value").cast("string"), msg_schema).alias("d")
).select("d.*")

predicted = parsed.withColumn(
    "Prediction",
    predict_udf(
        col("Carrier"), col("Origin"), col("Dest"),
        col("DepDelay"), col("Distance"),
        col("DayOfMonth"), col("DayOfWeek"), col("DayOfYear"),
    )
)

output = predicted.select(
    to_json(struct(
        col("UUID"),
        col("Prediction"),
        col("Origin"),
        col("Dest"),
        col("Carrier"),
        col("DepDelay"),
        col("Timestamp"),
    )).alias("value")
)

query = (
    output.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", "s3a://flight-data/spark-streaming-checkpoint")
    .start()
)

query.awaitTermination()
