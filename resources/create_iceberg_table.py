"""
Crea la tabla Iceberg flight_features en MinIO a partir de los datos de entrenamiento.
Ejecutado en K8s via spark-submit (deploy-mode cluster). Las credenciales S3A
se inyectan desde el spark-submit mediante --conf spark.hadoop.fs.s3a.*.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

spark = SparkSession.builder \
    .appName("CreateIcebergTable") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.minio_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_catalog.warehouse",
            "s3a://flight-data/iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("ArrDelay",   DoubleType(),  True),
    StructField("Carrier",    StringType(),  True),
    StructField("DayOfMonth", IntegerType(), True),
    StructField("DayOfWeek",  IntegerType(), True),
    StructField("DayOfYear",  IntegerType(), True),
    StructField("DepDelay",   DoubleType(),  True),
    StructField("Dest",       StringType(),  True),
    StructField("Distance",   DoubleType(),  True),
    StructField("FlightDate", StringType(),  True),
    StructField("FlightNum",  StringType(),  True),
    StructField("Origin",     StringType(),  True),
])

print("Leyendo datos de entrenamiento desde MinIO...")
df = spark.read.json(
    "s3a://flight-data/raw/simple_flight_delay_features.jsonl.bz2",
    schema=schema,
)
count = df.count()
print(f"Filas leidas: {count}")

print("Escribiendo tabla Iceberg en MinIO...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS minio_catalog")
df.writeTo("minio_catalog.flight_features") \
  .tableProperty("write.format.default", "parquet") \
  .createOrReplace()

spark.sql("SELECT COUNT(*) AS total FROM minio_catalog.flight_features").show()
spark.sql(
    "SELECT Carrier, Origin, Dest, DepDelay, ArrDelay "
    "FROM minio_catalog.flight_features LIMIT 5"
).show()

print(f"\nOK - Tabla Iceberg creada en s3a://flight-data/iceberg/flight_features")
spark.stop()
