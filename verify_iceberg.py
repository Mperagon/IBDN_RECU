"""
Verifica que la tabla Iceberg en MinIO es legible.
spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 verify_iceberg.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("VerifyIceberg") \
    .master("local[*]") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.minio_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_catalog.warehouse",
            "s3a://flight-data/iceberg") \
    .config("spark.hadoop.fs.s3a.endpoint",        "http://127.0.0.1:9000") \
    .config("spark.hadoop.fs.s3a.access.key",      "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key",      "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

count = spark.sql("SELECT COUNT(*) as total FROM minio_catalog.flight_features").collect()[0]["total"]
print(f"\n=== Filas en minio_catalog.flight_features: {count} ===\n")

spark.sql("SELECT Carrier, Origin, Dest, DepDelay, ArrDelay FROM minio_catalog.flight_features LIMIT 5").show()

spark.stop()
