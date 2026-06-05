"""
Paso 1: Crear bucket en MinIO y subir datos de entrenamiento.
Los datos se subirán como Parquet (formato base de Iceberg).
"""
import os, sys
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = "127.0.0.1:9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin"
BUCKET         = "flight-data"

client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

# Crear bucket si no existe
if not client.bucket_exists(BUCKET):
    client.make_bucket(BUCKET)
    print(f"Bucket '{BUCKET}' creado")
else:
    print(f"Bucket '{BUCKET}' ya existe")

# Subir el fichero de datos de entrenamiento
data_file = "/home/miguel/practica_creativa/data/simple_flight_delay_features.jsonl.bz2"
object_name = "training/simple_flight_delay_features.jsonl.bz2"

print(f"Subiendo datos a MinIO: {object_name}...")
client.fput_object(BUCKET, object_name, data_file)
print(f"OK - Datos subidos a minio://{BUCKET}/{object_name}")

# Verificar
objects = list(client.list_objects(BUCKET, recursive=True))
print(f"\nContenido del bucket '{BUCKET}':")
for obj in objects:
    size_mb = obj.size / 1024 / 1024
    print(f"  {obj.object_name}  ({size_mb:.1f} MB)")
