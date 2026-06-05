"""
Crea los buckets necesarios en MinIO.
Ejecutar una sola vez antes de entrenar el modelo.
"""
import os
from minio import Minio
MINIO_ENDPOINT = os.getenv("MINIO_HOST", "127.0.0.1") + ":9000"
MINIO_ACCESS   = "minioadmin"
MINIO_SECRET   = "minioadmin"

client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)

for bucket in ["flight-data", "models"]:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Bucket '{bucket}' creado")
    else:
        print(f"Bucket '{bucket}' ya existe")

print("\nBuckets disponibles:")
for b in client.list_buckets():
    print(f"  {b.name}")
