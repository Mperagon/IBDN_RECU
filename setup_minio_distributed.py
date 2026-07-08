"""
Setup distribuido de MinIO:
- Sube el JAR de inferencia a s3://models/
- Sube el script de entrenamiento a s3://flight-data/scripts/
- Sube los datos brutos a s3://flight-data/raw/
- Hace los buckets de lectura pública para que Spark los descargue via HTTP

Variables de entorno configurables:
  MINIO_ENDPOINT  (default: minio:9000)
  MINIO_ACCESS_KEY / MINIO_SECRET_KEY
  JAR_PATH, SCRIPT_PATH, DATA_PATH  (paths locales a los archivos)
  REPO_ROOT  (alternativa: prefijo base para las rutas por defecto)
"""
import hashlib, hmac, datetime, http.client, json, os, socket, time

ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "minio:9000")
REGION     = "us-east-1"

def sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

def signing_key(key, date, region, service):
    return sign(sign(sign(sign(("AWS4"+key).encode(), date), region), service), "aws4_request")

def s3_request(method, bucket, key, body=b"", content_type="application/octet-stream"):
    payload_hash = hashlib.sha256(body).hexdigest()
    now = datetime.datetime.utcnow()
    amz_date  = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    uri = f"/{bucket}/{key}" if key else f"/{bucket}"
    canonical_headers = f"host:{ENDPOINT}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = f"{method}\n{uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    cred_scope = f"{date_stamp}/{REGION}/s3/aws4_request"
    sts = f"AWS4-HMAC-SHA256\n{amz_date}\n{cred_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    sig = hmac.new(signing_key(SECRET_KEY, date_stamp, REGION, "s3"), sts.encode(), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{cred_scope}, SignedHeaders={signed_headers}, Signature={sig}"
    conn = http.client.HTTPConnection(ENDPOINT)
    headers = {
        "Host": ENDPOINT, "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash, "Authorization": auth,
        "Content-Length": str(len(body)), "Content-Type": content_type,
    }
    conn.request(method, uri, body=body, headers=headers)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, data

def wait_for_minio(retries=30, delay=3):
    host, port = ENDPOINT.split(":")
    for i in range(retries):
        try:
            sock = socket.create_connection((host, int(port)), timeout=2)
            sock.close()
            print(f"  MinIO listo en {ENDPOINT}")
            return
        except (ConnectionRefusedError, OSError):
            print(f"  Esperando MinIO... ({i+1}/{retries})")
            time.sleep(delay)
    raise RuntimeError(f"MinIO no disponible en {ENDPOINT} tras {retries} intentos")

def create_bucket(bucket):
    status, _ = s3_request("PUT", bucket, "", b"", "application/xml")
    if status in (200, 201, 204, 409):  # 409 = ya existe
        print(f"  OK: bucket '{bucket}'")
    else:
        print(f"  ERROR {status}: creando bucket '{bucket}'")

def upload_file(local_path, bucket, key):
    with open(local_path, "rb") as f:
        body = f.read()
    status, _ = s3_request("PUT", bucket, key, body)
    if status in (200, 201, 204):
        print(f"  OK: {local_path} -> s3://{bucket}/{key}")
    else:
        print(f"  ERROR {status}: {local_path} -> s3://{bucket}/{key}")

def set_bucket_public_read(bucket):
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{bucket}/*"]
        }]
    })
    # PUT bucket policy requires ?policy query param — use raw HTTP
    payload_hash = hashlib.sha256(policy.encode()).hexdigest()
    now = datetime.datetime.utcnow()
    amz_date  = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    uri = f"/{bucket}?policy"
    canonical_headers = f"host:{ENDPOINT}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n"
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = f"PUT\n/{bucket}\npolicy=\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    cred_scope = f"{date_stamp}/{REGION}/s3/aws4_request"
    sts = f"AWS4-HMAC-SHA256\n{amz_date}\n{cred_scope}\n{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    sig = hmac.new(signing_key(SECRET_KEY, date_stamp, REGION, "s3"), sts.encode(), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{cred_scope}, SignedHeaders={signed_headers}, Signature={sig}"
    conn = http.client.HTTPConnection(ENDPOINT)
    conn.request("PUT", uri, body=policy.encode(), headers={
        "Host": ENDPOINT, "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash, "Authorization": auth,
        "Content-Length": str(len(policy)), "Content-Type": "application/json",
    })
    resp = conn.getresponse()
    resp.read()
    conn.close()
    if resp.status in (200, 201, 204):
        print(f"  OK: bucket '{bucket}' set to public read")
    else:
        print(f"  ERROR {resp.status}: setting bucket policy on '{bucket}'")

_base = os.environ.get("REPO_ROOT", "/app")
SCRIPT_PATH        = os.environ.get("SCRIPT_PATH",        f"{_base}/resources/train_spark_mllib_model.py")
ICEBERG_SCRIPT_PATH= os.environ.get("ICEBERG_SCRIPT_PATH",f"{_base}/resources/create_iceberg_table.py")
DATA_PATH          = os.environ.get("DATA_PATH",          f"{_base}/data/simple_flight_delay_features.jsonl.bz2")
DISTANCES_PATH     = os.environ.get("DISTANCES_PATH",     f"{_base}/data/origin_dest_distances.jsonl")

print("=== 0. Esperando a que MinIO este listo ===")
wait_for_minio()

print("\n=== 0b. Creando buckets ===")
create_bucket("models")
create_bucket("flight-data")

print("\n=== 1. Subiendo script de entrenamiento a MinIO ===")
upload_file(SCRIPT_PATH, "flight-data", "scripts/train_spark_mllib_model.py")

print("\n=== 3. Subiendo script de creacion de tabla Iceberg a MinIO ===")
upload_file(ICEBERG_SCRIPT_PATH, "flight-data", "scripts/create_iceberg_table.py")

print("\n=== 4. Subiendo datos de entrenamiento a MinIO ===")
upload_file(DATA_PATH, "flight-data", "raw/simple_flight_delay_features.jsonl.bz2")

print("\n=== 4b. Subiendo distancias entre aeropuertos a MinIO ===")
upload_file(DISTANCES_PATH, "flight-data", "raw/origin_dest_distances.jsonl")

print("\n=== 4. Haciendo bucket 'models' de lectura publica ===")
set_bucket_public_read("models")

print("\n=== 5. Haciendo bucket 'flight-data' de lectura publica ===")
set_bucket_public_read("flight-data")

print("\nDone. Recursos disponibles en MinIO:")
print("  http://minio:9000/flight-data/scripts/train_spark_mllib_model.py")
print("  http://minio:9000/flight-data/scripts/create_iceberg_table.py")
print("  s3a://flight-data/raw/simple_flight_delay_features.jsonl.bz2")
print("  http://minio:9000/flight-data/raw/origin_dest_distances.jsonl")
