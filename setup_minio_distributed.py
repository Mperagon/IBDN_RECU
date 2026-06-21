"""
Setup distribuido de MinIO:
- Hace el bucket 'models' de lectura pública (para que Spark descargue el JAR por HTTP)
- Sube el JAR de inferencia a s3://models/
- Sube el script de entrenamiento a s3://flight-data/scripts/
Ejecutar desde spark-master: docker exec spark-master python3 /app/setup_minio_distributed.py
"""
import hashlib, hmac, datetime, http.client, json, os

ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
ENDPOINT   = "minio:9000"
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

BASE = "/app"
print("=== 1. Subiendo JAR de inferencia a MinIO ===")
upload_file(
    f"{BASE}/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar",
    "models", "flight_prediction_2.12-0.1.jar"
)

print("\n=== 2. Subiendo script de entrenamiento a MinIO ===")
upload_file(
    f"{BASE}/resources/train_spark_mllib_model.py",
    "flight-data", "scripts/train_spark_mllib_model.py"
)

print("\n=== 3. Haciendo bucket 'models' de lectura publica ===")
set_bucket_public_read("models")

print("\n=== 4. Haciendo bucket 'flight-data' de lectura publica ===")
set_bucket_public_read("flight-data")

print("\nDone. Spark puede descargar el JAR via:")
print("  http://minio:9000/models/flight_prediction_2.12-0.1.jar")
print("  http://minio:9000/flight-data/scripts/train_spark_mllib_model.py")
