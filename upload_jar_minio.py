"""Sube el JAR de inferencia a MinIO usando solo stdlib de Python (AWS Signature V4)."""
import hashlib, hmac, datetime, http.client, os

ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
ENDPOINT   = "minio:9000"
BUCKET     = "models"
OBJECT     = "flight_prediction_2.12-0.1.jar"
FILE_PATH  = "/app/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar"
REGION     = "us-east-1"
SERVICE    = "s3"

def sign(key, msg):
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

def get_signature_key(key, date_stamp, region, service):
    k = sign(("AWS4" + key).encode("utf-8"), date_stamp)
    k = sign(k, region)
    k = sign(k, service)
    return sign(k, "aws4_request")

with open(FILE_PATH, "rb") as f:
    payload = f.read()

payload_hash = hashlib.sha256(payload).hexdigest()
now = datetime.datetime.utcnow()
amz_date  = now.strftime("%Y%m%dT%H%M%SZ")
date_stamp = now.strftime("%Y%m%d")

canonical_uri     = f"/{BUCKET}/{OBJECT}"
canonical_qs      = ""
canonical_headers = (
    f"host:{ENDPOINT}\n"
    f"x-amz-content-sha256:{payload_hash}\n"
    f"x-amz-date:{amz_date}\n"
)
signed_headers = "host;x-amz-content-sha256;x-amz-date"

canonical_request = "\n".join([
    "PUT", canonical_uri, canonical_qs,
    canonical_headers, signed_headers, payload_hash
])

credential_scope = f"{date_stamp}/{REGION}/{SERVICE}/aws4_request"
string_to_sign = "\n".join([
    "AWS4-HMAC-SHA256", amz_date, credential_scope,
    hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
])

signing_key = get_signature_key(SECRET_KEY, date_stamp, REGION, SERVICE)
signature   = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

auth_header = (
    f"AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{credential_scope}, "
    f"SignedHeaders={signed_headers}, Signature={signature}"
)

conn = http.client.HTTPConnection(ENDPOINT)
conn.request("PUT", canonical_uri, body=payload, headers={
    "Host":                 ENDPOINT,
    "x-amz-date":          amz_date,
    "x-amz-content-sha256": payload_hash,
    "Authorization":        auth_header,
    "Content-Length":       str(len(payload)),
    "Content-Type":         "application/octet-stream",
})
resp = conn.getresponse()
print(f"Status: {resp.status} {resp.reason}")
if resp.status in (200, 201, 204):
    print("JAR subido a MinIO OK -> s3a://models/flight_prediction_2.12-0.1.jar")
else:
    print(resp.read().decode())
conn.close()
