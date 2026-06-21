#!/bin/bash
# Despliega el sistema completo en Kubernetes (Docker Desktop).
# Uso: bash k8s/deploy.sh
# Requisito: Docker Desktop con Kubernetes activado (Settings → Kubernetes → Enable Kubernetes)

set -e
NAMESPACE="flight-prediction"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== Verificando contexto kubectl ==="
kubectl config current-context
kubectl cluster-info --request-timeout=5s

echo ""
echo "=== 1. Construyendo imágenes custom ==="
docker build -t flight-prediction/flask:latest   -f "$REPO_ROOT/Dockerfile.flask"         "$REPO_ROOT"
docker build -t flight-prediction/mlflow:latest  -f "$REPO_ROOT/docker/Dockerfile.mlflow"  "$REPO_ROOT"
docker build -t flight-prediction/airflow:latest -f "$REPO_ROOT/docker/Dockerfile.airflow" "$REPO_ROOT"

echo ""
echo "=== 2. Aplicando manifiestos ==="
kubectl apply -f "$REPO_ROOT/k8s/00-namespace.yaml"
kubectl apply -f "$REPO_ROOT/k8s/01-secrets.yaml"
kubectl apply -f "$REPO_ROOT/k8s/02-rbac.yaml"

echo "  → Infraestructura de datos..."
kubectl apply -f "$REPO_ROOT/k8s/03-mongodb.yaml"
kubectl apply -f "$REPO_ROOT/k8s/04-cassandra.yaml"
kubectl apply -f "$REPO_ROOT/k8s/05-minio.yaml"
kubectl apply -f "$REPO_ROOT/k8s/06-kafka.yaml"

echo "  → Esperando Cassandra (puede tardar 2-3 minutos)..."
kubectl wait --for=condition=ready pod -l app=cassandra -n $NAMESPACE --timeout=300s
echo "  → Esperando job cassandra-init..."
kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s

echo "  → Spark..."
kubectl apply -f "$REPO_ROOT/k8s/07-spark.yaml"

echo "  → PostgreSQL..."
kubectl apply -f "$REPO_ROOT/k8s/08-postgres-mlflow.yaml"
kubectl apply -f "$REPO_ROOT/k8s/10-postgres-airflow.yaml"
kubectl wait --for=condition=ready pod -l app=postgres-mlflow  -n $NAMESPACE --timeout=60s
kubectl wait --for=condition=ready pod -l app=postgres-airflow -n $NAMESPACE --timeout=60s

echo "  → MLflow..."
kubectl apply -f "$REPO_ROOT/k8s/09-mlflow.yaml"

echo "  → Airflow..."
kubectl create configmap airflow-dags \
  --from-file=setup_k8s.py="$REPO_ROOT/resources/airflow/setup_k8s.py" \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$REPO_ROOT/k8s/11-airflow.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=120s

echo "  → Elasticsearch + Kibana + Logstash..."
kubectl apply -f "$REPO_ROOT/k8s/12-elasticsearch.yaml"
kubectl apply -f "$REPO_ROOT/k8s/13-kibana.yaml"
kubectl apply -f "$REPO_ROOT/k8s/14-logstash.yaml"

echo "  → Flask..."
kubectl apply -f "$REPO_ROOT/k8s/15-flask.yaml"

echo ""
echo "=== 3. Configuración inicial en MinIO ==="
echo "  Esperando MinIO..."
kubectl wait --for=condition=ready pod -l app=minio -n $NAMESPACE --timeout=120s
MINIO_POD=$(kubectl get pod -l app=minio -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n $NAMESPACE $MINIO_POD -- sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
  mc mb --ignore-existing local/flight-data
  mc mb --ignore-existing local/models
  echo 'Buckets OK'
"

echo ""
echo "=== 4. Subiendo JAR y script a MinIO ==="
SPARK_POD=$(kubectl get pod -l app=spark-master -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$REPO_ROOT/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar" \
  "$NAMESPACE/$SPARK_POD:/tmp/flight_prediction_2.12-0.1.jar"
kubectl cp "$REPO_ROOT/resources/train_spark_mllib_model.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/train_spark_mllib_model.py"
kubectl exec -n $NAMESPACE $SPARK_POD -- python3 /tmp/setup_minio_distributed.py 2>/dev/null || \
kubectl exec -n $NAMESPACE $SPARK_POD -- sh -c "
  python3 -c \"
import http.client, hashlib, hmac, datetime, json

def req(method, bucket, key, body=b''):
    now = datetime.datetime.utcnow()
    amz = now.strftime('%Y%m%dT%H%M%SZ')
    ds  = now.strftime('%Y%m%d')
    ph  = hashlib.sha256(body).hexdigest()
    uri = f'/{bucket}/{key}'
    ch  = f'host:minio:9000\nx-amz-content-sha256:{ph}\nx-amz-date:{amz}\n'
    sh  = 'host;x-amz-content-sha256;x-amz-date'
    cr  = f'PUT\n{uri}\n\n{ch}\n{sh}\n{ph}'
    cs  = f'{ds}/us-east-1/s3/aws4_request'
    sts = f'AWS4-HMAC-SHA256\n{amz}\n{cs}\n' + hashlib.sha256(cr.encode()).hexdigest()
    def sign(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
    sk = sign(sign(sign(sign(b'AWS4minioadmin', ds), 'us-east-1'), 's3'), 'aws4_request')
    sig = hmac.new(sk, sts.encode(), hashlib.sha256).hexdigest()
    auth = f'AWS4-HMAC-SHA256 Credential=minioadmin/{cs}, SignedHeaders={sh}, Signature={sig}'
    c = http.client.HTTPConnection('minio:9000')
    c.request(method, uri, body=body, headers={'Host':'minio:9000','x-amz-date':amz,'x-amz-content-sha256':ph,'Authorization':auth,'Content-Length':str(len(body))})
    r = c.getresponse(); r.read(); c.close(); return r.status

with open('/tmp/flight_prediction_2.12-0.1.jar','rb') as f: b=f.read()
print('JAR:', req('PUT','models','flight_prediction_2.12-0.1.jar',b))
with open('/tmp/train_spark_mllib_model.py','rb') as f: b=f.read()
print('Script:', req('PUT','flight-data','scripts/train_spark_mllib_model.py',b))
\"
"

echo ""
echo "=== Despliegue completado ==="
echo ""
echo "URLs (Docker Desktop — todos en localhost):"
echo "  Flask:         http://localhost:30501"
echo "  Airflow:       http://localhost:30808  (admin / admin)"
echo "  MLflow:        http://localhost:30500"
echo "  Spark UI:      http://localhost:30080"
echo "  MinIO Console: http://localhost:30901  (minioadmin / minioadmin)"
echo "  Kibana:        http://localhost:30561"
echo "  Elasticsearch: http://localhost:30920"
echo ""
echo "Para ver el estado de los pods:"
echo "  kubectl get pods -n $NAMESPACE"
