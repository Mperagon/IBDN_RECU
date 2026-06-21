#!/bin/bash
# Despliega el sistema completo en Google Kubernetes Engine (GKE).
# Uso: bash k8s/deploy-gke.sh <PROJECT_ID> [ZONE]
# Ejemplo: bash k8s/deploy-gke.sh mi-proyecto-gcp europe-west1-b

set -e

PROJECT_ID="${1:?Uso: $0 <PROJECT_ID> [ZONE]}"
ZONE="${2:-europe-west1-b}"
CLUSTER_NAME="flight-prediction-cluster"
NAMESPACE="flight-prediction"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE_PREFIX="gcr.io/${PROJECT_ID}/"

echo "=== Configuración ==="
echo "  Proyecto : $PROJECT_ID"
echo "  Zona     : $ZONE"
echo "  Registry : ${IMAGE_PREFIX}"

# ─── 1. Crear cluster GKE ────────────────────────────────────────────────────
echo ""
echo "=== 1. Creando cluster GKE ==="
gcloud config set project "$PROJECT_ID"

if gcloud container clusters describe "$CLUSTER_NAME" --zone="$ZONE" &>/dev/null; then
  echo "  Cluster ya existe, omitiendo creación."
else
  gcloud container clusters create "$CLUSTER_NAME" \
    --zone="$ZONE" \
    --num-nodes=3 \
    --machine-type=e2-standard-4 \
    --disk-size=50GB \
    --enable-autoscaling \
    --min-nodes=2 \
    --max-nodes=5
fi

gcloud container clusters get-credentials "$CLUSTER_NAME" --zone="$ZONE"

# ─── 2. Build & push imágenes a Google Container Registry ────────────────────
echo ""
echo "=== 2. Construyendo y subiendo imágenes a GCR ==="
gcloud auth configure-docker --quiet

docker build -t "${IMAGE_PREFIX}flask:latest"   -f "$REPO_ROOT/Dockerfile.flask"         "$REPO_ROOT"
docker build -t "${IMAGE_PREFIX}mlflow:latest"  -f "$REPO_ROOT/docker/Dockerfile.mlflow"  "$REPO_ROOT"
docker build -t "${IMAGE_PREFIX}airflow:latest" -f "$REPO_ROOT/docker/Dockerfile.airflow" "$REPO_ROOT"

docker push "${IMAGE_PREFIX}flask:latest"
docker push "${IMAGE_PREFIX}mlflow:latest"
docker push "${IMAGE_PREFIX}airflow:latest"

# ─── 3. Aplicar manifiestos ──────────────────────────────────────────────────
echo ""
echo "=== 3. Aplicando manifiestos ==="

# Función que sustituye IMAGE_PREFIX y SERVICE_TYPE en los YAMLs
apply() {
  IMAGE_PREFIX=$IMAGE_PREFIX SERVICE_TYPE=${SERVICE_TYPE:-ClusterIP} envsubst < "$1" | kubectl apply -f -
}

kubectl apply -f "$REPO_ROOT/k8s/00-namespace.yaml"
kubectl apply -f "$REPO_ROOT/k8s/01-secrets.yaml"
kubectl apply -f "$REPO_ROOT/k8s/02-rbac.yaml"

echo "  → Infraestructura..."
kubectl apply -f "$REPO_ROOT/k8s/03-mongodb.yaml"
kubectl apply -f "$REPO_ROOT/k8s/04-cassandra.yaml"
kubectl apply -f "$REPO_ROOT/k8s/05-minio.yaml"
kubectl apply -f "$REPO_ROOT/k8s/06-kafka.yaml"

echo "  → Esperando Cassandra..."
kubectl wait --for=condition=ready pod -l app=cassandra -n $NAMESPACE --timeout=300s
kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s

echo "  → Spark..."
kubectl apply -f "$REPO_ROOT/k8s/07-spark.yaml"

echo "  → PostgreSQL..."
kubectl apply -f "$REPO_ROOT/k8s/08-postgres-mlflow.yaml"
kubectl apply -f "$REPO_ROOT/k8s/10-postgres-airflow.yaml"
kubectl wait --for=condition=ready pod -l app=postgres-mlflow  -n $NAMESPACE --timeout=60s
kubectl wait --for=condition=ready pod -l app=postgres-airflow -n $NAMESPACE --timeout=60s

echo "  → MLflow..."
apply "$REPO_ROOT/k8s/09-mlflow.yaml"

echo "  → Airflow..."
kubectl create configmap airflow-dags \
  --from-file=setup_k8s.py="$REPO_ROOT/resources/airflow/setup_k8s.py" \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
apply "$REPO_ROOT/k8s/11-airflow.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=120s

echo "  → Elasticsearch + Kibana + Logstash..."
kubectl apply -f "$REPO_ROOT/k8s/12-elasticsearch.yaml"
kubectl apply -f "$REPO_ROOT/k8s/13-kibana.yaml"
kubectl apply -f "$REPO_ROOT/k8s/14-logstash.yaml"

echo "  → Flask..."
apply "$REPO_ROOT/k8s/15-flask.yaml"

# ─── 4. Cambiar servicios externos a LoadBalancer ────────────────────────────
echo ""
echo "=== 4. Exponiendo servicios externos con LoadBalancer ==="
for svc in flask airflow-webserver mlflow spark-master minio kibana elasticsearch; do
  kubectl patch service "$svc" -n $NAMESPACE \
    -p '{"spec":{"type":"LoadBalancer"}}' 2>/dev/null || true
done

# ─── 5. Setup MinIO ──────────────────────────────────────────────────────────
echo ""
echo "=== 5. Configuración inicial en MinIO ==="
kubectl wait --for=condition=ready pod -l app=minio -n $NAMESPACE --timeout=120s
MINIO_POD=$(kubectl get pod -l app=minio -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n $NAMESPACE $MINIO_POD -- sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
  mc mb --ignore-existing local/flight-data
  mc mb --ignore-existing local/models
  echo 'Buckets OK'
"

echo ""
echo "=== 6. Subiendo JAR, script y datos a MinIO ==="
SPARK_POD=$(kubectl get pod -l app=spark-master -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$REPO_ROOT/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar" \
  "$NAMESPACE/$SPARK_POD:/tmp/flight_prediction_2.12-0.1.jar"
kubectl cp "$REPO_ROOT/resources/train_spark_mllib_model.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/train_spark_mllib_model.py"
kubectl cp "$REPO_ROOT/resources/create_iceberg_table.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/create_iceberg_table.py"
kubectl cp "$REPO_ROOT/data/simple_flight_delay_features.jsonl.bz2" \
  "$NAMESPACE/$SPARK_POD:/tmp/raw.jsonl.bz2"
kubectl cp "$REPO_ROOT/setup_minio_distributed.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/setup_minio_distributed.py"
kubectl exec -n $NAMESPACE $SPARK_POD -- \
  env JAR_PATH=/tmp/flight_prediction_2.12-0.1.jar \
      SCRIPT_PATH=/tmp/train_spark_mllib_model.py \
      ICEBERG_SCRIPT_PATH=/tmp/create_iceberg_table.py \
      DATA_PATH=/tmp/raw.jsonl.bz2 \
  python3 /tmp/setup_minio_distributed.py

# ─── 7. IPs externas ─────────────────────────────────────────────────────────
echo ""
echo "=== Esperando IPs externas de LoadBalancer (puede tardar 1-2 min) ==="
for svc in flask airflow-webserver mlflow spark-master minio kibana; do
  echo -n "  $svc: "
  for i in $(seq 1 20); do
    IP=$(kubectl get svc "$svc" -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null)
    if [ -n "$IP" ]; then echo "$IP"; break; fi
    sleep 6
  done
done

echo ""
echo "=== Despliegue en GKE completado ==="
echo "Para ver todas las IPs:"
echo "  kubectl get svc -n $NAMESPACE"
echo ""
echo "Para parar el cluster (evita costes):"
echo "  gcloud container clusters delete $CLUSTER_NAME --zone=$ZONE"
