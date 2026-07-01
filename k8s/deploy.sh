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

IMAGE_PREFIX="flight-prediction/"

echo ""
echo "=== 1. Construyendo imágenes custom ==="
docker build -t ${IMAGE_PREFIX}flask:latest   -f "$REPO_ROOT/Dockerfile.flask"         "$REPO_ROOT"
docker build -t ${IMAGE_PREFIX}mlflow:latest  -f "$REPO_ROOT/docker/Dockerfile.mlflow"  "$REPO_ROOT"
docker build -t ${IMAGE_PREFIX}airflow:latest -f "$REPO_ROOT/docker/Dockerfile.airflow" "$REPO_ROOT"
docker build -t flink-custom:1.18.1           -f "$REPO_ROOT/Dockerfile.flink"          "$REPO_ROOT"

echo ""
echo "=== 2. Aplicando manifiestos ==="
apply_manifest() {
  IMAGE_PREFIX=$IMAGE_PREFIX envsubst < "$1" | kubectl apply -f -
}

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
kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s || \
  (kubectl delete job cassandra-init -n $NAMESPACE --ignore-not-found && \
   kubectl apply -f "$REPO_ROOT/k8s/04-cassandra.yaml" && \
   kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s)

echo "  → Spark..."
kubectl apply -f "$REPO_ROOT/k8s/07-spark.yaml"

echo "  → PostgreSQL..."
kubectl apply -f "$REPO_ROOT/k8s/08-postgres-mlflow.yaml"
kubectl apply -f "$REPO_ROOT/k8s/10-postgres-airflow.yaml"
kubectl wait --for=condition=ready pod -l app=postgres-mlflow  -n $NAMESPACE --timeout=60s
kubectl wait --for=condition=ready pod -l app=postgres-airflow -n $NAMESPACE --timeout=60s

echo "  → MLflow..."
apply_manifest "$REPO_ROOT/k8s/09-mlflow.yaml"

echo "  → Airflow..."
kubectl create configmap airflow-dags \
  --from-file=setup_k8s.py="$REPO_ROOT/resources/airflow/setup_k8s.py.disabled" \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
kubectl delete job airflow-init -n $NAMESPACE --ignore-not-found
apply_manifest "$REPO_ROOT/k8s/11-airflow.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=120s

echo "  → Elasticsearch + Kibana + Logstash..."
kubectl apply -f "$REPO_ROOT/k8s/12-elasticsearch.yaml"
kubectl apply -f "$REPO_ROOT/k8s/13-kibana.yaml"
kubectl apply -f "$REPO_ROOT/k8s/14-logstash.yaml"

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
echo "=== 4. Subiendo JAR, script y datos a MinIO ==="
SPARK_POD=$(kubectl get pod -l app=spark-master -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$REPO_ROOT/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar" \
  "$NAMESPACE/$SPARK_POD:/tmp/flight_prediction_2.12-0.1.jar"
kubectl cp "$REPO_ROOT/resources/train_spark_mllib_model.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/train_spark_mllib_model.py"
kubectl cp "$REPO_ROOT/resources/create_iceberg_table.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/create_iceberg_table.py"
kubectl cp "$REPO_ROOT/data/simple_flight_delay_features.jsonl.bz2" \
  "$NAMESPACE/$SPARK_POD:/tmp/raw.jsonl.bz2"
kubectl cp "$REPO_ROOT/data/origin_dest_distances.jsonl" \
  "$NAMESPACE/$SPARK_POD:/tmp/origin_dest_distances.jsonl"
kubectl cp "$REPO_ROOT/setup_minio_distributed.py" \
  "$NAMESPACE/$SPARK_POD:/tmp/setup_minio_distributed.py"
kubectl exec -n $NAMESPACE $SPARK_POD -- \
  env JAR_PATH=/tmp/flight_prediction_2.12-0.1.jar \
      SCRIPT_PATH=/tmp/train_spark_mllib_model.py \
      ICEBERG_SCRIPT_PATH=/tmp/create_iceberg_table.py \
      DATA_PATH=/tmp/raw.jsonl.bz2 \
      DISTANCES_PATH=/tmp/origin_dest_distances.jsonl \
  python3 /tmp/setup_minio_distributed.py

echo ""
echo "=== 5. NiFi — carga de distancias en Cassandra ==="
kubectl apply -f "$REPO_ROOT/k8s/17-nifi.yaml"
kubectl delete job nifi-init cassandra-nifi-wait -n $NAMESPACE --ignore-not-found 2>/dev/null || true
kubectl apply -f "$REPO_ROOT/k8s/18-nifi-init.yaml"
echo "  → Esperando que NiFi esté listo (puede tardar 3-4 minutos)..."
kubectl wait --for=condition=ready pod -l app=nifi -n $NAMESPACE --timeout=300s
echo "  → Lanzando job nifi-init (configura el flujo NiFi via API)..."
kubectl wait --for=condition=complete job/nifi-init -n $NAMESPACE --timeout=300s
echo "  → Lanzando job cassandra-nifi-wait (espera a que NiFi cargue las distancias)..."
kubectl apply -f "$REPO_ROOT/k8s/19-cassandra-nifi-wait.yaml"
kubectl wait --for=condition=complete job/cassandra-nifi-wait -n $NAMESPACE --timeout=3600s
echo "  → Distancias cargadas en Cassandra."

echo ""
echo "=== 6. Flask ==="
apply_manifest "$REPO_ROOT/k8s/15-flask.yaml"

echo ""
echo "=== 7. Flink ==="
kubectl apply -f "$REPO_ROOT/k8s/20-flink.yaml"

echo ""
echo "=== Despliegue completado ==="
echo ""
echo "URLs (Docker Desktop — todos en localhost):"
echo "  Flask:         http://localhost:30501"
echo "  NiFi:          http://localhost:30850/nifi"
echo "  Airflow:       http://localhost:30808  (admin / admin)"
echo "  MLflow:        http://localhost:30500"
echo "  Spark UI:      http://localhost:30080"
echo "  MinIO Console: http://localhost:30901  (minioadmin / minioadmin)"
echo "  Kibana:        http://localhost:30561"
echo "  Elasticsearch: http://localhost:30920"
echo "  Flink UI:      http://localhost:30082"
echo ""
echo "Para ver el estado de los pods:"
echo "  kubectl get pods -n $NAMESPACE"
