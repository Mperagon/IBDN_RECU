#!/bin/bash
# Script de despliegue completo en Kubernetes (minikube)
# Uso: bash k8s/deploy.sh

set -e
NAMESPACE="flight-prediction"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "=== 1. Iniciando minikube (si no está corriendo) ==="
minikube status || minikube start --memory=12288 --cpus=4 --disk-size=50g

echo "=== 2. Construyendo imágenes dentro de minikube ==="
eval $(minikube docker-env)

docker build -t flight-prediction/flask:latest   -f "$REPO_ROOT/Dockerfile.flask"        "$REPO_ROOT"
docker build -t flight-prediction/mlflow:latest  -f "$REPO_ROOT/docker/Dockerfile.mlflow" "$REPO_ROOT"
docker build -t flight-prediction/airflow:latest -f "$REPO_ROOT/docker/Dockerfile.airflow" "$REPO_ROOT"

echo "=== 3. Aplicando manifiestos ==="
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

echo "  → PostgreSQL para MLflow y Airflow..."
kubectl apply -f "$REPO_ROOT/k8s/08-postgres-mlflow.yaml"
kubectl apply -f "$REPO_ROOT/k8s/10-postgres-airflow.yaml"
kubectl wait --for=condition=ready pod -l app=postgres-mlflow  -n $NAMESPACE --timeout=60s
kubectl wait --for=condition=ready pod -l app=postgres-airflow -n $NAMESPACE --timeout=60s

echo "  → MLflow..."
kubectl apply -f "$REPO_ROOT/k8s/09-mlflow.yaml"

echo "  → DAG de Airflow (ConfigMap)..."
kubectl create configmap airflow-dags \
  --from-file=setup_k8s.py="$REPO_ROOT/resources/airflow/setup_k8s.py" \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

echo "  → Airflow..."
kubectl apply -f "$REPO_ROOT/k8s/11-airflow.yaml"
kubectl apply -f "$REPO_ROOT/k8s/16-airflow-dags-configmap.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=120s

echo "  → Elasticsearch + Kibana + Logstash..."
kubectl apply -f "$REPO_ROOT/k8s/12-elasticsearch.yaml"
kubectl apply -f "$REPO_ROOT/k8s/13-kibana.yaml"
kubectl apply -f "$REPO_ROOT/k8s/14-logstash.yaml"

echo "  → Flask..."
kubectl apply -f "$REPO_ROOT/k8s/15-flask.yaml"

echo ""
echo "=== 4. Configuración inicial en MinIO ==="
echo "  Esperando MinIO..."
kubectl wait --for=condition=ready pod -l app=minio -n $NAMESPACE --timeout=120s

MINIO_POD=$(kubectl get pod -l app=minio -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')

echo "  Creando buckets..."
kubectl exec -n $NAMESPACE $MINIO_POD -- sh -c "
  mc alias set local http://localhost:9000 minioadmin minioadmin 2>/dev/null || true
  mc mb local/flight-data 2>/dev/null || true
  mc mb local/models 2>/dev/null || true
  echo 'Buckets OK'
"

echo ""
echo "=== 5. Subiendo JAR y script a MinIO ==="
SPARK_POD=$(kubectl get pod -l app=spark-master -n $NAMESPACE -o jsonpath='{.items[0].metadata.name}')
kubectl cp "$REPO_ROOT/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar" \
  $NAMESPACE/$SPARK_POD:/tmp/flight_prediction_2.12-0.1.jar
kubectl cp "$REPO_ROOT/resources/train_spark_mllib_model.py" \
  $NAMESPACE/$SPARK_POD:/tmp/train_spark_mllib_model.py
kubectl exec -n $NAMESPACE $SPARK_POD -- python3 /app/setup_minio_distributed.py 2>/dev/null || \
kubectl exec -n $NAMESPACE $SPARK_POD -- sh -c "
  wget -q http://localhost:9000 2>/dev/null || true
"

echo ""
echo "=== Despliegue completado ==="
echo ""
echo "URLs (ejecuta 'minikube service list -n $NAMESPACE' para ver los NodePorts):"
minikube service list -n $NAMESPACE 2>/dev/null || true
echo ""
echo "  Flask:        http://\$(minikube ip):30501"
echo "  Airflow:      http://\$(minikube ip):30808  (admin/admin)"
echo "  MLflow:       http://\$(minikube ip):30500"
echo "  Spark UI:     http://\$(minikube ip):30080"
echo "  MinIO:        http://\$(minikube ip):30901  (minioadmin/minioadmin)"
echo "  Kibana:       http://\$(minikube ip):30561"
echo "  Elasticsearch: http://\$(minikube ip):30920"
