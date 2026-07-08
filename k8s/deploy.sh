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

export IMAGE_PREFIX="flight-prediction/"

# Sustituye ${IMAGE_PREFIX} en los manifiestos que lo usan
apply_manifest() {
  IMAGE_PREFIX=$IMAGE_PREFIX envsubst < "$1" | kubectl apply -f -
}

# ── 1. Imágenes custom ────────────────────────────────────────────────────────
echo ""
echo "=== 1. Construyendo imágenes custom ==="
docker build -t ${IMAGE_PREFIX}flask:latest   -f "$REPO_ROOT/Dockerfile.flask"         "$REPO_ROOT"
docker build -t ${IMAGE_PREFIX}mlflow:latest  -f "$REPO_ROOT/docker/Dockerfile.mlflow"  "$REPO_ROOT"
docker build -t ${IMAGE_PREFIX}airflow:latest -f "$REPO_ROOT/docker/Dockerfile.airflow" "$REPO_ROOT"
docker build -t flink-custom:1.18.1           -f "$REPO_ROOT/Dockerfile.flink"          "$REPO_ROOT"
docker build -t ${IMAGE_PREFIX}spark:3.5.3    -f "$REPO_ROOT/Dockerfile.spark"          "$REPO_ROOT"

# ── 2. Namespace, Secrets, RBAC ───────────────────────────────────────────────
echo ""
echo "=== 2. Namespace / Secrets / RBAC ==="
kubectl apply -f "$REPO_ROOT/k8s/00-namespace.yaml"
kubectl apply -f "$REPO_ROOT/k8s/01-secrets.yaml"
kubectl apply -f "$REPO_ROOT/k8s/02-rbac.yaml"

# ── 3. Infraestructura de datos ───────────────────────────────────────────────
echo ""
echo "=== 3. Infraestructura de datos (MongoDB, Cassandra, MinIO, Kafka) ==="
kubectl apply -f "$REPO_ROOT/k8s/03-mongodb.yaml"
kubectl apply -f "$REPO_ROOT/k8s/04-cassandra.yaml"
kubectl apply -f "$REPO_ROOT/k8s/05-minio.yaml"
kubectl apply -f "$REPO_ROOT/k8s/06-kafka.yaml"

echo "  → Esperando Cassandra (puede tardar 2-3 minutos)..."
kubectl wait --for=condition=ready pod -l app=cassandra -n $NAMESPACE --timeout=300s
echo "  → Esperando job cassandra-init..."
kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s || {
  kubectl delete job cassandra-init -n $NAMESPACE --ignore-not-found
  kubectl apply -f "$REPO_ROOT/k8s/04-cassandra.yaml"
  kubectl wait --for=condition=complete job/cassandra-init -n $NAMESPACE --timeout=120s
}

# ── 4. Spark: History Server + Streaming Job ─────────────────────────────────
echo ""
echo "=== 4. Spark — History Server + Streaming Job (k8s://) ==="
kubectl apply -f "$REPO_ROOT/k8s/07-spark.yaml"

# ── 5. PostgreSQL ─────────────────────────────────────────────────────────────
echo ""
echo "=== 5. PostgreSQL (MLflow + Airflow) ==="
kubectl apply -f "$REPO_ROOT/k8s/08-postgres-mlflow.yaml"
kubectl apply -f "$REPO_ROOT/k8s/10-postgres-airflow.yaml"
kubectl wait --for=condition=ready pod -l app=postgres-mlflow  -n $NAMESPACE --timeout=60s
kubectl wait --for=condition=ready pod -l app=postgres-airflow -n $NAMESPACE --timeout=60s

# ── 6. MLflow ─────────────────────────────────────────────────────────────────
echo ""
echo "=== 6. MLflow ==="
apply_manifest "$REPO_ROOT/k8s/09-mlflow.yaml"

# ── 7. Airflow ────────────────────────────────────────────────────────────────
echo ""
echo "=== 7. Airflow ==="
kubectl create configmap airflow-dags \
  --from-file=setup_k8s.py="$REPO_ROOT/resources/airflow/setup_k8s.py" \
  -n $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -
kubectl delete job airflow-init -n $NAMESPACE --ignore-not-found
apply_manifest "$REPO_ROOT/k8s/11-airflow.yaml"
kubectl wait --for=condition=complete job/airflow-init -n $NAMESPACE --timeout=120s

# ── 8. ELK ────────────────────────────────────────────────────────────────────
echo ""
echo "=== 8. Elasticsearch + Kibana + Logstash ==="
kubectl apply -f "$REPO_ROOT/k8s/12-elasticsearch.yaml"
kubectl apply -f "$REPO_ROOT/k8s/13-kibana.yaml"
kubectl apply -f "$REPO_ROOT/k8s/14-logstash.yaml"

# ── 9. MinIO: buckets + scripts + datos ──────────────────────────────────────
echo ""
echo "=== 9. MinIO — subiendo scripts y datos ==="
echo "  → Esperando MinIO..."
kubectl wait --for=condition=ready pod -l app=minio -n $NAMESPACE --timeout=120s

echo "  → Creando pod temporal para subida de datos..."
kubectl run minio-uploader -n $NAMESPACE \
  --image=python:3.12-slim --restart=Never \
  --command -- sleep 600
kubectl wait --for=condition=ready pod/minio-uploader -n $NAMESPACE --timeout=60s

echo "  → Copiando archivos al pod..."
kubectl exec -n $NAMESPACE minio-uploader -i -- bash -c "cat > /tmp/train_spark_mllib_model.py" \
  < "$REPO_ROOT/resources/train_spark_mllib_model.py"
kubectl exec -n $NAMESPACE minio-uploader -i -- bash -c "cat > /tmp/create_iceberg_table.py" \
  < "$REPO_ROOT/resources/create_iceberg_table.py"
kubectl exec -n $NAMESPACE minio-uploader -i -- bash -c "cat > /tmp/predict_spark_streaming.py" \
  < "$REPO_ROOT/resources/spark/predict_spark_streaming.py"
kubectl exec -n $NAMESPACE minio-uploader -i -- bash -c "cat > /tmp/raw.jsonl.bz2" \
  < "$REPO_ROOT/data/simple_flight_delay_features.jsonl.bz2"
kubectl exec -n $NAMESPACE minio-uploader -i -- bash -c "cat > /tmp/origin_dest_distances.jsonl" \
  < "$REPO_ROOT/data/origin_dest_distances.jsonl"
kubectl exec -n $NAMESPACE minio-uploader -i -- bash -c "cat > /tmp/setup_minio_distributed.py" \
  < "$REPO_ROOT/setup_minio_distributed.py"

echo "  → Subiendo a MinIO..."
kubectl exec -n $NAMESPACE minio-uploader -- bash -c "
  pip install boto3 -q &&
  SCRIPT_PATH=/tmp/train_spark_mllib_model.py \
  ICEBERG_SCRIPT_PATH=/tmp/create_iceberg_table.py \
  DATA_PATH=/tmp/raw.jsonl.bz2 \
  DISTANCES_PATH=/tmp/origin_dest_distances.jsonl \
  MINIO_ENDPOINT=minio:9000 \
  python3 /tmp/setup_minio_distributed.py
"

# Subir el script de streaming y crear directorio spark-history
kubectl exec -n $NAMESPACE minio-uploader -- bash -c "
  pip install boto3 -q 2>/dev/null
  python3 -c \"
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin')
with open('/tmp/predict_spark_streaming.py','rb') as f:
    s3.put_object(Bucket='flight-data', Key='scripts/predict_spark_streaming.py', Body=f.read())
# Crear directorio spark-history (objeto vacío como marcador)
s3.put_object(Bucket='flight-data', Key='spark-history/.keep', Body=b'')
print('OK: predict_spark_streaming.py subido y spark-history creado')
\"
"

kubectl delete pod minio-uploader -n $NAMESPACE --ignore-not-found
echo "  → Datos subidos correctamente."

# ── 10. NiFi — carga distancias (batch 100) en Cassandra ─────────────────────
echo ""
echo "=== 10. NiFi — carga de distancias en Cassandra (BATCH 100) ==="
kubectl apply -f "$REPO_ROOT/k8s/17-nifi.yaml"
kubectl delete job nifi-init cassandra-nifi-wait -n $NAMESPACE --ignore-not-found 2>/dev/null || true
kubectl apply -f "$REPO_ROOT/k8s/18-nifi-init.yaml"
echo "  → Esperando que NiFi esté listo (puede tardar 3-4 minutos)..."
kubectl rollout status deployment/nifi -n $NAMESPACE --timeout=300s
echo "  → Configurando flujo NiFi (BATCH 100 registros)..."
kubectl wait --for=condition=complete job/nifi-init -n $NAMESPACE --timeout=300s
echo "  → Esperando que NiFi cargue las distancias en Cassandra..."
kubectl apply -f "$REPO_ROOT/k8s/19-cassandra-nifi-wait.yaml"
kubectl wait --for=condition=complete job/cassandra-nifi-wait -n $NAMESPACE --timeout=3600s
echo "  → Distancias cargadas en Cassandra."

# ── 11. Flask ─────────────────────────────────────────────────────────────────
echo ""
echo "=== 11. Flask ==="
apply_manifest "$REPO_ROOT/k8s/15-flask.yaml"

# ── 12. Flink ─────────────────────────────────────────────────────────────────
echo ""
echo "=== 12. Flink ==="
kubectl apply -f "$REPO_ROOT/k8s/20-flink.yaml"

# ── 13. Iceberg: crear tabla de features ──────────────────────────────────────
echo ""
echo "=== 13. Iceberg — creando tabla de features en MinIO ==="
kubectl delete job iceberg-init -n $NAMESPACE --ignore-not-found 2>/dev/null || true
cat <<'YAML' | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: iceberg-init
  namespace: flight-prediction
spec:
  backoffLimit: 2
  template:
    spec:
      serviceAccountName: spark
      restartPolicy: OnFailure
      containers:
        - name: iceberg-submit
          image: flight-prediction/spark:3.5.3
          imagePullPolicy: IfNotPresent
          command: [bash, -c]
          args:
            - |
              /opt/spark/bin/spark-submit \
                --master k8s://https://kubernetes.default.svc:443 \
                --deploy-mode cluster \
                --name iceberg-init \
                --conf spark.executor.instances=1 \
                --conf spark.executor.memory=512m \
                --conf spark.driver.memory=512m \
                --conf spark.kubernetes.container.image=flight-prediction/spark:3.5.3 \
                --conf spark.kubernetes.namespace=flight-prediction \
                --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
                --conf spark.kubernetes.executor.request.cores=200m \
                --conf spark.kubernetes.driver.request.cores=100m \
                --conf spark.jars.ivy=/tmp/.ivy2 \
                --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                --conf spark.hadoop.fs.s3a.access.key=minioadmin \
                --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
                --conf spark.hadoop.fs.s3a.path.style.access=true \
                --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
                --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
                --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
                http://minio:9000/flight-data/scripts/create_iceberg_table.py
          resources:
            requests:
              memory: 256Mi
              cpu: 100m
YAML
kubectl wait --for=condition=complete job/iceberg-init -n $NAMESPACE --timeout=600s
echo "  → Tabla Iceberg creada."

echo ""
echo "=== 14. Trigger DAG de entrenamiento en Airflow ==="
echo "  → Esperando Airflow scheduler..."
kubectl wait --for=condition=ready pod -l component=scheduler -n $NAMESPACE --timeout=60s 2>/dev/null || true
SCHEDULER_POD=$(kubectl get pods -n $NAMESPACE -l component=scheduler -o name 2>/dev/null | head -1)
if [ -n "$SCHEDULER_POD" ]; then
  kubectl exec -n $NAMESPACE $SCHEDULER_POD -- airflow dags trigger agile_data_science_batch_prediction_model_training 2>/dev/null || true
  echo "  → DAG lanzado. Ejecutar las tareas manualmente:"
  echo "     kubectl exec -n $NAMESPACE \$SCHEDULER_POD -- airflow tasks run agile_data_science_batch_prediction_model_training train_model \$(date -u +%Y-%m-%d) --local"
fi

# ── Resumen ───────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║              Despliegue K8s completado                          ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Acceso via start-portforwards.ps1 (PowerShell Windows)         ║"
echo "║  Flask (UI predicciones) → http://localhost:5001/flights/delays/predict_kafka ║"
echo "║  Airflow                 → http://localhost:8080  (admin/admin) ║"
echo "║  MLflow                  → http://localhost:5000               ║"
echo "║  NiFi                    → http://localhost:8850/nifi          ║"
echo "║  MinIO Console           → http://localhost:9001  (minioadmin) ║"
echo "║  Kibana                  → http://localhost:5601               ║"
echo "║  Flink UI                → http://localhost:8081               ║"
echo "║  Spark History Server    → http://localhost:18080              ║"
echo "╠══════════════════════════════════════════════════════════════════╣"
echo "║  Spark: --master k8s://https://kubernetes.default.svc:443       ║"
echo "║  Driver + executors como pods K8s bajo demanda                  ║"
echo "║  Checkpoint streaming: s3a://flight-data/spark-streaming-checkpoint ║"
echo "║  Event logs:           s3a://flight-data/spark-history          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""
echo "Estado de los pods:"
kubectl get pods -n $NAMESPACE
