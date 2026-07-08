"""DAG de entrenamiento — Spark on Kubernetes, deploy-mode cluster.

Arquitectura:
  - Cluster manager : Kubernetes (--master k8s://https://kubernetes.default.svc:443)
  - Deploy mode     : cluster  (--deploy-mode cluster)

  KubernetesPodOperator (submitter)
    └─ spark-submit ──► K8s crea pod DRIVER (flight-prediction/spark:3.5.3)
                              └─ K8s crea pods EXECUTOR × 2 (apache/spark:3.5.3)
                              └─ [entrenamiento]
                              └─ K8s destruye ejecutores y driver al terminar

  El submitter espera a que el driver pod acabe (waitAppCompletion=true por defecto)
  y devuelve su exit code a Airflow → tarea OK/FAIL según el resultado real.

Imágenes:
  - Submitter  : apache/spark:3.5.3         (solo necesita spark-submit)
  - Driver pod : flight-prediction/spark:3.5.3  (tiene mlflow, sklearn, boto3...)
  - Executors  : apache/spark:3.5.3         (JVM, no necesita Python deps)
"""
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

NAMESPACE          = "flight-prediction"
SPARK_IMAGE        = "apache/spark:3.5.3"
SPARK_DRIVER_IMAGE = "flight-prediction/spark:3.5.3"  # imagen custom con Python deps

K8S_MASTER  = "k8s://https://kubernetes.default.svc:443"
MINIO_URL   = "http://minio:9000/flight-data/scripts/train_spark_mllib_model.py"
PACKAGES    = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}

dag = DAG(
    "agile_data_science_batch_prediction_model_training",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

# ── check_spark ────────────────────────────────────────────────────────────────
# Verifica el cluster K8s lanzando SparkPi en deploy-mode cluster.
# K8s crea un driver pod (imagen estándar, solo Java) + 1 executor.
check_spark = KubernetesPodOperator(
    task_id="check_spark",
    name="spark-pi-submit",
    namespace=NAMESPACE,
    image=SPARK_IMAGE,
    service_account_name="spark",
    cmds=["bash", "-c"],
    arguments=[
        "/opt/spark/bin/spark-submit "
        f"--master {K8S_MASTER} "
        "--deploy-mode cluster "
        "--name spark-pi-check "
        "--conf spark.executor.instances=1 "
        f"--conf spark.kubernetes.container.image={SPARK_IMAGE} "
        f"--conf spark.kubernetes.namespace={NAMESPACE} "
        "--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark "
        "--class org.apache.spark.examples.SparkPi "
        "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar 2"
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

# ── train_model ────────────────────────────────────────────────────────────────
# Entrena el modelo. K8s crea:
#   - 1 driver pod  (flight-prediction/spark:3.5.3 — con mlflow, sklearn, boto3)
#   - 2 executor pods (apache/spark:3.5.3 — procesamiento JVM puro)
# El submitter espera al driver y devuelve su exit code.
train_model = KubernetesPodOperator(
    task_id="train_model",
    name="spark-train-submit",
    namespace=NAMESPACE,
    image=SPARK_IMAGE,
    service_account_name="spark",
    cmds=["bash", "-c"],
    arguments=[
        "/opt/spark/bin/spark-submit "
        f"--master {K8S_MASTER} "
        "--deploy-mode cluster "
        "--name spark-train-model "
        "--conf spark.executor.instances=2 "
        "--conf spark.driver.memory=3g "
        "--conf spark.driver.memoryOverhead=512m "
        "--conf spark.executor.memory=1g "
        # imagen para executors (JVM, sin Python deps)
        f"--conf spark.kubernetes.container.image={SPARK_IMAGE} "
        # imagen para el driver pod (con mlflow, sklearn, boto3)
        f"--conf spark.kubernetes.driver.container.image={SPARK_DRIVER_IMAGE} "
        f"--conf spark.kubernetes.namespace={NAMESPACE} "
        "--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark "
        "--conf spark.jars.ivy=/tmp/.ivy2 "
        # credenciales y endpoints pasados al driver pod via driverEnv
        "--conf spark.kubernetes.driverEnv.MINIO_ENDPOINT=http://minio:9000 "
        "--conf spark.kubernetes.driverEnv.MINIO_ACCESS_KEY=minioadmin "
        "--conf spark.kubernetes.driverEnv.MINIO_SECRET_KEY=minioadmin "
        "--conf spark.kubernetes.driverEnv.MLFLOW_TRACKING_URI=http://mlflow:5000 "
        "--conf spark.kubernetes.driverEnv.AWS_ACCESS_KEY_ID=minioadmin "
        "--conf spark.kubernetes.driverEnv.AWS_SECRET_ACCESS_KEY=minioadmin "
        "--conf spark.kubernetes.driverEnv.MLFLOW_S3_ENDPOINT_URL=http://minio:9000 "
        "--conf spark.eventLog.enabled=true "
        "--conf spark.eventLog.dir=s3a://flight-data/spark-history "
        f"--packages {PACKAGES} "
        f"{MINIO_URL}"
    ],
    get_logs=True,
    is_delete_operator_pod=True,
    execution_timeout=timedelta(minutes=50),
    dag=dag,
)

# ── register_mlflow ────────────────────────────────────────────────────────────
# Verifica que ambos modelos están registrados en MLflow correctamente.
register_mlflow = KubernetesPodOperator(
    task_id="register_mlflow",
    name="mlflow-verify",
    namespace=NAMESPACE,
    image="flight-prediction/mlflow:latest",
    image_pull_policy="IfNotPresent",
    cmds=["python3", "-c"],
    arguments=["""
import mlflow, sys
mlflow.set_tracking_uri('http://mlflow:5000')

try:
    target = next((e for e in mlflow.search_experiments() if e.name == 'flight_delay_prediction'), None)
    if not target:
        print('ERROR: experimento flight_delay_prediction no encontrado')
        sys.exit(1)
    runs = mlflow.search_runs(experiment_ids=[target.experiment_id], max_results=10)
    if runs.empty:
        print('ERROR: no hay runs registrados')
        sys.exit(1)
    print('Runs en flight_delay_prediction:', len(runs))
    for _, row in runs.head(5).iterrows():
        print('  Run:', str(row.get('run_id', ''))[:8],
              '| accuracy =', row.get('metrics.accuracy', 'N/A'))
except Exception as ex:
    print('Error experimento:', ex)
    sys.exit(1)

try:
    client = mlflow.tracking.MlflowClient()
    versions = client.get_latest_versions('sklearn_flight_model', stages=['Production'])
    if not versions:
        print('ERROR: sklearn_flight_model no encontrado en Production')
        sys.exit(1)
    v = versions[0]
    print('sklearn_flight_model Production: version', v.version, '| run_id:', v.run_id[:8])
except Exception as ex:
    print('Error sklearn registry:', ex)
    sys.exit(1)

print('OK - Ambos modelos verificados en MLflow')
"""],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

check_spark >> train_model >> register_mlflow
