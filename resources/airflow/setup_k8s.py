"""DAG de entrenamiento para despliegue en Kubernetes.
Usa KubernetesPodOperator en lugar de docker exec.
"""
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from datetime import datetime, timedelta

NAMESPACE = "flight-prediction"
MINIO_ENV = [
    k8s.V1EnvVar(name="MINIO_ENDPOINT",   value="http://minio:9000"),
    k8s.V1EnvVar(name="MINIO_ACCESS_KEY", value="minioadmin"),
    k8s.V1EnvVar(name="MINIO_SECRET_KEY", value="minioadmin"),
    k8s.V1EnvVar(name="MLFLOW_TRACKING_URI", value="http://mlflow:5000"),
    k8s.V1EnvVar(name="AWS_ACCESS_KEY_ID",     value="minioadmin"),
    k8s.V1EnvVar(name="AWS_SECRET_ACCESS_KEY", value="minioadmin"),
    k8s.V1EnvVar(name="MLFLOW_S3_ENDPOINT_URL", value="http://minio:9000"),
]

IVY2_VOLUME = k8s.V1Volume(
    name="ivy2",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="ivy2-cache")
)
IVY2_MOUNT = k8s.V1VolumeMount(name="ivy2", mount_path="/root/.ivy2")

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

check_spark = KubernetesPodOperator(
    task_id="check_spark",
    name="spark-pi-check",
    namespace=NAMESPACE,
    image="apache/spark:3.5.3",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=[
        "--master", "spark://spark-master:7077",
        "--conf", "spark.driver.host=check-spark-pod",
        "--class", "org.apache.spark.examples.SparkPi",
        "/opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar", "2"
    ],
    volumes=[IVY2_VOLUME],
    volume_mounts=[IVY2_MOUNT],
    get_logs=True,
    is_delete_operator_pod=True,
    execution_timeout=timedelta(minutes=10),
    dag=dag,
)

train_model = KubernetesPodOperator(
    task_id="train_model",
    name="spark-train-model",
    namespace=NAMESPACE,
    image="apache/spark:3.5.3",
    cmds=["/opt/spark/bin/spark-submit"],
    arguments=[
        "--master", "spark://spark-master:7077",
        "--conf", "spark.driver.host=train-model-pod",
        "--packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "http://minio:9000/flight-data/scripts/train_spark_mllib_model.py"
    ],
    env_vars=MINIO_ENV,
    volumes=[IVY2_VOLUME],
    volume_mounts=[IVY2_MOUNT],
    get_logs=True,
    is_delete_operator_pod=True,
    execution_timeout=timedelta(minutes=50),
    dag=dag,
)

register_mlflow = KubernetesPodOperator(
    task_id="register_mlflow",
    name="mlflow-verify",
    namespace=NAMESPACE,
    image="flight-prediction/mlflow:latest",
    image_pull_policy="Never",
    cmds=["python3", "-c"],
    arguments=["""
import mlflow, sys
mlflow.set_tracking_uri('http://mlflow:5000')
exps = mlflow.search_experiments()
print('Experimentos en MLflow:', len(exps))
target = next((e for e in exps if e.name == 'flight_delay_prediction'), None)
if not target:
    print('ERROR: experimento flight_delay_prediction no encontrado')
    sys.exit(1)
runs = mlflow.search_runs(experiment_ids=[target.experiment_id], max_results=5)
print('Runs:', len(runs))
if runs.empty:
    sys.exit(1)
for _, row in runs.head(3).iterrows():
    print('  accuracy =', row.get('metrics.accuracy', 'N/A'))
"""],
    get_logs=True,
    is_delete_operator_pod=True,
    dag=dag,
)

check_spark >> train_model >> register_mlflow
