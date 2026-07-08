import sys, os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

PROJECT_HOME = os.getenv("PROJECT_HOME", "/practica")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}

training_dag = DAG(
    'agile_data_science_batch_prediction_model_training',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
)

check_spark = BashOperator(
    task_id='check_spark',
    bash_command="""
docker exec spark-master /opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  /opt/spark/examples/jars/spark-examples_2.12-3.5.3.jar 2
""",
    execution_timeout=timedelta(minutes=10),
    dag=training_dag,
)

train_model = BashOperator(
    task_id='train_model',
    bash_command="""
docker exec -u root spark-master bash -c "pip install --quiet mlflow scikit-learn pandas boto3 2>/dev/null; exit 0" || true
docker exec -u root spark-master \
  /opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.rpc.message.maxSize=256 \
  http://minio:9000/flight-data/scripts/train_spark_mllib_model.py
""",
    execution_timeout=timedelta(minutes=70),
    dag=training_dag,
)

register_mlflow = BashOperator(
    task_id='register_mlflow',
    bash_command="""
docker exec mlflow python3 << 'EOF'
import mlflow, sys
mlflow.set_tracking_uri('http://localhost:5000')

# Verificar runs del experimento (Spark MLlib + sklearn)
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

# Verificar sklearn en Model Registry (Production)
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
EOF
""",
    dag=training_dag,
)

check_spark >> train_model >> register_mlflow
