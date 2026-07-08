"""
Entrena un modelo sklearn Random Forest para predicción de retrasos de vuelos.
Lee los datos desde la tabla Iceberg en MinIO, entrena el modelo, lo guarda en MinIO
y lo registra en MLflow Registry (stage Production).

Ejecutado por el DAG train_model_k8s en el pod driver (deploy-mode cluster).
El driver usa la imagen flight-prediction/spark:3.5.3 que incluye mlflow,
scikit-learn, pandas y boto3. Los executor pods usan apache/spark:3.5.3 (JVM).
"""
import os
import io
import logging

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT",          "http://minio:9000")
MINIO_ACCESS   = os.environ.get("AWS_ACCESS_KEY_ID",       "minioadmin")
MINIO_SECRET   = os.environ.get("AWS_SECRET_ACCESS_KEY",   "minioadmin")
MLFLOW_URI     = os.environ.get("MLFLOW_TRACKING_URI",     "http://mlflow:5000")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, when

spark = SparkSession.builder \
    .appName("TrainSklearnFlightModel") \
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.minio_catalog",
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.minio_catalog.type", "hadoop") \
    .config("spark.sql.catalog.minio_catalog.warehouse",
            "s3a://flight-data/iceberg") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

log.info("Leyendo datos desde minio_catalog.flight_features...")
df = (
    spark.sql("SELECT * FROM minio_catalog.flight_features")
    .withColumn("Route", concat_ws("-", col("Origin"), col("Dest")))
    .withColumn(
        "ArrDelayBucket",
        when(col("ArrDelay") < -15, 0)
        .when(col("ArrDelay") <   0, 1)
        .when(col("ArrDelay") <  30, 2)
        .otherwise(3),
    )
    .select(
        "Carrier", "Origin", "Dest", "Route",
        "DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear",
        "ArrDelayBucket",
    )
    .na.drop()
)

row_count = df.count()
log.info(f"Filas de entrenamiento: {row_count}")

# Convertir a pandas en el driver (3 g de memoria reservados en el DAG)
log.info("Convirtiendo a pandas para entrenamiento sklearn...")
pdf = df.toPandas()

import pandas as pd
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib
import boto3
import mlflow
import mlflow.sklearn
from datetime import datetime, timezone

CATEGORICAL = ["Carrier", "Origin", "Dest", "Route"]
NUMERIC     = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
FEATURES    = CATEGORICAL + NUMERIC

X = pdf[FEATURES]
y = pdf["ArrDelayBucket"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

pipeline = Pipeline([
    ("prep", ColumnTransformer([
        ("cat", OrdinalEncoder(
            handle_unknown="use_encoded_value", unknown_value=-1
        ), CATEGORICAL),
        ("num", "passthrough", NUMERIC),
    ])),
    ("clf", RandomForestClassifier(
        n_estimators=100, max_depth=12, n_jobs=-1, random_state=42
    )),
])

log.info("Entrenando Random Forest sklearn...")
pipeline.fit(X_train, y_train)

accuracy = accuracy_score(y_test, pipeline.predict(X_test))
log.info(f"Accuracy: {accuracy:.4f}")

# Guardar modelo en MinIO
log.info("Guardando modelo en MinIO (flight-data/models/sklearn_random_forest.joblib)...")
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
)
buf = io.BytesIO()
joblib.dump(pipeline, buf)
buf.seek(0)
s3.put_object(
    Bucket="flight-data",
    Key="models/sklearn_random_forest.joblib",
    Body=buf.getvalue(),
)
log.info("Modelo guardado en MinIO.")

# Registrar en MLflow y promover a Production
log.info("Registrando en MLflow Registry...")
mlflow.set_tracking_uri(MLFLOW_URI)
mlflow.set_experiment("flight_delay_prediction")

with mlflow.start_run() as run:
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 12)
    mlflow.log_param("features", FEATURES)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.sklearn.log_model(
        pipeline,
        artifact_path="model",
        registered_model_name="sklearn_flight_model",
    )
    run_id = run.info.run_id

client = mlflow.tracking.MlflowClient()
versions = client.get_latest_versions("sklearn_flight_model", stages=["None", "Staging"])
if versions:
    client.transition_model_version_stage(
        name="sklearn_flight_model",
        version=versions[-1].version,
        stage="Production",
        archive_existing_versions=True,
    )
    log.info(f"sklearn_flight_model v{versions[-1].version} → Production")

# Escribir referencia en Iceberg minio_catalog.models (usada por Spark Streaming)
log.info("Escribiendo referencia en minio_catalog.models...")
trained_at = datetime.now(timezone.utc).isoformat()
models_df = spark.createDataFrame([{
    "model_name": "sklearn_random_forest",
    "model_path": "s3a://flight-data/models/sklearn_random_forest.joblib",
    "trained_at": trained_at,
    "accuracy":   float(accuracy),
    "run_id":     run_id,
}])
spark.sql("CREATE NAMESPACE IF NOT EXISTS minio_catalog")
models_df.writeTo("minio_catalog.models").createOrReplace()
log.info("Referencia escrita en minio_catalog.models.")

log.info("=== Entrenamiento completado ===")
log.info(f"  Filas:      {row_count}")
log.info(f"  Accuracy:   {accuracy:.4f}")
log.info(f"  MLflow run: {run_id[:8]}")
log.info(f"  Modelo:     s3://flight-data/models/sklearn_random_forest.joblib")
log.info(f"  Registry:   sklearn_flight_model / Production")

spark.stop()
