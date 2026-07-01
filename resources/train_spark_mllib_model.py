# !/usr/bin/env python

import sys, os

# Cargar JARs de Iceberg + S3A automáticamente aunque se invoque con "python3"
os.environ.setdefault(
    'PYSPARK_SUBMIT_ARGS',
    '--packages '
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,'
    'org.apache.hadoop:hadoop-aws:3.3.4,'
    'com.amazonaws:aws-java-sdk-bundle:1.12.262 '
    'pyspark-shell'
)

def main(base_path):

    try: base_path
    except NameError: base_path = "."
    if not base_path:
        base_path = "."

    APP_NAME = "train_spark_mllib_model.py"

    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS   = "minioadmin"
    MINIO_SECRET   = "minioadmin"
    MINIO_MODELS   = "s3a://models"

    try:
        sc and spark
    except (NameError, UnboundLocalError):
        try:
            import findspark
            findspark.init()
        except ImportError:
            pass
        import pyspark
        import pyspark.sql

        _driver_host = os.environ.get("SPARK_DRIVER_HOST")
        _b = pyspark.sql.SparkSession.builder \
            .appName(APP_NAME) \
            .master("spark://spark-master:7077")
        if _driver_host:
            _b = _b.config("spark.driver.host", _driver_host) \
                   .config("spark.driver.bindAddress", "0.0.0.0")
        spark = _b \
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.minio_catalog",
                    "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.minio_catalog.type", "hadoop") \
            .config("spark.sql.catalog.minio_catalog.warehouse",
                    "s3a://flight-data/iceberg") \
            .config("spark.hadoop.fs.s3a.endpoint",           MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key",         MINIO_ACCESS) \
            .config("spark.hadoop.fs.s3a.secret.key",         MINIO_SECRET) \
            .config("spark.hadoop.fs.s3a.path.style.access",  "true") \
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .getOrCreate()
        sc = spark.sparkContext

    spark.sparkContext.setLogLevel("WARN")

    from pyspark.sql.functions import lit, concat

    # Leer datos de entrenamiento desde MinIO/Iceberg
    print("Leyendo datos de entrenamiento desde MinIO/Iceberg...")
    features = spark.sql("SELECT * FROM minio_catalog.flight_features")
    features.first()

    null_counts = [(c, features.where(features[c].isNull()).count()) for c in features.columns]
    print(list(filter(lambda x: x[1] > 0, null_counts)))

    features_with_route = features.withColumn(
        'Route',
        concat(features.Origin, lit('-'), features.Dest)
    )
    features_with_route.show(6)

    from pyspark.ml.feature import Bucketizer

    splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
    arrival_bucketizer = Bucketizer(
        splits=splits,
        inputCol="ArrDelay",
        outputCol="ArrDelayBucket"
    )

    arrival_bucketizer.write().overwrite().save(
        "{}/arrival_bucketizer_2.0.bin".format(MINIO_MODELS))

    ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
    ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

    from pyspark.ml.feature import StringIndexer, VectorAssembler

    for column in ["Carrier", "Origin", "Dest", "Route"]:
        string_indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
        string_indexer_model = string_indexer.fit(ml_bucketized_features)
        ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)
        ml_bucketized_features = ml_bucketized_features.drop(column)

        string_indexer_model.write().overwrite().save(
            "{}/string_indexer_model_{}.bin".format(MINIO_MODELS, column))

    numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
    index_columns   = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]
    vector_assembler = VectorAssembler(
        inputCols=numeric_columns + index_columns,
        outputCol="Features_vec"
    )
    final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

    vector_assembler.write().overwrite().save(
        "{}/numeric_vector_assembler.bin".format(MINIO_MODELS))

    for column in index_columns:
        final_vectorized_features = final_vectorized_features.drop(column)

    final_vectorized_features.show()

    from pyspark.ml.classification import RandomForestClassifier
    rfc = RandomForestClassifier(
        featuresCol="Features_vec",
        labelCol="ArrDelayBucket",
        predictionCol="Prediction",
        maxBins=4657,
        maxMemoryInMB=1024
    )
    model = rfc.fit(final_vectorized_features)

    model.write().overwrite().save(
        "{}/spark_random_forest_classifier.flight_delays.5.0.bin".format(MINIO_MODELS))

    print("\n=== Modelos guardados en MinIO: {}/".format(MINIO_MODELS))

    predictions = model.transform(final_vectorized_features)

    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(
        predictionCol="Prediction",
        labelCol="ArrDelayBucket",
        metricName="accuracy"
    )
    accuracy = evaluator.evaluate(predictions)
    print("Accuracy = {}".format(accuracy))

    # MLflow tracking
    import mlflow
    mlflow_uri = os.environ.get('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("flight_delay_prediction")
    with mlflow.start_run():
        mlflow.log_param("maxBins", 4657)
        mlflow.log_param("maxMemoryInMB", 1024)
        mlflow.log_metric("accuracy", accuracy)
        print("MLflow: metricas registradas en {}".format(mlflow_uri))

    predictions.groupBy("Prediction").count().show()
    predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)

    # ── Modelo sklearn para Flink ────────────────────────────────────────────
    print("\nEntrenando modelo sklearn para Flink...")
    try:
        import io as _io, boto3 as _boto3, numpy as _np, pandas as _pd
        import joblib as _joblib
        from sklearn.pipeline import Pipeline as _Pipeline
        from sklearn.compose import ColumnTransformer as _ColumnTransformer
        from sklearn.preprocessing import OrdinalEncoder as _OrdinalEncoder
        from sklearn.ensemble import RandomForestClassifier as _SkRFC

        sk_cat = ["Carrier", "Origin", "Dest", "Route"]
        sk_num = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]

        sk_pdf = (
            features_with_route
            .select(sk_cat + sk_num + ["ArrDelay"])
            .dropna()
            .sample(False, 0.3, 42)
            .limit(200000)
            .toPandas()
        )

        sk_pdf["ArrDelayBucket"] = _pd.cut(
            sk_pdf["ArrDelay"],
            bins=[-_np.inf, -15, 0, 30, _np.inf],
            labels=[0, 1, 2, 3],
            right=False,
        ).astype(int)
        sk_pdf = sk_pdf.dropna(subset=["ArrDelayBucket"])

        sk_pre = _ColumnTransformer([
            ("cat", _OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), sk_cat),
            ("num", "passthrough", sk_num),
        ])
        sk_pipe = _Pipeline([
            ("preprocessor", sk_pre),
            ("classifier",   _SkRFC(n_estimators=100, max_depth=15, random_state=42, n_jobs=-1)),
        ])
        sk_pipe.fit(sk_pdf[sk_cat + sk_num], sk_pdf["ArrDelayBucket"])

        sk_buf = _io.BytesIO()
        _joblib.dump(sk_pipe, sk_buf)
        sk_buf.seek(0)
        sk_s3 = _boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        sk_s3.put_object(Bucket="models", Key="sklearn_flight_model.joblib", Body=sk_buf.getvalue())
        print("sklearn model guardado en MinIO: models/sklearn_flight_model.joblib")
    except Exception as sk_err:
        print("WARN: sklearn training failed: {}".format(sk_err))


# MLflow tracking ya integrado arriba
if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
