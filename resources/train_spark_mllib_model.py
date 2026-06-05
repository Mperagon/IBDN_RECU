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

        spark = pyspark.sql.SparkSession.builder \
            .appName(APP_NAME) \
            .master("spark://spark-master:7077") \
            .config("spark.driver.host", "spark-master") \
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

    # Guardar bucketizer — local Y MinIO
    arrival_bucketizer.write().overwrite().save(
        "{}/models/arrival_bucketizer_2.0.bin".format(base_path))
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

        # Guardar string indexer — local Y MinIO
        string_indexer_model.write().overwrite().save(
            "{}/models/string_indexer_model_{}.bin".format(base_path, column))
        string_indexer_model.write().overwrite().save(
            "{}/string_indexer_model_{}.bin".format(MINIO_MODELS, column))

    numeric_columns = ["DepDelay", "Distance", "DayOfMonth", "DayOfWeek", "DayOfYear"]
    index_columns   = ["Carrier_index", "Origin_index", "Dest_index", "Route_index"]
    vector_assembler = VectorAssembler(
        inputCols=numeric_columns + index_columns,
        outputCol="Features_vec"
    )
    final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

    # Guardar vector assembler — local Y MinIO
    vector_assembler.write().overwrite().save(
        "{}/models/numeric_vector_assembler.bin".format(base_path))
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

    # Guardar modelo RF — local Y MinIO
    model.write().overwrite().save(
        "{}/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(base_path))
    model.write().overwrite().save(
        "{}/spark_random_forest_classifier.flight_delays.5.0.bin".format(MINIO_MODELS))

    print("\n=== Modelos guardados ===")
    print("  Local: {}/models/".format(base_path))
    print("  MinIO: {}/".format(MINIO_MODELS))

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
    mlflow_uri = os.environ.get('MLFLOW_TRACKING_URI', 'http://localhost:5000')
    mlflow.set_tracking_uri(mlflow_uri)
    mlflow.set_experiment("flight_delay_prediction")
    with mlflow.start_run():
        mlflow.log_param("maxBins", 4657)
        mlflow.log_param("maxMemoryInMB", 1024)
        mlflow.log_metric("accuracy", accuracy)
        print("MLflow: metricas registradas en {}".format(mlflow_uri))

    predictions.groupBy("Prediction").count().show()
    predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)


# MLflow tracking ya integrado arriba
if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else ".")
