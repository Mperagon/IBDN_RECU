import os
import io
import json
import logging

KAFKA_BROKER    = os.environ.get("KAFKA_BROKER",    "kafka:9092")
MINIO_ENDPOINT  = os.environ.get("MINIO_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS    = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET    = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MODEL_BUCKET    = "models"
MODEL_KEY       = "sklearn_flight_model.joblib"

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer,
)
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import MapFunction


class FlightPredictFunction(MapFunction):
    """Carga el modelo sklearn desde MinIO y predice retrasos de vuelos."""

    def __init__(self):
        self._model = None

    def open(self, runtime_context):
        import boto3, joblib
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS,
            aws_secret_access_key=MINIO_SECRET,
        )
        buf = io.BytesIO()
        s3.download_fileobj(MODEL_BUCKET, MODEL_KEY, buf)
        buf.seek(0)
        self._model = joblib.load(buf)
        logging.info("Modelo sklearn cargado desde MinIO")

    def map(self, message):
        import pandas as pd
        try:
            data = json.loads(message)
            route = "{}-{}".format(data.get("Origin", ""), data.get("Dest", ""))
            row = pd.DataFrame([{
                "Carrier":    str(data.get("Carrier", "")),
                "Origin":     str(data.get("Origin", "")),
                "Dest":       str(data.get("Dest", "")),
                "Route":      route,
                "DepDelay":   float(data.get("DepDelay",   0) or 0),
                "Distance":   float(data.get("Distance",   0) or 0),
                "DayOfMonth": int(data.get("DayOfMonth",   1) or 1),
                "DayOfWeek":  int(data.get("DayOfWeek",    1) or 1),
                "DayOfYear":  int(data.get("DayOfYear",    1) or 1),
            }])
            prediction = float(self._model.predict(row)[0])
            result = {
                "UUID":       data.get("UUID", ""),
                "Prediction": prediction,
                "Origin":     data.get("Origin", ""),
                "Dest":       data.get("Dest", ""),
                "Carrier":    data.get("Carrier", ""),
                "DepDelay":   float(data.get("DepDelay", 0) or 0),
                "Timestamp":  data.get("Timestamp", ""),
            }
            return json.dumps(result)
        except Exception as exc:
            logging.error("Error en prediccion: %s", exc)
            return None


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics("flight-delay-ml-request")
        .set_group_id("flink-flight-predictor")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source - flight-delay-ml-request",
    )

    predictions = (
        stream
        .map(FlightPredictFunction(), output_type=Types.STRING())
        .filter(lambda x: x is not None)
    )

    kafka_sink = (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic("flight-predictions")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )

    predictions.sink_to(kafka_sink)
    env.execute("Flight Delay Prediction - Flink")


if __name__ == "__main__":
    main()
