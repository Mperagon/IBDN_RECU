#!/bin/bash
# Script de arranque completo del sistema

echo "=== Arrancando sistema practica_creativa ==="

# ── Spark ────────────────────────────────────────────────────
echo "[1] Arrancando Spark..."
source ~/.sdkman/bin/sdkman-init.sh

nohup spark-submit \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /home/miguel/practica_creativa/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar \
  > /home/miguel/practica_creativa/logs/spark.log 2>&1 &

SPARK_PID=$!
echo "    Spark PID: $SPARK_PID"

# ── Flask ────────────────────────────────────────────────────
echo "[2] Arrancando Flask..."
source /home/miguel/practica_creativa/env/bin/activate
export PROJECT_HOME=/home/miguel/practica_creativa
cd /home/miguel/practica_creativa/resources/web

nohup python predict_flask.py \
  > /home/miguel/practica_creativa/logs/flask.log 2>&1 &

FLASK_PID=$!
echo "    Flask PID: $FLASK_PID"

# ── Guardar PIDs ─────────────────────────────────────────────
echo $SPARK_PID > /home/miguel/practica_creativa/logs/spark.pid
echo $FLASK_PID > /home/miguel/practica_creativa/logs/flask.pid

echo ""
echo "=== Esperando 15s para que arranquen... ==="
sleep 15

echo ""
echo "=== ESTADO FINAL ==="
echo -n "Cassandra: "; pgrep -c CassandraDaemon && echo "OK" || echo "NO CORRE"
echo -n "Kafka:     "; pgrep -f kafka.Kafka > /dev/null && echo "OK" || echo "NO CORRE"
echo -n "Spark:     "; pgrep -f MakePrediction > /dev/null && echo "OK" || echo "NO CORRE"
echo -n "Flask:     "; pgrep -f predict_flask > /dev/null && echo "OK" || echo "NO CORRE"

echo ""
echo "=== Ultimas lineas Flask ==="
tail -5 /home/miguel/practica_creativa/logs/flask.log

echo ""
echo "=== Listo. Abre: http://localhost:5001/flights/delays/predict_kafka ==="
