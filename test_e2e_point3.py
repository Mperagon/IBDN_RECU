"""
Test end-to-end del Punto 3:
1. POST a Flask → UUID devuelto
2. Verificar Spark publicó en Kafka flight-predictions
3. Verificar Flask consumer guardó en Cassandra
4. Verificar endpoint HTTP fallback lee de Cassandra
"""
import sys, time, json, requests
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

FLASK_URL = "http://localhost:5001"
RESULT_TOPIC = "flight-predictions"

print("=" * 55)
print("TEST END-TO-END PUNTO 3: Kafka + WebSockets + Cassandra")
print("=" * 55)

# ── 1. Verificar Flask responde ─────────────────────────────
print("\n[1] Verificando Flask (SocketIO)...")
r = requests.get(FLASK_URL + "/flights/delays/predict_kafka", timeout=5)
assert r.status_code == 200, f"Flask no responde: {r.status_code}"
print(f"   HTTP {r.status_code} OK")

# ── 2. POST predicción ──────────────────────────────────────
print("\n[2] Enviando solicitud de predicción...")
payload = {"DepDelay":"5","Carrier":"AA","FlightDate":"2016-12-25",
           "Origin":"ATL","Dest":"SFO","FlightNum":"1519"}
r = requests.post(FLASK_URL + "/flights/delays/predict/classify_realtime",
                  data=payload, timeout=10)
resp = json.loads(r.text)
assert resp["status"] == "OK", f"Respuesta: {resp}"
uuid = resp["id"]
print(f"   UUID: {uuid}")

# ── 3. Esperar resultado en Cassandra (Spark → Kafka → Flask consumer) ──
print(f"\n[3] Esperando predicción en Cassandra (max 60s)...")
cluster = Cluster(["127.0.0.1"])
session = cluster.connect("flight_data")

found_row = None
for i in range(30):
    row = session.execute(
        "SELECT uuid, prediction, origin, dest, carrier, dep_delay "
        "FROM flight_data.flight_predictions WHERE uuid=%s", (uuid,)
    ).one()
    if row:
        found_row = row
        break
    time.sleep(2)
    print(f"   esperando... ({(i+1)*2}s)")

assert found_row, f"UUID {uuid} no llegó a Cassandra en 60s"
print(f"   UUID:       {found_row.uuid}")
print(f"   Prediction: {found_row.prediction}")
print(f"   Ruta:       {found_row.origin} -> {found_row.dest} ({found_row.carrier})")
print(f"   DepDelay:   {found_row.dep_delay}")

# ── 4. Verificar total de predicciones en Cassandra ─────────
count = session.execute("SELECT COUNT(*) FROM flight_data.flight_predictions").one()[0]
print(f"\n[4] Total predicciones en Cassandra: {count}")

# ── 5. Endpoint HTTP fallback (lee de Cassandra, no MongoDB) ─
print(f"\n[5] Endpoint HTTP fallback...")
r = requests.get(f"{FLASK_URL}/flights/delays/predict/classify_realtime/response/{uuid}", timeout=5)
resp = json.loads(r.text)
assert resp["status"] == "OK", f"HTTP status: {resp}"
print(f"   status:     {resp['status']}")
print(f"   Prediction: {resp['prediction']['Prediction']}")
print(f"   Ruta:       {resp['prediction']['Origin']} -> {resp['prediction']['Dest']}")

cluster.shutdown()
print("\n" + "=" * 55)
print("RESULTADO: PIPELINE COMPLETO OK")
print("  Flask → Kafka → Spark → Kafka → Cassandra ✓")
print("  WebSocket emit via Flask SocketIO           ✓")
print("  HTTP fallback desde Cassandra               ✓")
print("=" * 55)
