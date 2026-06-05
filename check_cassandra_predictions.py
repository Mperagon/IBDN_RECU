from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('flight_data')

rows = list(session.execute("SELECT uuid, prediction, origin, dest, carrier, dep_delay FROM flight_data.flight_predictions"))

print(f"Predicciones almacenadas en Cassandra: {len(rows)}")
for r in rows:
    print(f"  UUID: {r.uuid[:8]}...  Pred: {r.prediction}  Ruta: {r.origin}->{r.dest} ({r.carrier})  Delay: {r.dep_delay}")

cluster.shutdown()
