import json, os, time
from cassandra.cluster import Cluster

host = os.environ.get('CASSANDRA_HOST', 'cassandra')
cluster = None
for attempt in range(10):
    try:
        cluster = Cluster([host])
        session = cluster.connect('flight_data')
        break
    except Exception as e:
        print(f"Cassandra no disponible ({attempt+1}/10): {e}")
        time.sleep(5)

if cluster is None:
    raise RuntimeError("No se pudo conectar a Cassandra")

stmt = session.prepare(
    'INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (?, ?, ?)'
)
count = 0
with open('/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        session.execute(stmt, (r['Origin'], r['Dest'], float(r['Distance'])))
        count += 1

print(f'Distancias importadas OK: {count} registros')
cluster.shutdown()
