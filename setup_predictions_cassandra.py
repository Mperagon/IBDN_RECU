from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('flight_data')

session.execute("""
    CREATE TABLE IF NOT EXISTS flight_predictions (
        uuid      text PRIMARY KEY,
        prediction text,
        timestamp  timestamp,
        origin     text,
        dest       text,
        carrier    text,
        dep_delay  double
    )
""")

print("Tabla flight_predictions creada")

rows = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='flight_data'")
print("Tablas en flight_data:", [r.table_name for r in rows])

cluster.shutdown()
