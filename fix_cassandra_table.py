from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('flight_data')

# Recrear tabla con prediction como double
session.execute("DROP TABLE IF EXISTS flight_data.flight_predictions")
session.execute("""
    CREATE TABLE flight_predictions (
        uuid       text PRIMARY KEY,
        prediction double,
        timestamp  timestamp,
        origin     text,
        dest       text,
        carrier    text,
        dep_delay  double
    )
""")

rows = list(session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='flight_data'"))
print("Tablas en flight_data:", [r.table_name for r in rows])
print("OK - tabla flight_predictions con prediction:double")
cluster.shutdown()
