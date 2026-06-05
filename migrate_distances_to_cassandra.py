"""
Migración de distancias de MongoDB a Cassandra.
Crea el keyspace y tabla si no existen, luego importa los datos.
"""
import sys
from cassandra.cluster import Cluster
from pymongo import MongoClient

CASSANDRA_HOST = '127.0.0.1'
KEYSPACE = 'flight_data'

def setup_cassandra():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS flight_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    print(f"Keyspace '{KEYSPACE}' listo.")

    session.set_keyspace(KEYSPACE)

    session.execute("""
        CREATE TABLE IF NOT EXISTS origin_dest_distances (
            origin text,
            dest   text,
            distance double,
            PRIMARY KEY ((origin, dest))
        )
    """)
    print("Tabla 'origin_dest_distances' lista.")
    return cluster, session


def migrate_distances(session):
    mongo = MongoClient()
    collection = mongo.agile_data_science.origin_dest_distances

    total = collection.count_documents({})
    print(f"Documentos encontrados en MongoDB: {total}")

    insert_stmt = session.prepare(
        "INSERT INTO flight_data.origin_dest_distances (origin, dest, distance) VALUES (?, ?, ?)"
    )

    count = 0
    errors = 0
    for doc in collection.find({}, {'_id': 0, 'Origin': 1, 'Dest': 1, 'Distance': 1}):
        try:
            session.execute(insert_stmt, (doc['Origin'], doc['Dest'], float(doc['Distance'])))
            count += 1
            if count % 500 == 0:
                print(f"  Insertados {count}/{total}...")
        except Exception as e:
            errors += 1
            print(f"  Error en {doc}: {e}", file=sys.stderr)

    mongo.close()
    print(f"\nMigración completada: {count} registros insertados, {errors} errores.")
    return count


def verify(session):
    row = session.execute("SELECT COUNT(*) FROM flight_data.origin_dest_distances").one()
    print(f"Verificación — filas en Cassandra: {row[0]}")

    sample = session.execute(
        "SELECT origin, dest, distance FROM flight_data.origin_dest_distances LIMIT 5"
    )
    print("Muestra de 5 registros:")
    for r in sample:
        print(f"  {r.origin} -> {r.dest}: {r.distance:.1f} millas")


if __name__ == '__main__':
    cluster, session = setup_cassandra()
    migrate_distances(session)
    verify(session)
    cluster.shutdown()
    print("\nListo.")
