"""
Verifica que predict_flask.py puede importar y conectar a Cassandra sin errores.
No arranca el servidor, solo comprueba las conexiones y la lógica de distancias.
"""
import sys
sys.path.insert(0, '/home/miguel/practica_creativa/resources/web')

import os
os.environ['PROJECT_HOME'] = '/home/miguel/practica_creativa'

import predict_utils
from cassandra.cluster import Cluster
from pymongo import MongoClient

print("1. Conectando a Cassandra...")
cluster, session = predict_utils.get_cassandra_session()
print("   OK")

print("2. Verificando tabla...")
rows = list(session.execute("SELECT COUNT(*) FROM flight_data.origin_dest_distances"))
print(f"   {rows[0][0]} filas en Cassandra")

print("3. Probando consultas de distancia...")
pares = [('ATL','SFO'), ('JFK','LAX'), ('ORD','MIA'), ('DFW','SEA'), ('BOS','DEN')]
for orig, dest in pares:
    d = predict_utils.get_flight_distance(session, orig, dest)
    print(f"   {orig} -> {dest}: {d:.0f} millas")

print("4. Verificando MongoDB sigue disponible para predicciones...")
mongo = MongoClient()
count = mongo.agile_data_science.flight_delay_ml_response.count_documents({})
print(f"   flight_delay_ml_response: {count} documentos")
mongo.close()

cluster.shutdown()
print("\nTodo OK — Flask puede arrancar con Cassandra integrado.")
