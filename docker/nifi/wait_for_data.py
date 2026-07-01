#!/usr/bin/env python3
"""
Espera hasta que NiFi haya cargado las distancias en Cassandra.
Se usa como dependencia del servicio Flask.
"""

import os
import time
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
MAX_ATTEMPTS = 60

cluster = None
for attempt in range(MAX_ATTEMPTS):
    try:
        if cluster is None:
            cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect("flight_data")
        count = session.execute(
            "SELECT COUNT(*) FROM origin_dest_distances"
        ).one()[0]
        if count > 0:
            print(f"OK: {count} distancias cargadas en Cassandra por NiFi")
            cluster.shutdown()
            break
        print(f"Esperando que NiFi cargue datos... ({attempt+1}/{MAX_ATTEMPTS}, registros actuales: {count})")
    except Exception as e:
        print(f"Esperando Cassandra/NiFi... ({attempt+1}/{MAX_ATTEMPTS}): {e}")
        cluster = None
    time.sleep(10)
else:
    raise RuntimeError("NiFi no cargó las distancias en el tiempo esperado")
