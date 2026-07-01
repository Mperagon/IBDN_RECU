#!/usr/bin/env python3
"""
Configura Apache NiFi via REST API para cargar origin_dest_distances.jsonl
desde MinIO (Data Lakehouse) en Cassandra.

NiFi en modo HTTP sin TLS -> API accesible sin autenticación.

Flow: GenerateFlowFile → InvokeHTTP (MinIO) → SplitText → EvaluateJsonPath
                       → ReplaceText (CQL) → PutCassandraQL
"""

import requests
import time
import sys

NIFI_URL  = "http://nifi:8080/nifi-api"
MINIO_URL = "http://minio:9000/flight-data/raw/origin_dest_distances.jsonl"
HDR = {"Content-Type": "application/json"}


def wait_for_nifi():
    print("Esperando que NiFi esté listo...")
    for attempt in range(40):
        try:
            r = requests.get(f"{NIFI_URL}/system-diagnostics", timeout=10)
            if r.status_code == 200:
                print("NiFi listo.")
                return
            print(f"  NiFi respondió {r.status_code} ({attempt+1}/40)")
        except Exception as e:
            print(f"  Esperando NiFi... ({attempt+1}/40): {e}")
        time.sleep(15)
    sys.exit("ERROR: NiFi no respondió a tiempo")


def get_root_pg_id():
    r = requests.get(f"{NIFI_URL}/flow/process-groups/root", headers=HDR)
    r.raise_for_status()
    return r.json()["processGroupFlow"]["id"]


def create_processor(pg_id, proc_type, name, properties, position, auto_terminate=None):
    body = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": position[0], "y": position[1]},
            "config": {
                "properties": properties,
                "schedulingStrategy": "TIMER_DRIVEN",
                "schedulingPeriod": "1 sec",
                "autoTerminatedRelationships": auto_terminate or [],
            },
        },
    }
    r = requests.post(f"{NIFI_URL}/process-groups/{pg_id}/processors", headers=HDR, json=body)
    if not r.ok:
        print(f"  ERROR creando {name}: {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    proc = r.json()
    print(f"  Procesador creado: {name}  id={proc['id']}")
    return proc


def create_connection(pg_id, src_id, dst_id, relationships):
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": relationships,
            "backPressureDataSizeThreshold": "1 GB",
            "backPressureObjectThreshold": 10000,
            "flowFileExpiration": "0 sec",
        },
    }
    r = requests.post(f"{NIFI_URL}/process-groups/{pg_id}/connections", headers=HDR, json=body)
    if not r.ok:
        print(f"  ERROR creando conexión {relationships}: {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    print(f"  Conexión creada: {relationships}")
    return r.json()


def get_processor_state(proc_id):
    r = requests.get(f"{NIFI_URL}/processors/{proc_id}", headers=HDR)
    r.raise_for_status()
    data = r.json()
    return data["revision"]["version"], data["component"].get("validationErrors", [])


def start_processor(proc_id):
    version, errors = get_processor_state(proc_id)
    if errors:
        print(f"  ADVERTENCIA validación {proc_id}: {errors}")
    body = {
        "revision": {"version": version},
        "state": "RUNNING",
        "disconnectedNodeAcknowledged": False,
    }
    r = requests.put(f"{NIFI_URL}/processors/{proc_id}/run-status", headers=HDR, json=body)
    if not r.ok:
        print(f"  ERROR iniciando {proc_id}: {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    print(f"  Procesador iniciado: {proc_id}")


def main():
    print("=== Configurando flujo NiFi: MinIO → Cassandra ===\n")
    print(f"Fuente: {MINIO_URL}\n")

    wait_for_nifi()
    pg_id = get_root_pg_id()
    print(f"Root Process Group: {pg_id}\n")

    print("Creando procesadores...")

    # 1. GenerateFlowFile — dispara el flujo periódicamente (cada hora)
    # El UPSERT de Cassandra hace que re-ejecutar sea idempotente
    generate = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.GenerateFlowFile",
        "GenerateFlowFile - disparador",
        {
            "File Size": "0 B",
            "Batch Size": "1",
            "Data Format": "Text",
            "Unique FlowFiles": "false",
        },
        position=(0, 0),
    )
    # Cambiamos el scheduling a 1 hora para que no genere constantemente
    # (actualizamos después de crear)

    # 2. InvokeHTTP — descarga el fichero desde MinIO (Data Lakehouse)
    invoke_http = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.InvokeHTTP",
        "InvokeHTTP - descargar desde MinIO",
        {
            "HTTP Method": "GET",
            "Remote URL": MINIO_URL,
            "Connection Timeout": "10 secs",
            "Read Timeout": "30 secs",
        },
        position=(300, 0),
        auto_terminate=["Original", "Retry", "No Retry", "Failure"],
    )

    # 3. SplitText — una línea por FlowFile
    split_text = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.SplitText",
        "SplitText - una línea por registro",
        {
            "Line Split Count": "1",
            "Header Line Count": "0",
            "Remove Trailing Newlines": "true",
        },
        position=(600, 0),
        auto_terminate=["original", "failure"],
    )

    # 4. EvaluateJsonPath — extrae Origin, Dest, Distance como atributos
    evaluate_json = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        "EvaluateJsonPath - extraer campos",
        {
            "Destination": "flowfile-attribute",
            "Return Type": "scalar",
            "origin": "$.Origin",
            "dest": "$.Dest",
            "distance": "$.Distance",
        },
        position=(900, 0),
        auto_terminate=["failure", "unmatched"],
    )

    # 5. ReplaceText — genera el CQL como contenido del FlowFile
    # PutCassandraQL lee el CQL del CONTENIDO del FlowFile.
    # ReplaceText evalúa la Expression Language sustituyendo ${origin} etc.
    cql = (
        "INSERT INTO origin_dest_distances(origin, dest, distance) "
        "VALUES ('${origin}', '${dest}', ${distance})"
    )
    replace_text = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.ReplaceText",
        "ReplaceText - generar CQL INSERT",
        {
            "Replacement Value": cql,
            "Replacement Strategy": "Always Replace",
            "Evaluation Mode": "Entire text",
            "Character Set": "UTF-8",
        },
        position=(1200, 0),
        auto_terminate=["failure"],
    )

    # 6. PutCassandraQL — ejecuta el CQL del contenido del FlowFile en Cassandra
    put_cassandra = create_processor(
        pg_id,
        "org.apache.nifi.processors.cassandra.PutCassandraQL",
        "PutCassandraQL - insertar distancias",
        {
            "Cassandra Contact Points": "cassandra:9042",
            "Keyspace": "flight_data",
        },
        position=(1500, 0),
        auto_terminate=["success", "failure", "retry"],
    )

    print("\nCreando conexiones...")
    create_connection(pg_id, generate["id"],      invoke_http["id"],   ["success"])
    create_connection(pg_id, invoke_http["id"],   split_text["id"],    ["Response"])
    create_connection(pg_id, split_text["id"],    evaluate_json["id"], ["splits"])
    create_connection(pg_id, evaluate_json["id"], replace_text["id"],  ["matched"])
    create_connection(pg_id, replace_text["id"],  put_cassandra["id"], ["success"])

    # Actualizar GenerateFlowFile a scheduling de 1 hora
    version, _ = get_processor_state(generate["id"])
    r = requests.put(
        f"{NIFI_URL}/processors/{generate['id']}",
        headers=HDR,
        json={
            "revision": {"version": version},
            "component": {
                "id": generate["id"],
                "config": {
                    "schedulingPeriod": "3600 sec",
                    "schedulingStrategy": "TIMER_DRIVEN",
                },
            },
        },
    )
    if r.ok:
        print("  GenerateFlowFile scheduling actualizado a 3600 sec (1 hora)")

    print("\nArrancando procesadores...")
    time.sleep(2)
    start_processor(put_cassandra["id"])
    start_processor(replace_text["id"])
    start_processor(evaluate_json["id"])
    start_processor(split_text["id"])
    start_processor(invoke_http["id"])
    start_processor(generate["id"])

    print(f"\n=== Flujo NiFi activo. Leyendo distancias desde MinIO → Cassandra ===")
    print(f"    Fuente: {MINIO_URL}")


if __name__ == "__main__":
    main()
