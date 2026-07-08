#!/usr/bin/env python3
"""
Configura Apache NiFi via REST API para cargar origin_dest_distances.jsonl
desde MinIO (Data Lakehouse) en Cassandra con inserciones en BATCH.

Flow: GenerateFlowFile → InvokeHTTP (MinIO) → SplitText → EvaluateJsonPath
                       → ReplaceText (CQL INSERT) → MergeContent (100 registros)
                       → ReplaceText (BEGIN BATCH) → ReplaceText (APPLY BATCH)
                       → PutCassandraQL (ejecuta BATCH de 100 inserts)
"""

import requests
import time
import sys

NIFI_URL   = "http://nifi:8080/nifi-api"
MINIO_URL  = "http://minio:9000/flight-data/raw/origin_dest_distances.jsonl"
BATCH_SIZE = 100
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


def cleanup_existing_flow(pg_id):
    """Para y elimina todos los procesadores y conexiones existentes."""
    r = requests.get(f"{NIFI_URL}/process-groups/{pg_id}/processors", headers=HDR)
    if not r.ok:
        return
    processors = r.json().get("processors", [])
    if not processors:
        return

    print("  Limpiando flow existente...")

    # Parar todos los procesadores
    for proc in processors:
        requests.put(
            f"{NIFI_URL}/processors/{proc['id']}/run-status",
            headers=HDR,
            json={
                "revision": {"version": proc["revision"]["version"]},
                "state": "STOPPED",
                "disconnectedNodeAcknowledged": False,
            },
        )
    time.sleep(3)

    # Vaciar colas y eliminar conexiones
    r = requests.get(f"{NIFI_URL}/process-groups/{pg_id}/connections", headers=HDR)
    if r.ok:
        for conn in r.json().get("connections", []):
            conn_id = conn["id"]
            requests.post(f"{NIFI_URL}/flowfile-queues/{conn_id}/drop-requests", headers=HDR)
            time.sleep(0.3)
            requests.delete(
                f"{NIFI_URL}/connections/{conn_id}?version={conn['revision']['version']}",
                headers=HDR,
            )
    time.sleep(1)

    # Eliminar procesadores
    for proc in processors:
        r_get = requests.get(f"{NIFI_URL}/processors/{proc['id']}", headers=HDR)
        if r_get.ok:
            version = r_get.json()["revision"]["version"]
            requests.delete(
                f"{NIFI_URL}/processors/{proc['id']}?version={version}", headers=HDR
            )

    print("  Flow existente eliminado.")


def create_processor(pg_id, proc_type, name, properties, position,
                     auto_terminate=None, concurrent_tasks=1):
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
                "concurrentlySchedulableTaskCount": concurrent_tasks,
                "autoTerminatedRelationships": auto_terminate or [],
            },
        },
    }
    r = requests.post(
        f"{NIFI_URL}/process-groups/{pg_id}/processors", headers=HDR, json=body
    )
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
    r = requests.post(
        f"{NIFI_URL}/process-groups/{pg_id}/connections", headers=HDR, json=body
    )
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
    r = requests.put(
        f"{NIFI_URL}/processors/{proc_id}/run-status", headers=HDR, json=body
    )
    if not r.ok:
        print(f"  ERROR iniciando {proc_id}: {r.status_code} {r.text[:300]}")
        r.raise_for_status()
    print(f"  Procesador iniciado: {proc_id}")


def main():
    print("=== Configurando flujo NiFi: MinIO → Cassandra (BATCH) ===\n")
    print(f"Fuente: {MINIO_URL}")
    print(f"Batch size: {BATCH_SIZE} registros por INSERT BATCH\n")

    wait_for_nifi()
    pg_id = get_root_pg_id()
    print(f"Root Process Group: {pg_id}\n")

    cleanup_existing_flow(pg_id)

    print("Creando procesadores...")

    # 1. GenerateFlowFile — dispara el flujo cada hora
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

    # 3. SplitText — una línea JSON por FlowFile
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

    # 5. ReplaceText — genera el CQL INSERT con punto y coma + salto de línea.
    # El \n al final permite que MergeContent concatene sin demarcador adicional.
    cql = (
        "INSERT INTO origin_dest_distances(origin, dest, distance) "
        "VALUES ('${origin}', '${dest}', ${distance});\n"
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

    # 6. MergeContent — agrupa BATCH_SIZE FlowFiles en un solo FlowFile
    # Cada FlowFile fusionado contiene BATCH_SIZE sentencias INSERT.
    # Cada INSERT ya termina en ";\n" (via ReplaceText anterior), así que
    # no necesitamos demarcador adicional — usamos "Do Not Use Delimiters".
    merge_content = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.MergeContent",
        f"MergeContent - agrupar {BATCH_SIZE} registros",
        {
            "Merge Strategy": "Bin-Packing Algorithm",
            "Merge Format": "Binary Concatenation",
            "Minimum Number of Entries": str(BATCH_SIZE),
            "Maximum Number of Entries": str(BATCH_SIZE),
            "Max Bin Age": "30 sec",
            "Delimiter Strategy": "Do Not Use Delimiters",
        },
        position=(1500, 0),
        auto_terminate=["failure", "original"],
    )

    # 7. ReplaceText — añade "BEGIN BATCH\n" al inicio
    prepend_batch = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.ReplaceText",
        "ReplaceText - BEGIN BATCH",
        {
            "Replacement Value": "BEGIN BATCH\n",
            "Replacement Strategy": "Prepend",
            "Evaluation Mode": "Entire text",
            "Character Set": "UTF-8",
        },
        position=(1800, 0),
        auto_terminate=["failure"],
    )

    # 8. ReplaceText — añade "\nAPPLY BATCH;" al final
    append_batch = create_processor(
        pg_id,
        "org.apache.nifi.processors.standard.ReplaceText",
        "ReplaceText - APPLY BATCH",
        {
            "Replacement Value": "\nAPPLY BATCH;",
            "Replacement Strategy": "Append",
            "Evaluation Mode": "Entire text",
            "Character Set": "UTF-8",
        },
        position=(2100, 0),
        auto_terminate=["failure"],
    )

    # 9. PutCassandraQL — ejecuta el BATCH completo (4 tareas concurrentes)
    put_cassandra = create_processor(
        pg_id,
        "org.apache.nifi.processors.cassandra.PutCassandraQL",
        "PutCassandraQL - insertar BATCH en Cassandra",
        {
            "Cassandra Contact Points": "cassandra:9042",
            "Keyspace": "flight_data",
        },
        position=(2400, 0),
        auto_terminate=["success", "failure", "retry"],
        concurrent_tasks=4,
    )

    print("\nCreando conexiones...")
    create_connection(pg_id, generate["id"],      invoke_http["id"],   ["success"])
    create_connection(pg_id, invoke_http["id"],   split_text["id"],    ["Response"])
    create_connection(pg_id, split_text["id"],    evaluate_json["id"], ["splits"])
    create_connection(pg_id, evaluate_json["id"], replace_text["id"],  ["matched"])
    create_connection(pg_id, replace_text["id"],  merge_content["id"], ["success"])
    create_connection(pg_id, merge_content["id"], prepend_batch["id"], ["merged"])
    create_connection(pg_id, prepend_batch["id"], append_batch["id"],  ["success"])
    create_connection(pg_id, append_batch["id"],  put_cassandra["id"], ["success"])

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
    start_processor(append_batch["id"])
    start_processor(prepend_batch["id"])
    start_processor(merge_content["id"])
    start_processor(replace_text["id"])
    start_processor(evaluate_json["id"])
    start_processor(split_text["id"])
    start_processor(invoke_http["id"])
    start_processor(generate["id"])

    print(f"\n=== Flujo NiFi activo (BATCH mode: {BATCH_SIZE} registros/BATCH) ===")
    print(f"    Fuente: {MINIO_URL}")
    print(f"    ~4700 registros → ~{4700 // BATCH_SIZE} BATCHes → Cassandra")


if __name__ == "__main__":
    main()
