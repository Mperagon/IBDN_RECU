# Práctica Creativa — Sistema de Predicción de Retrasos de Vuelos

Arquitectura de Big Data distribuida con Spark, Kafka, Cassandra, MinIO, Airflow y MLflow, todo orquestado con Docker Compose.

## Arquitectura

```
Flask (web) → Kafka → Spark Streaming → Cassandra
                 ↓
             Logstash → Elasticsearch → Kibana
                                   ↑
                            Modelo entrenado
                            (MinIO / local)
                                   ↑
                    Airflow DAG → Spark MLlib ← Iceberg (MinIO)
                                      ↓
                                   MLflow
```

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) y Docker Compose v2
- Git
- 12 GB de RAM mínimo disponibles para Docker (recomendado 16 GB+)

---

## Puesta en marcha

### 1. Clonar el repositorio

```bash
git clone https://github.com/Mperagon/IBDN.git practica_creativa
cd practica_creativa
```

### 2. Preparar directorios

```bash
mkdir -p models mlflow/artifacts logs
chmod 777 models
```

### 3. Descargar los datos de entrenamiento

```bash
bash resources/download_data.sh
```

Descarga `data/simple_flight_delay_features.jsonl.bz2` y `data/origin_dest_distances.jsonl`. Puede tardar varios minutos.

### 4. Levantar todos los servicios

```bash
docker compose up -d
```

Espera ~2-3 minutos a que todos los contenedores estén sanos. Comprueba el estado con:

```bash
docker compose ps
```

Todos deben aparecer como `Up` o `Up (healthy)`. Es normal que `airflow-init` aparezca como `Exited (0)` — es un contenedor de inicialización que solo corre una vez.

---

## Configuración inicial (solo la primera vez)

Ejecuta estos pasos en orden una única vez. En arranques posteriores no es necesario repetirlos.

### Paso 1 — Crear buckets en MinIO

```bash
docker exec -e MINIO_HOST=minio flask python3 /app/setup_minio_buckets.py
```

Si el comando anterior falla por dependencias, usa el cliente `mc`:

```bash
docker run --rm --network practica_creativa_bigdata-net \
  --entrypoint /bin/sh minio/mc \
  -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb local/flight-data && mc mb local/models"
```

Debe mostrar:
```
Bucket created successfully `local/flight-data`.
Bucket created successfully `local/models`.
```

### Paso 2 — Crear keyspace y tablas en Cassandra

Espera a que Cassandra esté `healthy` antes de ejecutar esto:

```bash
docker exec -i cassandra cqlsh < setup_cassandra.cql
```

### Paso 2.1 — Reiniciar Flask

Flask arranca junto con el resto de servicios pero se cae porque Cassandra aún no tiene el keyspace creado. Una vez ejecutado el paso anterior, reinícialo:

```bash
docker compose restart flask
```

Verifica que arrancó correctamente:

```bash
docker logs flask --tail 10
```

### Paso 3 — Importar distancias entre aeropuertos

Si el fichero no existe dentro del contenedor Flask, cópialo primero:

```bash
docker cp spark-master:/app/data/origin_dest_distances.jsonl /tmp/
docker exec flask mkdir -p /app/data
docker cp /tmp/origin_dest_distances.jsonl flask:/app/data/origin_dest_distances.jsonl
```

Luego importa los datos:

```bash
docker exec flask python3 -c "
import json
from cassandra.cluster import Cluster
cluster = Cluster(['cassandra'])
session = cluster.connect('flight_data')
stmt = session.prepare('INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (?, ?, ?)')
with open('/app/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        session.execute(stmt, (r['Origin'], r['Dest'], float(r['Distance'])))
print('Distancias importadas OK')
cluster.shutdown()
"
```

Importa ~4.696 registros.

### Paso 4 — Crear tabla Iceberg en MinIO

Este paso convierte los datos de entrenamiento al formato Parquet/Iceberg distribuido en MinIO. Tarda varios minutos.

```bash
docker exec -u root spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.driver.host=spark-master --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" /app/resources/create_iceberg_table.py 2>&1 | grep -E "Filas|total|OK|ERROR"
```

Debe terminar con:
```
Filas leidas: 457013
OK - Tabla Iceberg creada en s3a://flight-data/iceberg/flight_features
```

### Paso 5 — Entrenar el modelo con Airflow

1. Abre **Airflow** en `http://localhost:8081` (usuario: `admin`, contraseña: `admin`)
2. Activa el DAG `agile_data_science_batch_prediction_model_training`
3. Pulsa el botón **Trigger DAG** (▶)
4. Espera a que las 3 tareas estén en verde (~10-15 minutos):
   - `check_spark` — verifica que el cluster Spark funciona
   - `train_model` — entrena el Random Forest y guarda los modelos
   - `register_mlflow` — verifica las métricas en MLflow

Una vez completado, los modelos quedan guardados en `/app/models/` y en MinIO `s3a://models/`.

---

## Observabilidad con Kibana

Kibana permite visualizar en tiempo real todas las predicciones que pasan por Kafka.

### Configurar el Data View

Una vez que Kibana esté disponible en `http://localhost:5601`:

1. Ve a **Management → Stack Management → Data Views**
2. Pulsa **Create data view** y rellena:
   - **Name**: `flight-predictions`
   - **Index pattern**: `flight-predictions-*`
   - **Timestamp field**: `@timestamp`
3. Pulsa **Save data view**

### Ver predicciones en tiempo real

1. Haz una predicción en `http://localhost:5001/flights/delays/predict_kafka`
2. Ve a `http://localhost:5601/app/discover`
3. Selecciona el Data View `flight-predictions`
4. Verás cada predicción como un documento con los campos: `Carrier`, `Origin`, `Dest`, `Distance`, `DepDelay`

### Crear un dashboard

En Kibana → **Dashboards → Create dashboard** puedes añadir visualizaciones como:
- Histograma de predicciones por aerolínea (`Carrier`)
- Mapa de calor de rutas con más retrasos (`Origin` → `Dest`)
- Serie temporal de predicciones por minuto

---

## Uso normal

Con la configuración inicial ya hecha, para arrancar el sistema en futuras sesiones:

```bash
docker compose up -d
```

Abre el navegador en `http://localhost:5001/flights/delays/predict_kafka`, rellena el formulario y obtendrás la predicción en segundos.

---

## URLs de los servicios

| Servicio         | URL                     | Credenciales            |
|------------------|-------------------------|-------------------------|
| Flask (web)      | http://localhost:5001   | —                       |
| Airflow          | http://localhost:8081   | admin / admin           |
| MLflow           | http://localhost:5000   | —                       |
| Spark Master UI  | http://localhost:8080   | —                       |
| MinIO Console    | http://localhost:9001   | minioadmin / minioadmin |
| Kibana           | http://localhost:5601   | —                       |
| Elasticsearch    | http://localhost:9200   | —                       |

---

## Puertos expuestos

| Servicio       | Puerto     |
|----------------|------------|
| Flask          | 5001       |
| Airflow        | 8081       |
| MLflow         | 5000       |
| Spark Master   | 8080, 7077 |
| MinIO API      | 9000       |
| MinIO Web      | 9001       |
| Kafka          | 9092       |
| Cassandra      | 9042       |
| MongoDB        | 27017      |
| Elasticsearch  | 9200       |
| Kibana         | 5601       |

---

## Despliegue en Google Cloud (GCP)

### Requisitos en la VM
- VM con al menos 16 GB de RAM (e2-standard-4 o superior)
- Docker y Docker Compose instalados
- Zona recomendada: `europe-west1-b`

### 1. Arrancar la VM y conectarse

```bash
gcloud compute instances start big-data-vm --zone=europe-west1-b
gcloud compute ssh big-data-vm --zone=europe-west1-b
```

### 2. Subir el proyecto desde local

Desde tu máquina local, empaquetar y subir:

```bash
cd ~
tar --exclude='practica_creativa/env' \
    --exclude='practica_creativa/y' \
    --exclude='practica_creativa/.git' \
    --exclude='practica_creativa/spark-warehouse' \
    --exclude='practica_creativa/logs' \
    --exclude='practica_creativa/mlflow' \
    --exclude='practica_creativa/models' \
    -czf practica.tar.gz practica_creativa/
gcloud compute scp practica.tar.gz big-data-vm:~ --zone=europe-west1-b
```

En la VM, descomprimir y preparar directorios:

```bash
tar -xzf practica.tar.gz
cd practica_creativa
mkdir -p mlflow/artifacts models logs
chmod 777 models
```

### 3. Abrir puertos en el firewall de GCP

Ejecutar desde la máquina local (no desde la VM):

```bash
gcloud compute firewall-rules create bigdata-ports \
  --allow tcp:5001,tcp:8081,tcp:5000,tcp:8080,tcp:9001,tcp:5601,tcp:9200 \
  --network=default \
  --source-ranges=0.0.0.0/0
```

### 4. Obtener la IP externa de la VM

```bash
gcloud compute instances describe big-data-vm \
  --zone=europe-west1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

### 5. Levantar el sistema y setup inicial

Igual que en local, pero reemplazando `localhost` por la IP externa de la VM en todas las URLs.

Para crear los buckets de MinIO usa el cliente `mc` en lugar del script Python:

```bash
docker run --rm --network practica_creativa_bigdata-net \
  --entrypoint /bin/sh minio/mc \
  -c "mc alias set local http://minio:9000 minioadmin minioadmin && mc mb local/flight-data && mc mb local/models"
```

Si el fichero de distancias no está en el contenedor Flask, cópialo primero:

```bash
docker cp spark-master:/app/data/origin_dest_distances.jsonl /tmp/
docker exec flask mkdir -p /app/data
docker cp /tmp/origin_dest_distances.jsonl flask:/app/data/origin_dest_distances.jsonl
```

### 6. Acceder a los servicios

Sustituye `IP_EXTERNA` por la IP obtenida en el paso 4:

| Servicio      | URL                         |
|---------------|-----------------------------|
| Flask (web)   | http://IP_EXTERNA:5001      |
| Airflow       | http://IP_EXTERNA:8081      |
| MLflow        | http://IP_EXTERNA:5000      |
| Kibana        | http://IP_EXTERNA:5601      |
| MinIO Console | http://IP_EXTERNA:9001      |
| Spark UI      | http://IP_EXTERNA:8080      |

---

## Parar el sistema

```bash
docker compose down
```

Para parar y eliminar todos los datos (volúmenes):

```bash
docker compose down -v
```

> Si eliminas los volúmenes tendrás que repetir la configuración inicial completa.
