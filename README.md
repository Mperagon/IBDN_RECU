# Práctica Creativa — Sistema de Predicción de Retrasos de Vuelos

Arquitectura de Big Data completamente distribuida con Spark, Kafka, Cassandra, MinIO, Airflow y MLflow, todo orquestado con Docker Compose.

## Arquitectura

```
Flask (web) → Kafka → Spark Streaming → Cassandra
                 ↓
             Logstash → Elasticsearch → Kibana
                                   ↑
                            Modelo entrenado
                            (MinIO s3a://)
                                   ↑
                    Airflow DAG → Spark MLlib ← Iceberg (MinIO)
                                      ↓
                                   MLflow (PostgreSQL + MinIO)
```

### Modo distribuido

- **Modelos**: guardados y leídos exclusivamente desde MinIO (`s3a://models/`)
- **JAR de inferencia**: servido desde MinIO por HTTP (`http://minio:9000/models/...`)
- **Script de entrenamiento**: servido desde MinIO por HTTP (`http://minio:9000/flight-data/scripts/...`)
- **Spark Streaming**: `--deploy-mode cluster` — el driver corre en un worker del cluster
- **Checkpoint Kafka**: `s3a://models/checkpoints/` (persiste entre reinicios)
- **MLflow**: PostgreSQL como backend + artefactos en MinIO
- **Cassandra**: keyspace e tablas creados automáticamente por `cassandra-init` antes de que Flask arranque

## Requisitos previos

- [Docker](https://docs.docker.com/get-docker/) y Docker Compose v2
- Git
- 16 GB de RAM mínimo disponibles para Docker

---

## Puesta en marcha

### 1. Clonar el repositorio

```bash
git clone https://github.com/Mperagon/IBDN.git practica_creativa
cd practica_creativa
```

### 2. Descargar los datos de entrenamiento

```bash
bash resources/download_data.sh
```

Descarga `data/simple_flight_delay_features.jsonl.bz2` y `data/origin_dest_distances.jsonl`. Puede tardar varios minutos.

### 3. Levantar todos los servicios

```bash
docker compose up -d --build
```

El flag `--build` compila la imagen personalizada de MLflow (con psycopg2 y boto3). Solo es necesario la primera vez o cuando cambie `docker/Dockerfile.mlflow`.

Espera ~3-4 minutos a que todos los contenedores estén sanos:

```bash
docker compose ps
```

Los siguientes contenedores aparecerán como `Exited (0)` — es normal, son de inicialización:
- `airflow-init` — crea el esquema de base de datos de Airflow
- `cassandra-init` — crea el keyspace `flight_data` y las tablas en Cassandra

Flask **no arrancará** hasta que `cassandra-init` complete con éxito.

---

## Configuración inicial (solo la primera vez)

Ejecuta estos pasos en orden una única vez. En arranques posteriores no es necesario repetirlos.

### Paso 1 — Crear buckets en MinIO

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

### Paso 2 — Importar distancias entre aeropuertos en Cassandra

```bash
docker exec spark-master python3 -c "
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

### Paso 3 — Configurar MinIO para modo distribuido

Sube el JAR de inferencia y el script de entrenamiento a MinIO, y configura los buckets con acceso de lectura pública para que Spark los descargue por HTTP:

```bash
docker exec spark-master python3 /app/setup_minio_distributed.py
```

Debe mostrar:
```
=== 1. Subiendo JAR de inferencia a MinIO ===
  OK: .../flight_prediction_2.12-0.1.jar -> s3://models/flight_prediction_2.12-0.1.jar
=== 2. Subiendo script de entrenamiento a MinIO ===
  OK: .../train_spark_mllib_model.py -> s3://flight-data/scripts/train_spark_mllib_model.py
=== 3. Haciendo bucket 'models' de lectura publica ===
  OK: bucket 'models' set to public read
=== 4. Haciendo bucket 'flight-data' de lectura publica ===
  OK: bucket 'flight-data' set to public read
```

### Paso 4 — Crear tabla Iceberg en MinIO

Convierte los datos de entrenamiento al formato Parquet/Iceberg en MinIO. Tarda varios minutos:

```bash
docker exec -u root spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /app/resources/create_iceberg_table.py 2>&1 | grep -E "Filas|total|OK|ERROR"
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
   - `train_model` — entrena el Random Forest y guarda los modelos **exclusivamente en MinIO**
   - `register_mlflow` — verifica las métricas en MLflow

Una vez completado, los modelos quedan en `s3a://models/` y las métricas en MLflow (PostgreSQL + MinIO).

### Verificar el modo distribuido

Comprueba en la **Spark Master UI** (`http://localhost:8080`) que el job de inferencia aparece bajo **Drivers** (no bajo Applications). Esto confirma que el driver corre en el cluster con `--deploy-mode cluster`.

```bash
docker compose logs spark-job --tail 20
```

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

Esta sección explica cómo crear una VM en GCP desde cero, instalar Docker y desplegar el sistema completo.

---

### Paso 1 — Instalar Google Cloud SDK en tu máquina local

Si no tienes `gcloud` instalado:

```bash
# Linux / WSL
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
```

Durante `gcloud init` te pedirá que inicies sesión con tu cuenta de Google y que selecciones el proyecto de GCP.

---

### Paso 2 — Crear la VM desde cero

Ejecuta esto desde tu máquina local. Crea una VM con 16 GB de RAM y 50 GB de disco:

```bash
gcloud compute instances create big-data-vm \
  --zone=europe-west1-b \
  --machine-type=e2-standard-4 \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-standard \
  --tags=bigdata
```

> `e2-standard-4` = 4 vCPUs y 16 GB de RAM. Mínimo recomendado para este sistema.

---

### Paso 3 — Abrir los puertos en el firewall de GCP

Ejecuta desde tu máquina local (no desde la VM):

```bash
gcloud compute firewall-rules create bigdata-ports \
  --allow tcp:5001,tcp:8081,tcp:5000,tcp:8080,tcp:9001,tcp:5601,tcp:9200 \
  --network=default \
  --source-ranges=0.0.0.0/0 \
  --target-tags=bigdata
```

Esto abre los puertos de Flask, Airflow, MLflow, Spark, MinIO y Kibana al exterior.

---

### Paso 4 — Conectarse a la VM e instalar Docker

```bash
# Conectarse a la VM
gcloud compute ssh big-data-vm --zone=europe-west1-b
```

Una vez dentro de la VM, instalar Docker:

```bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
docker --version
docker compose version
```

---

### Paso 5 — Subir el proyecto a la VM

Desde tu **máquina local**, empaquetar el proyecto y subirlo:

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

En la **VM**, descomprimir:

```bash
tar -xzf practica.tar.gz
cd practica_creativa
```

---

### Paso 6 — Levantar el sistema

```bash
docker compose up -d --build
```

Espera 3-4 minutos y comprueba que todos los contenedores están arriba:

```bash
docker compose ps
```

---

### Paso 7 — Configuración inicial (igual que en local)

Sigue los mismos pasos de la sección **Configuración inicial** de este README: crear buckets en MinIO, importar distancias en Cassandra, configurar MinIO distribuido, crear tabla Iceberg y entrenar el modelo con Airflow.

---

### Paso 8 — Obtener la IP externa y acceder

```bash
gcloud compute instances describe big-data-vm \
  --zone=europe-west1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

Sustituye `IP_EXTERNA` por la IP obtenida:

| Servicio      | URL                         | Credenciales            |
|---------------|-----------------------------|-------------------------|
| Flask (web)   | http://IP_EXTERNA:5001      | —                       |
| Airflow       | http://IP_EXTERNA:8081      | admin / admin           |
| MLflow        | http://IP_EXTERNA:5000      | —                       |
| Spark UI      | http://IP_EXTERNA:8080      | —                       |
| MinIO Console | http://IP_EXTERNA:9001      | minioadmin / minioadmin |
| Kibana        | http://IP_EXTERNA:5601      | —                       |

---

### Paso 9 — Arrancar y parar la VM

```bash
# Arrancar la VM (desde local)
gcloud compute instances start big-data-vm --zone=europe-west1-b

# Parar la VM cuando no la uses (evita costes)
gcloud compute instances stop big-data-vm --zone=europe-west1-b
```

> Parar la VM detiene el cobro por cómputo. Los datos del disco persisten.
> Cuando la vuelvas a arrancar, solo necesitas `docker compose up -d` dentro de la VM.

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
