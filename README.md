# Práctica Creativa — Sistema de Predicción de Retrasos de Vuelos

Arquitectura de Big Data completamente distribuida con Spark, Kafka, Cassandra, MinIO, NiFi, Airflow y MLflow, todo orquestado con Docker Compose.

## Arquitectura

```
Flask (web) → Kafka → Flink (predicción RT) → Cassandra
                 ↓                                  ↑
             Logstash → Elasticsearch           NiFi (carga distancias)
                 ↓           ↓                      ↑
               Kibana    (visualización)     origin_dest_distances.jsonl
                                   ↑
                         Modelo sklearn (MinIO s3a://models/)
                                   ↑
                    Airflow DAG → Spark MLlib ← Iceberg (MinIO)
                                      ↓
                                   MLflow (PostgreSQL + MinIO)
```

### Modo 100% distribuido

Todo el estado persiste en servicios distribuidos, sin dependencias de disco local:

| Componente | Dónde se almacena |
|---|---|
| Modelos Random Forest | `s3a://models/` (MinIO) |
| JAR de inferencia Spark | `http://minio:9000/models/...` (HTTP público) |
| Script de entrenamiento | `s3a://flight-data/scripts/...` (MinIO) |
| Datos brutos (JSONL) | `s3a://flight-data/raw/...` (MinIO) |
| Tabla Iceberg (Parquet) | `s3a://flight-data/iceberg/` (MinIO) |
| Checkpoint Kafka Spark | `s3a://models/checkpoints/` (MinIO) |
| Métricas y runs MLflow | PostgreSQL (`postgres-mlflow`) |
| Artefactos MLflow | `s3a://models/mlflow-artifacts/` (MinIO) |
| Predicciones | Cassandra (`flight_data.flight_predictions`) |
| Distancias aeropuertos | Cassandra (`flight_data.origin_dest_distances`) cargadas por **NiFi** |
| Estado Airflow | PostgreSQL (`postgres-airflow`) |
- **Spark Streaming**: `--deploy-mode cluster` — el driver corre en un worker del clúster
- **Airflow**: sólo los DAGs (`./resources/airflow`) se montan en el contenedor; ningún otro fichero local

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

### 3. Compilar el JAR de inferencia Spark

Requiere `sbt` instalado:

```bash
cd flight_prediction
sbt package
cd ..
```

El JAR queda en `flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar`.

### 4. Levantar todos los servicios

```bash
docker compose up -d --build
```

El flag `--build` compila las imágenes personalizadas (Flask, MLflow, Airflow). Solo es necesario la primera vez o cuando cambien sus Dockerfiles.

Espera ~4-5 minutos a que todos los contenedores estén sanos:

```bash
docker compose ps
```

Los siguientes contenedores aparecerán como `Exited (0)` — es normal, son de inicialización automática:

| Contenedor | Qué hace |
|---|---|
| `cassandra-init` | Crea el keyspace `flight_data` y las tablas en Cassandra |
| `nifi-init` | Configura el flujo NiFi via REST API (`GetFile → SplitText → EvaluateJsonPath → ReplaceText → PutCassandraQL`) |
| `cassandra-nifi-wait` | Espera a que NiFi termine de cargar las 4696 distancias en Cassandra |
| `minio-init` | Sube el JAR, el script de entrenamiento y los datos brutos a MinIO; configura buckets públicos |
| `airflow-init` | Crea el esquema de base de datos de Airflow y el usuario admin |

> **NiFi** (`apache/nifi:1.23.2`) carga automáticamente `data/origin_dest_distances.jsonl` en Cassandra mediante un pipeline de datos. La UI está disponible en `http://localhost:8085/nifi`.

Flask **no arrancará** hasta que `cassandra-nifi-wait` confirme que los datos están en Cassandra.

---

## Configuración inicial (solo la primera vez)

Una vez que todos los contenedores de inicialización hayan terminado (`Exited (0)`), solo queda:

### Paso 1 — Crear tabla Iceberg en MinIO

Convierte los datos brutos (subidos a MinIO por `minio-init`) al formato Parquet/Iceberg. Tarda varios minutos:

```bash
docker exec -u root spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  http://minio:9000/flight-data/scripts/create_iceberg_table.py 2>&1 | grep -E "Filas|total|OK|ERROR"
```

Debe terminar con:
```
Filas leidas: 457013
OK - Tabla Iceberg creada en s3a://flight-data/iceberg/flight_features
```

> **Nota:** el script se lee directamente desde MinIO (`http://minio:9000/flight-data/scripts/...`), sin acceso a disco local.

### Paso 2 — Entrenar el modelo con Airflow

1. Abre **Airflow** en `http://localhost:8081` (usuario: `admin`, contraseña: `admin`)
2. Activa el DAG `agile_data_science_batch_prediction_model_training`
3. Pulsa el botón **Trigger DAG** (▶)
4. Espera a que las 3 tareas estén en verde (~10-15 minutos):
   - `check_spark` — verifica que el clúster Spark funciona
   - `train_model` — entrena el Random Forest y guarda los modelos **exclusivamente en MinIO**
   - `register_mlflow` — verifica las métricas en MLflow

Una vez completado, los modelos quedan en `s3a://models/` y las métricas en MLflow (PostgreSQL + MinIO).

### Verificar el modo distribuido

Comprueba en la **Spark Master UI** (`http://localhost:8080`) que el job de inferencia aparece bajo **Drivers** (no bajo Applications). Esto confirma que el driver corre en el clúster con `--deploy-mode cluster`.

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
| NiFi             | http://localhost:8085   | —                       |
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
| NiFi           | 8085       |
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

## Despliegue en Kubernetes

### Docker Desktop

#### Requisitos previos

- Docker Desktop con Kubernetes activado: **Settings → Kubernetes → Enable Kubernetes**
- El JAR compilado en `flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar`
- Los datos descargados en `data/`

---

> ⚠️ **IMPORTANTE — evitar conflictos de puertos entre K8s y Docker Compose**
>
> K8s y Docker Compose **no pueden correr a la vez** — comparten puertos del host.
>
> - **Antes de arrancar K8s**: para Docker Compose primero:
>   ```bash
>   docker compose down
>   ```
> - **Antes de arrancar Docker Compose**: elimina el namespace de K8s y espera a que libere los puertos:
>   ```bash
>   kubectl delete namespace flight-prediction
>   # Espera hasta que el comando termine completamente
>   docker compose up -d
>   ```

---

#### 1. Levantar todos los servicios

```bash
bash k8s/deploy.sh
```

El script realiza automáticamente:
1. Construye las imágenes Docker personalizadas (Flask, MLflow, Airflow, **Flink**)
2. Aplica todos los manifiestos de Kubernetes
3. Espera a que Cassandra esté lista e inicializa el esquema
4. Sube el JAR, scripts y datos de entrenamiento a MinIO
5. Despliega NiFi y configura el flujo de carga de distancias via API REST
6. Espera a que NiFi cargue las 4696 distancias en Cassandra antes de arrancar Flask
7. Despliega el cluster Flink (jobmanager + taskmanager + job-submitter)

> NiFi puede tardar **15-20 minutos** en cargar las 4696 distancias. El script espera automáticamente.

#### 2. Crear tabla Iceberg

Una vez el script haya terminado, crea la tabla Iceberg ejecutando:

```bash
SPARK_POD=$(kubectl get pod -l app=spark-master -n flight-prediction -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n flight-prediction $SPARK_POD -- bash -c '
export SPARK_DRIVER_HOST=$(hostname -i)
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=512m \
  --conf spark.executor.memoryOverhead=128m \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262" \
  /tmp/create_iceberg_table.py
'
```

Tarda varios minutos. Debe terminar con:
```
OK - Tabla Iceberg creada en s3a://flight-data/iceberg/flight_features
     457013 filas almacenadas en formato Parquet/Iceberg
```

#### 3. Entrenar el modelo con Airflow

1. Abre **Airflow** en `http://localhost:30808` (admin / admin)
2. Activa el DAG `agile_data_science_batch_prediction_model_training`
3. Pulsa **Trigger DAG** (▶)
4. Espera a que las 3 tareas estén en verde (~10-60 minutos):
   - `check_spark` — verifica que el clúster Spark funciona
   - `train_model` — entrena el Random Forest (Spark MLlib) y el modelo sklearn, guarda ambos en MinIO
   - `register_mlflow` — verifica las métricas en MLflow

Una vez completado, el job de **Flink** detecta el modelo sklearn en MinIO (`models/sklearn_flight_model.joblib`) y empieza a procesar predicciones en tiempo real desde Kafka.

#### URLs de los servicios (K8s — NodePort)

| Servicio         | URL                          | Credenciales            |
|------------------|------------------------------|-------------------------|
| Flask            | http://localhost:30501       | —                       |
| NiFi             | http://localhost:30850/nifi  | —                       |
| Airflow          | http://localhost:30808       | admin / admin           |
| MLflow           | http://localhost:30500       | —                       |
| Spark UI         | http://localhost:30080       | —                       |
| MinIO Console    | http://localhost:30901       | minioadmin / minioadmin |
| Kibana           | http://localhost:30561       | —                       |
| **Flink UI**     | **http://localhost:30082**   | —                       |

#### Limpiar el despliegue

```bash
kubectl delete namespace flight-prediction
```

Esto elimina todos los recursos (pods, servicios, PVCs) y libera los puertos del host.

### Google Kubernetes Engine (GKE)

```bash
bash k8s/deploy-gke.sh <PROJECT_ID> [ZONE]
# Ejemplo:
bash k8s/deploy-gke.sh mi-proyecto-gcp europe-west1-b
```

El script:
1. Crea un clúster GKE (`e2-standard-4 × 3`, autoescalado 2-5 nodos)
2. Construye y sube las imágenes a Google Container Registry
3. Aplica todos los manifiestos Kubernetes
4. Expone los servicios con LoadBalancer
5. Sube el JAR, el script y los datos brutos a MinIO

Para obtener las IPs externas una vez desplegado:
```bash
kubectl get svc -n flight-prediction
```

Para eliminar el clúster y evitar costes:
```bash
gcloud container clusters delete flight-prediction-cluster --zone=europe-west1-b
```

---

## Despliegue en Google Cloud VM (Docker Compose)

### Paso 1 — Instalar Google Cloud SDK en tu máquina local

```bash
curl https://sdk.cloud.google.com | bash
exec -l $SHELL
gcloud init
```

### Paso 2 — Crear la VM

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

### Paso 3 — Abrir puertos en el firewall

```bash
gcloud compute firewall-rules create bigdata-ports \
  --allow tcp:5001,tcp:8081,tcp:5000,tcp:8080,tcp:9001,tcp:5601,tcp:9200 \
  --network=default \
  --source-ranges=0.0.0.0/0 \
  --target-tags=bigdata
```

### Paso 4 — Conectarse e instalar Docker

```bash
gcloud compute ssh big-data-vm --zone=europe-west1-b
```

Dentro de la VM:

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
```

### Paso 5 — Subir el proyecto y arrancar

Desde tu **máquina local**:

```bash
tar --exclude='practica_creativa/env' \
    --exclude='practica_creativa/.git' \
    --exclude='practica_creativa/logs' \
    --exclude='practica_creativa/mlflow' \
    --exclude='practica_creativa/models' \
    -czf practica.tar.gz practica_creativa/
gcloud compute scp practica.tar.gz big-data-vm:~ --zone=europe-west1-b
```

Dentro de la VM:

```bash
tar -xzf practica.tar.gz
cd practica_creativa
docker compose up -d --build
```

### Paso 6 — Obtener la IP externa

```bash
gcloud compute instances describe big-data-vm \
  --zone=europe-west1-b \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)'
```

| Servicio      | URL                         | Credenciales            |
|---------------|-----------------------------|-------------------------|
| Flask         | http://IP_EXTERNA:5001      | —                       |
| Airflow       | http://IP_EXTERNA:8081      | admin / admin           |
| MLflow        | http://IP_EXTERNA:5000      | —                       |
| Spark UI      | http://IP_EXTERNA:8080      | —                       |
| MinIO Console | http://IP_EXTERNA:9001      | minioadmin / minioadmin |
| Kibana        | http://IP_EXTERNA:5601      | —                       |

### Arrancar y parar la VM

```bash
gcloud compute instances start big-data-vm --zone=europe-west1-b
gcloud compute instances stop big-data-vm --zone=europe-west1-b
```

> Parar la VM detiene el cobro por cómputo. Los datos del disco persisten. Al volver a arrancar solo hace falta `docker compose up -d`.

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
