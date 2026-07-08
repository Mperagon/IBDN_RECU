# Práctica Creativa — Sistema de Predicción de Retrasos de Vuelos

Arquitectura de Big Data completamente distribuida con Spark, Kafka, Cassandra, MinIO, NiFi, Airflow y MLflow, todo orquestado con Docker Compose.

## Arquitectura

```
Flask (web) → Kafka → Flink (predicción RT, parallelism 2) → flight-predictions
                 ↓                                                     ↑
                 └──→ Spark Streaming (predicción RT, cluster K8s) → spark-flight-predictions
                 ↓
             Logstash → Elasticsearch → Kibana (visualización)
                                   ↑
                         Modelo sklearn (MLflow Registry / MinIO)
                                   ↑
                    Airflow DAG → Spark MLlib ← Iceberg (MinIO)
                                      ↓
                                   MLflow (PostgreSQL + MinIO)

NiFi (carga distancias) ← origin_dest_distances.jsonl (MinIO)
         ↓
    Cassandra (flight_data.origin_dest_distances)
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
git clone https://github.com/Mperagon/IBDN_RECU.git practica_creativa
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

El flag `--build` compila las imágenes personalizadas (Flask, MLflow, Airflow). Solo es necesario la primera vez o cuando cambien sus Dockerfiles.

Espera ~4-5 minutos a que todos los contenedores estén sanos:

```bash
docker compose ps
```

Los siguientes contenedores aparecerán como `Exited (0)` — es normal, son de inicialización automática:

| Contenedor | Qué hace |
|---|---|
| `cassandra-init` | Crea el keyspace `flight_data` y las tablas en Cassandra |
| `nifi-init` | Configura el flujo NiFi via REST API (`GenerateFlowFile → InvokeHTTP (MinIO) → SplitText → EvaluateJsonPath → ReplaceText → MergeContent (batch 100) → PutCassandraQL`) |
| `cassandra-nifi-wait` | Espera a que NiFi termine de cargar las 4696 distancias en Cassandra |
| `minio-init` | Sube el script de entrenamiento y los datos brutos a MinIO; configura buckets públicos |
| `airflow-init` | Crea el esquema de base de datos de Airflow y el usuario admin |

> **NiFi** (`apache/nifi:1.23.2`) carga automáticamente `data/origin_dest_distances.jsonl` en Cassandra mediante un pipeline de datos. La UI está disponible en `http://localhost:8085/nifi`.

Flask **no arrancará** hasta que `cassandra-nifi-wait` confirme que los datos están en Cassandra.

---

## Configuración inicial (solo la primera vez)

Una vez que todos los contenedores de inicialización hayan terminado (`Exited (0)`), solo queda:

### Paso 1 — Crear tabla Iceberg en MinIO *(solo la primera vez)*

Convierte los datos brutos al formato Parquet/Iceberg. Los datos persisten en MinIO aunque reinicies los contenedores, por lo que este paso **no es necesario repetirlo**. Tarda varios minutos:

```bash
docker cp resources/create_iceberg_table.py spark-master:/tmp/create_iceberg_table.py

docker exec spark-master spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /tmp/create_iceberg_table.py
```

Debe terminar con:
```
Filas leidas: 457013
OK - Tabla Iceberg creada en s3a://flight-data/iceberg/flight_features
```



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

Comprueba en el **Spark History Server** (`http://localhost:18080`) que los jobs de entrenamiento aparecen en estado `FINISHED`. Esto confirma que el driver corrió en modo cluster dentro del clúster Kubernetes (`--deploy-mode cluster --master k8s://...`).

Para ver los pods del driver y los executors distribuidos en los nodos:

```bash
kubectl get pods -n flight-prediction -o wide | grep -E "driver|exec"
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
| YARN ResourceManager | http://localhost:8088 | —                     |
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
| YARN ResourceManager | 8088  |
| MinIO API      | 9000       |
| MinIO Web      | 9001       |
| Kafka          | 9092       |
| Cassandra      | 9042       |
| Elasticsearch  | 9200       |
| Kibana         | 5601       |

---

## Despliegue en Kubernetes

### Docker Desktop

#### Requisitos previos

- Docker Desktop con Kubernetes activado (cluster kind multi-nodo recomendado)
- Los datos descargados en `data/` (ejecutar `bash resources/download_data.sh`)

---

#### 1. Levantar todos los servicios

```bash
bash k8s/deploy.sh
```

El script realiza automáticamente (~20-30 min):
1. Construye las imágenes Docker personalizadas (Flask, MLflow, Airflow, Spark, Flink)
2. Aplica todos los manifiestos de Kubernetes en el namespace `flight-prediction`
3. Espera a que Cassandra esté lista e inicializa el esquema
4. Sube scripts y datos de entrenamiento a MinIO
5. Despliega NiFi y espera a que cargue las 4696 distancias origen-destino en Cassandra
6. Crea la tabla Iceberg en MinIO automáticamente (job `iceberg-init`)
7. Despliega el cluster Flink (jobmanager + taskmanager + job-submitter)
8. Despliega Spark History Server y Spark Streaming Job (cluster mode K8s)
9. Lanza el DAG de entrenamiento en Airflow automáticamente

> NiFi puede tardar **15-20 minutos** en cargar las 4696 distancias. El script espera automáticamente.

#### 2. Entrenar el modelo con Airflow

El deploy.sh lanza el DAG automáticamente. Puedes seguirlo en Airflow:

1. Abre **Airflow** (ver URLs abajo, admin / admin)
2. Activa el DAG `agile_data_science_batch_prediction_model_training` si no está activo
3. Espera a que las 3 tareas estén en verde (~10-60 minutos):
   - `check_spark` — verifica que Spark on K8s funciona
   - `train_model` — entrena Random Forest (Spark MLlib) + sklearn, guarda en MinIO y registra en MLflow
   - `register_mlflow` — promueve el modelo a stage `Production` en MLflow Registry

Una vez completado:
- **Flink** (parallelism 2, 2 TaskManagers, checkpointing cada 10s) detecta el modelo en MLflow Registry (stage `Production`) y empieza a publicar predicciones en el topic `flight-predictions`.
- **Spark Streaming** (cluster mode K8s, driver + 2 executors) detecta el modelo en MinIO/Iceberg y publica en `spark-flight-predictions`.

#### 3. Acceder a los servicios (Windows + Docker Desktop)

En Docker Desktop con WSL2 los NodePorts no se exponen automáticamente. Usa el script de port-forwards incluido.

Abre **PowerShell de Windows** y ejecuta:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
cd \\wsl.localhost\Ubuntu\home\migue\practica_creativa
.\start-portforwards.ps1
```

| Servicio             | URL                                                | Credenciales            |
|----------------------|----------------------------------------------------|-------------------------|
| Flask (predicciones) | http://localhost:5001/flights/delays/predict_kafka | —                       |
| Airflow              | http://localhost:8080                              | admin / admin           |
| MLflow               | http://localhost:5000                              | —                       |
| Kibana               | http://localhost:5601                              | —                       |
| Flink UI             | http://localhost:8081                              | —                       |
| Spark History Server | http://localhost:18080                             | —                       |
| NiFi                 | http://localhost:8850/nifi                         | —                       |
| MinIO Console        | http://localhost:9001                              | minioadmin / minioadmin |

Para parar los port-forwards:
```powershell
Stop-Job * ; Remove-Job *
```

#### 4. Limpiar el despliegue

```bash
kubectl delete namespace flight-prediction
```

Esto elimina todos los recursos (pods, servicios, PVCs).

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
