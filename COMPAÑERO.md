# Guía de instalación y puesta en marcha — Practica Creativa

Esta guía explica todo lo que hay que hacer desde cero para tener el sistema funcionando.
Sigue los pasos en orden.

---

## 1. Requisitos previos

Asegúrate de tener instalado:

- WSL2 con Ubuntu 24.04
- SDKMAN (`curl -s "https://get.sdkman.io" | bash`)
- Java 11 y Java 17 via SDKMAN:
  ```bash
  sdk install java 11.0.25-amzn
  sdk install java 17.0.14-amzn
  ```
- Spark 3.5.3 via SDKMAN:
  ```bash
  sdk install spark 3.5.3
  ```
- Scala 2.12.10 + SBT via SDKMAN:
  ```bash
  sdk install scala 2.12.10
  sdk install sbt
  ```
- Kafka 3.9.0 con KRaft (descargado en `~/kafka`)
- Cassandra 4.1.7 (instalado en el sistema con Java 11)
- Python 3.12

---

## 2. Clonar el repositorio y configurar Python

```bash
git clone <URL_DEL_REPO> ~/practica_creativa
cd ~/practica_creativa

python3 -m venv env
source env/bin/activate

pip install -r requirements.txt
pip install cassandra-driver flask-socketio minio
```

> `cassandra-driver`, `flask-socketio` y `minio` **no están** en requirements.txt, hay que instalarlos manualmente.

---

## 3. Descargar los datos de entrenamiento

```bash
cd ~/practica_creativa
source env/bin/activate
resources/download_data.sh
```

Esto descarga `data/simple_flight_delay_features.jsonl.bz2` y `data/origin_dest_distances.jsonl`.

---

## 4. Instalar y arrancar MinIO

Descargar el binario:
```bash
wget https://dl.min.io/server/minio/release/linux-amd64/minio -O ~/minio
chmod +x ~/minio
```

Crear directorio de datos y arrancar:
```bash
mkdir -p ~/minio_data
mkdir -p ~/practica_creativa/logs
nohup ~/minio server ~/minio_data --console-address ":9001" \
  > ~/practica_creativa/logs/minio.log 2>&1 &
```

Verificar que arrancó (espera 3 segundos):
```bash
curl -s http://127.0.0.1:9000/minio/health/live && echo "MinIO OK"
```

---

## 5. Crear buckets en MinIO

```bash
cd ~/practica_creativa
source env/bin/activate
python setup_minio_buckets.py
```

Debe mostrar:
```
Bucket 'flight-data' creado
Bucket 'models' creado
```

---

## 6. Subir datos a MinIO y crear tabla Iceberg

Primero sube los datos al bucket `flight-data`:
```bash
source env/bin/activate
python setup_minio_iceberg.py
```

Después crea la tabla Iceberg (lee el JSONL y lo convierte a Parquet/Iceberg en MinIO):
```bash
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 17.0.14-amzn

spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  resources/create_iceberg_table.py
```

> Tarda varios minutos. Al terminar debe decir `OK - Tabla Iceberg creada` con 457013 filas.

Verificar:
```bash
spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  verify_iceberg.py
```

---

## 7. Configurar Cassandra

Cassandra necesita Java 11. Arrancarlo:
```bash
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 11.0.25-amzn
cassandra -f
```

Espera hasta ver `Starting listening for CQL clients` y luego abre otra terminal.

### Crear keyspace y tablas

Crea el fichero `/tmp/setup_cassandra.cql` con este contenido:

```cql
CREATE KEYSPACE IF NOT EXISTS flight_data
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE flight_data;

CREATE TABLE IF NOT EXISTS origin_dest_distances (
  origin  TEXT,
  dest    TEXT,
  distance DOUBLE,
  PRIMARY KEY ((origin, dest))
);

CREATE TABLE IF NOT EXISTS flight_predictions (
  uuid       TEXT PRIMARY KEY,
  prediction DOUBLE,
  timestamp  TIMESTAMP,
  origin     TEXT,
  dest       TEXT,
  carrier    TEXT,
  dep_delay  DOUBLE
);
```

Ejecutarlo con Python (cqlsh no funciona bien en Python 3.12):

```bash
source ~/practica_creativa/env/bin/activate
python3 - <<'EOF'
from cassandra.cluster import Cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
cql = open('/tmp/setup_cassandra.cql').read()
for stmt in [s.strip() for s in cql.split(';') if s.strip()]:
    session.execute(stmt)
    print("OK:", stmt[:60])
cluster.shutdown()
EOF
```

### Importar distancias entre aeropuertos

```bash
source ~/practica_creativa/env/bin/activate
python3 - <<'EOF'
import json
from cassandra.cluster import Cluster

cluster = Cluster(['127.0.0.1'])
session = cluster.connect('flight_data')
stmt = session.prepare(
    "INSERT INTO origin_dest_distances (origin, dest, distance) VALUES (?, ?, ?)"
)
with open('/home/miguel/practica_creativa/data/origin_dest_distances.jsonl') as f:
    for line in f:
        r = json.loads(line)
        session.execute(stmt, (r['Origin'], r['Dest'], float(r['Distance'])))
print("Distancias importadas")
cluster.shutdown()
EOF
```

> Importa 4.696 registros.

---

## 8. Entrenar el modelo

Con MinIO e Iceberg ya configurados, entrena el modelo desde la tabla Iceberg:

```bash
cd ~/practica_creativa
source env/bin/activate
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 17.0.14-amzn

python3 resources/train_spark_mllib_model.py .
```

> Tarda bastante (puede ser 10-20 minutos). Al terminar muestra `Accuracy = ~0.58` y guarda los modelos en `./models/` y en MinIO `s3a://models/`.

---

## 9. Compilar el JAR de Spark (MakePrediction)

```bash
cd ~/practica_creativa/flight_prediction
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 17.0.14-amzn
sbt package
```

El JAR queda en:
`flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar`

---

## 10. Arrancar el sistema completo

Necesitas **5 terminales** abiertas al mismo tiempo.

### Terminal 1 — Cassandra
```bash
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 11.0.25-amzn
cassandra -f
```
Espera: `Starting listening for CQL clients`

### Terminal 2 — Kafka
```bash
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 17.0.14-amzn
cd ~/kafka

# Solo la primera vez:
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties

# Siempre:
bin/kafka-server-start.sh config/kraft/server.properties
```
Espera: `Kafka Server started`

Crear los topics (solo la primera vez, en otra terminal):
```bash
cd ~/kafka
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic flight-delay-ml-request
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic flight-predictions
```

### Terminal 3 — MinIO
```bash
nohup ~/minio server ~/minio_data --console-address ":9001" \
  > ~/practica_creativa/logs/minio.log 2>&1 &
```

### Terminal 4 — Spark (predictor)
```bash
source ~/.sdkman/bin/sdkman-init.sh
sdk use java 17.0.14-amzn

spark-submit \
  --class es.upm.dit.ging.predictor.MakePrediction \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  ~/practica_creativa/flight_prediction/target/scala-2.12/flight_prediction_2.12-0.1.jar
```
Espera: `Streaming query started`

### Terminal 5 — Flask (web)
```bash
source ~/practica_creativa/env/bin/activate
export PROJECT_HOME=~/practica_creativa
cd ~/practica_creativa/resources/web
python predict_flask.py
```

---

## 11. Probar

Abre el navegador en:
```
http://localhost:5001/flights/delays/predict_kafka
```

Rellena el formulario (ejemplo: Origin `ATL`, Dest `SFO`, Carrier `AA`, DepDelay `10`, Date `2016-12-25`) y envía. La predicción debe aparecer en segundos via WebSocket.

---

## Resumen de puertos

| Servicio   | Puerto |
|------------|--------|
| Cassandra  | 9042   |
| Kafka      | 9092   |
| MinIO API  | 9000   |
| MinIO Web  | 9001   |
| Flask      | 5001   |
