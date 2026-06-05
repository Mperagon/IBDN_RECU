import sys, os
sys.path.insert(0, '/home/miguel/practica_creativa/resources/web')
os.environ['PROJECT_HOME'] = '/home/miguel/practica_creativa'

print("1. Verificando flask-socketio...")
from flask_socketio import SocketIO
print("   OK")

print("2. Verificando KafkaConsumer...")
from kafka import KafkaConsumer
print("   OK")

print("3. Verificando cassandra-driver...")
import predict_utils
cluster, session = predict_utils.get_cassandra_session()
print("   OK")

print("4. Verificando tabla flight_predictions...")
row = session.execute("SELECT COUNT(*) FROM flight_data.flight_predictions").one()
print(f"   filas actuales: {row[0]}")

print("5. Verificando prepared statement de insercion...")
stmt = session.prepare(
    "INSERT INTO flight_data.flight_predictions "
    "(uuid, prediction, timestamp, origin, dest, carrier, dep_delay) "
    "VALUES (?, ?, ?, ?, ?, ?, ?)"
)
print("   OK")

print("6. Verificando topic Kafka 'flight-predictions'...")
import subprocess
result = subprocess.run(
    ['/home/miguel/kafka/bin/kafka-topics.sh', '--list', '--bootstrap-server', 'localhost:9092'],
    capture_output=True, text=True, timeout=10
)
topics = result.stdout.strip().split('\n')
if 'flight-predictions' in topics:
    print(f"   OK - topic existe. Topics: {topics}")
else:
    print(f"   WARN - topic no encontrado. Topics: {topics}")

cluster.shutdown()
print("\nTodo OK - Punto 3 listo para arrancar")
