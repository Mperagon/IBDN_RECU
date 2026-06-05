import sys
sys.path.insert(0, '/home/miguel/practica_creativa/resources/web')
import predict_utils

cluster, session = predict_utils.get_cassandra_session()

dist = predict_utils.get_flight_distance(session, 'ATL', 'SFO')
print(f'ATL -> SFO: {dist} millas')

dist2 = predict_utils.get_flight_distance(session, 'JFK', 'LAX')
print(f'JFK -> LAX: {dist2} millas')

cluster.shutdown()
print('OK - Cassandra funciona correctamente')
