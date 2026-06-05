from minio import Minio
c = Minio('127.0.0.1:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
objs = list(c.list_objects('flight-data', recursive=True))
print(f'Objetos en MinIO: {len(objs)}')
for o in objs:
    print(f'  {o.object_name}  ({o.size/1024/1024:.2f} MB)')
