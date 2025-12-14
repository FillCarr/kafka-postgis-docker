import json
import csv
from kafka import KafkaProducer
import json
# https://kafka-python.readthedocs.io/en/master/
# https://docs.docker.com/guides/kafka/
# serialized json needed for kafka consumer
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for i in range(1,10):
    reader = csv.DictReader(open(f'Project1/PVS {i}/dataset_gps.csv'))
    for row in reader:
        producer.send(f'SensorData', row)
producer.flush()
print(f'sent and flushed')
