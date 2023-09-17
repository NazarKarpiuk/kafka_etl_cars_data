from time import time, sleep
from json import dumps
from kafka import KafkaProducer
import random

topic_name = 'car_speed'

def custom_partitioner(key, all_partitions, available):
    print(f"The key is: {key}")
    print(f"All partitions: {all_partitions}")
    print(f"After decoding of the key: {key.decode('UTF-8')}")
    return int(key.decode('UTF-8')) % len(all_partitions)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8'),
    partitioner=custom_partitioner
)

car_brands = ["Tesla", "Ford", "Honda", "Volvo"]
car_speeds = [10, 20, 40, 100, 90, 65]

for car_id in range(1000):
    data = {
        "car_id": car_id,
        "car_name": random.choice(car_brands),
        "car_speed": random.choice(car_speeds),
        "capture_time": int(time())
    }
    print(f"Inserting the data: {data}")
    producer.send(topic_name, key=str(car_id).encode(), value=data)
    sleep(1)