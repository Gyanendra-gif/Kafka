from kafka import KafkaProducer
from time import sleep

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
for i in range(10):
    producer.send('topic1',b'Hello World !!!')
    sleep(2)
