from kafka import KafkaConsumer

consumer = KafkaConsumer('basic',bootstrap_servers=['localhost:9092'])

for messsage in consumer:
    print(messsage.value)
