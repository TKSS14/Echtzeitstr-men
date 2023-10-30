from kafka import KafkaProducer
import time

# create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
while(True):
    # send a message to a topic
    producer.send('my_topic', b'Hello, Kafka!')
    time.sleep(0.5)

