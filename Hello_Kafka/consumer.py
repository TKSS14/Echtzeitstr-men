from kafka import KafkaConsumer

# create a Kafka consumer
consumer = KafkaConsumer('my_topic', bootstrap_servers ='localhost:9092')

# consume messages from the topic
for message in consumer:
    print(message.value.decode('utf -8'))