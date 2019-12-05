from confluent_kafka import Consumer, KafkaError

bootstrap_servers = 'localhost:9092'
topic = 'test'

datetimeformat = '%Y-%m-%dT%H:%M:%S'


c = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe([topic])

while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print("Consumer error: {}".format(msg.error()))
        continue

    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()
