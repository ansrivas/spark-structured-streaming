# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Docstring for module."""


from datetime import datetime, timedelta

from pykafka import KafkaClient

bootstrap_servers = 'localhost:9092'
topic = 'test'

datetimeformat = '%Y-%m-%dT%H:%M:%S'



def confluent_kafka_producer():
    from confluent_kafka import Producer


    p = Producer({'bootstrap.servers': bootstrap_servers})

    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    start_date = datetime.strptime('2015-05-01T00:00:00', datetimeformat)
    count = 60

    for i in range(1, count + 1):
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)
        tstamp = start_date + timedelta(minutes=i + 10)
        data = "user_id:{0};{1};event_number_{2}".format(i, tstamp, i)
        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        p.produce(topic, data.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()

if __name__ == '__main__':
    confluent_kafka_producer()
