# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Docstring for module."""


from datetime import datetime, timedelta

from pykafka import KafkaClient

bootstrap_servers = 'localhost:9092'
topic = 'test.1'

datetimeformat = '%Y-%m-%dT%H:%M:%S'


def pykafka_producer(use_rdkafka=True):
    """Example pykafka-producer to be used."""
    client = KafkaClient(hosts=bootstrap_servers)
    topic_to_write = client.topics[topic]
    start_date = datetime.strptime('2015-05-01T00:00:00', datetimeformat)
    count = 60
    with topic_to_write.get_producer(use_rdkafka=use_rdkafka) as producer:
        for i in xrange(1, count + 1):
            tstamp = start_date + timedelta(minutes=i + 10)
            msg_payload = "user_id:{0};{1};event_number_{2}".format(i, tstamp, i)
            print msg_payload
            producer.produce(msg_payload)
    print "Successfully dumped data to kafka-topic: {0}".format(topic)


if __name__ == '__main__':
    pykafka_producer()
