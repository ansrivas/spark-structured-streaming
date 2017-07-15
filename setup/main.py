# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Docstring for module."""

from pykafka import KafkaClient
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import random
import time
import io
import arrow
from faker import Faker
import argparse

bootstrap_servers = 'localhost:9092'
topic = str.encode('test.1')

datetimeformat = '%Y-%m-%dT%H:%M:%S'

fake = Faker()


def generate_data():
    """Generate some fake data to generate messages in this format.

    message testlog {
     optional int64 user_id = 1; # This is a random number between 1-100
     optional string timestamp = 2;   # This generates time stamp between time.now()-1day to now.
     optional string event_id = 3;
    }
    """
    event_id = {1: 'search', 2: 'filter', 3: 'click'}
    user_id = "user_{0}".format(random.randint(1, 100))

    time = arrow.utcnow().to("local").format('YYYY-MM-DD HH:mm:ssZZ')
    event = event_id[random.randint(1, len(event_id))]
    log = {"user_id": user_id, "timestamp": time, "event_id": event}
    print(log)
    return log


def parse_schema(path='./message.avsc'):
    """Parse schema will parse the avro schema ."""
    return avro.schema.Parse(open(path).read())


def get_data_for_kafka(schema):
    """Encode data in binary for a given avro schema."""
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(generate_data(), encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes


def pykafka_producer(use_rdkafka=True, count_msg=10):
    """Publishe data to kafka."""
    client = KafkaClient(hosts=bootstrap_servers)
    topic_to_write = client.topics[topic]
    schema = parse_schema(path='./message.avsc')
    with topic_to_write.get_producer(use_rdkafka=use_rdkafka) as producer:
        for i in range(1, count_msg + 1):
            msg_payload = get_data_for_kafka(schema)
            producer.produce(msg_payload)
    print("===========================================================")
    print("Successfully dumped data to kafka-topic: {0}".format(topic))
    print("===========================================================")


if __name__ == '__main__':

    help_seed = "Generate data with a given seed for part4 of the task1. Needed to repeat the experiment."
    help_samples = "Number of samples to generate."
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", help=help_seed, type=int)
    parser.add_argument("--samples", help=help_samples, type=int)
    args = parser.parse_args()
    if args.seed:
        fake.random.seed(args.seed)

    samples = 10
    if args.samples:
        samples = args.samples

    t = time.time()
    pykafka_producer(count_msg=samples)
    print("===========================================================")
    print("Total time take to publish this data to Kafka: {0}s".format(time.time() - t))
    print("===========================================================")
