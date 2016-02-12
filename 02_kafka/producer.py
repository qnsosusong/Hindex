# Kafka producer that reads the real data to push them into Kafka topic "kafka_raw"
import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer, SimpleProducer
from datetime import datetime
import time

kafka = KafkaClient("52.89.50.90:9092")
source_file = '/home/ubuntu/01_raw/records-2016-01-04.json'

def genData(topic):
    producer = SimpleProducer(kafka, async=False)
    while True:
        with open(source_file) as f:
            for line in f:
                producer.send(topic, line) 
        
        source_file.close()

genData("kafka_raw")
