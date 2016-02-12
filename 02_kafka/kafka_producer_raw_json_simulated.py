# Kafka producer that reads the simulated data in a loop to push them into Kafka, topic = json_raw_wk4_simulated
import os
import sys
from kafka import KafkaClient, KeyedProducer, SimpleConsumer, SimpleProducer
from datetime import datetime
import time
import json
import logging

logging.basicConfig()

kafka = KafkaClient("localhost:9092")

def genData(topic):
	producer = SimpleProducer(kafka, async=True)
	with open(source_file) as f:
		for line in f:
			print line
			jd = json.dumps(line)
 			producer.send_messages(topic, line.encode('utf-8')) 

topic = "json_raw_wk4_simulated"

for i in range(1, 19):
	if i == 1:
		source_file = '/home/ubuntu/01_raw/90_simulated/fake_data.json'
	else: 
		source_file = '/home/ubuntu/01_raw/90_simulated/fake_data_p' + str(i) + '.json'

	genData(topic)
	finish_message = "finish injexting fake_data_p" + str(i)
	print fininsh_message


