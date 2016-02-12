#! /bin/bash

/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper localhost:2181
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic json_raw_wk4_real
/usr/local/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 4 --topic json_raw_wk4_simulated

python kafka_producer_raw_json_real.py
python kafka_producer_raw_json_simulated.py

