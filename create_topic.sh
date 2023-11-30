#!/bin/bash

# Đổi tên chủ đề nếu cần thiết
KAFKA_TOPIC=weather

# Chờ đến khi Kafka và Zookeeper khởi động
sleep 10

# Tạo chủ đề trong Kafka
/opt/kafka/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC

echo "Đã tạo chủ đề Kafka: $KAFKA_TOPIC"
