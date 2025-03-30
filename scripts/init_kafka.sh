#!/bin/bash

echo "Creating Kafka topics..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic ecommerce-transactions --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic ecommerce-inventory --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic ecommerce-user-activity --partitions 3 --replication-factor 1

echo "Listing Kafka topics..."
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

echo "Kafka topics initialized successfully!"