#!/bin/bash

# Colors for better output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}Checking infrastructure health...${NC}"

# Check if Zookeeper is running
echo -n "Zookeeper: "
if docker-compose exec zookeeper bash -c "echo ruok | nc localhost 2181 | grep imok" &> /dev/null; then
    echo -e "${GREEN}Running${NC}"
else
    echo -e "${RED}Not Running${NC}"
fi

# Check if Kafka is running
echo -n "Kafka: "
if docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; then
    echo -e "${GREEN}Running${NC}"
else
    echo -e "${RED}Not Running${NC}"
fi

# Check if Cassandra is running
echo -n "Cassandra: "
if docker-compose exec cassandra cqlsh -e "describe keyspaces" &> /dev/null; then
    echo -e "${GREEN}Running${NC}"
else
    echo -e "${RED}Not Running${NC}"
fi

# Check if Hazelcast is running
echo -n "Hazelcast: "
if docker-compose logs hazelcast | grep "Members {size:1, ver:1} \[" &> /dev/null; then
    echo -e "${GREEN}Running${NC}"
else
    echo -e "${RED}Not Running${NC}"
fi

# Check Kafka topics
echo -e "\n${YELLOW}Checking Kafka topics:${NC}"
topics=$(docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list)
if echo "$topics" | grep -q "ecommerce-transactions"; then
    echo -e "ecommerce-transactions: ${GREEN}Exists${NC}"
else
    echo -e "ecommerce-transactions: ${RED}Does not exist${NC}"
fi

if echo "$topics" | grep -q "ecommerce-inventory"; then
    echo -e "ecommerce-inventory: ${GREEN}Exists${NC}"
else
    echo -e "ecommerce-inventory: ${RED}Does not exist${NC}"
fi

if echo "$topics" | grep -q "ecommerce-user-activity"; then
    echo -e "ecommerce-user-activity: ${GREEN}Exists${NC}"
else
    echo -e "ecommerce-user-activity: ${RED}Does not exist${NC}"
fi

# Check microservices
echo -e "\n${YELLOW}Checking microservices:${NC}"
for service in user-service sales-service inventory-service
do
    echo -n "$service: "
    if docker-compose ps | grep $service | grep -q "Up"; then
        echo -e "${GREEN}Running${NC}"
    else
        echo -e "${RED}Not Running${NC}"
    fi
done

# Check data generator
echo -n "data-generator: "
if docker-compose ps | grep data-generator | grep -q "Up"; then
    echo -e "${GREEN}Running${NC}"
else
    echo -e "${RED}Not Running${NC}"
fi

echo -e "\n${YELLOW}Useful URLs:${NC}"
echo -e "Kafka UI: ${GREEN}http://localhost:8080${NC}"
echo -e "KSQLDB Server: ${GREEN}http://localhost:8088${NC}"

echo -e "\n${YELLOW}Done!${NC}"