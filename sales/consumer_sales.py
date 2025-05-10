import json
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
import uuid

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

TOPIC = 'sales'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

BATCH_SIZE = 100
batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
counter = 0

insert_stmt = session.prepare("""
  INSERT INTO sales (sale_id, product_id, quantity, ts, user_id, store_id)
  VALUES (?, ?, ?, ?, ?, ?)
""")

for msg in consumer:
    sale = msg.value
    sale_ts = datetime.fromisoformat(sale['ts'])
    
    batch.add(
        insert_stmt,
        (
            uuid.UUID(sale['sale_id']),
            uuid.UUID(sale['product_id']),
            sale['quantity'],
            sale_ts,
            uuid.UUID(sale['user_id']),
            uuid.UUID(sale['store_id'])
        )
    )
    counter += 1

    if counter >= BATCH_SIZE:
        session.execute(batch)
        print(f"← wrote batch of {counter} sales to Cassandra")
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        counter = 0