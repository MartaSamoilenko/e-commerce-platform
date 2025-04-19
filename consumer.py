import json
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import uuid

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

consumer = KafkaConsumer(
    'sales',
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

insert_stmt = session.prepare("""
  INSERT INTO sales (sale_id, product_id, price, quantity, ts)
  VALUES (?, ?, ?, ?, ?)
""")

for msg in consumer:
    sale = msg.value
    sale_ts = datetime.fromisoformat(sale['ts'])
    session.execute(insert_stmt, (
        uuid.UUID(sale['sale_id']),
        sale['product_id'],
        sale['price'],
        sale['quantity'],
        sale_ts
    ))
    print(f"← wrote to Cassandra: {sale}")
