import json
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
import uuid

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

TOPIC = 'items'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

BATCH_SIZE = 10
batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
counter = 0

insert_stmt = session.prepare("""
  INSERT INTO inventory (product_id, store_id_quantity, ts)
  VALUES (?, ?, ?)
""")

for msg in consumer:
    item = msg.value
    item_ts = datetime.fromisoformat(item['ts'])

    # convert store_id_quantity from string to UUID
    item['store_id_quantity'] = {uuid.UUID(k): v for k, v in item['store_id_quantity'].items()}
    
    batch.add(
        insert_stmt,
        (
            uuid.UUID(item['product_id']),
            item['store_id_quantity'],
            item_ts,
        )
    )
    counter += 1

    if counter >= BATCH_SIZE:
        session.execute(batch)
        print(f"← wrote batch of {counter} items to Cassandra")
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        counter = 0