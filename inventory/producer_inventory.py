import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

TOPIC = 'items'

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = {uuid.uuid4() : {'product_name': 'product_1', 'product_price': round(random.uniform(5, 100), 2)}, 
            uuid.uuid4(): {'product_name': 'product_2', 'product_price': round(random.uniform(5, 100), 2)}, 
            uuid.uuid4() : {'product_name': 'product_3', 'product_price': round(random.uniform(5, 100), 2)}, 
            uuid.uuid4() : {'product_name': 'product_4', 'product_price': round(random.uniform(5, 100), 2)}}

stores = {uuid.uuid4() : 'store_1', 
          uuid.uuid4() : 'store_2', 
          uuid.uuid4() : 'store_3', 
          uuid.uuid4() : 'store_4'}

def init_products():
    insert_stmt = session.prepare("""
    INSERT INTO products (product_id, product_name, product_price)
    VALUES (?, ?, ?)
    """)

    for product_id, product in products.items():
        session.execute(insert_stmt, (product_id, product['product_name'], product['product_price']))

def init_stores():
    insert_stmt = session.prepare("""
    INSERT INTO stores (store_id, store_name)
    VALUES (?, ?)
    """)

    for store_id, store_name in stores.items():
        session.execute(insert_stmt, (store_id, store_name))

def get_item_from_inventory_by_id(product_id : uuid.UUID):

    query = session.prepare("""
        SELECT product_id, store_id_quantity, ts
        FROM inventory
        WHERE product_id = ?
        LIMIT 1
    """)
    
    return session.execute(query, (product_id,)).one()

def gen_inventory():
    product_id = random.choice(list(products.keys()))
    quantity = random.randint(1, 5)
    store_id = random.choice(list(stores.keys()))

    item = get_item_from_inventory_by_id(product_id)
    if item:

        if store_id in item.store_id_quantity:
            item.store_id_quantity[store_id] += quantity
        else:
            item.store_id_quantity[store_id] = quantity

        # comvert store_id in store_id_quantity to string
        store_id_quantity = {str(k): v for k, v in item.store_id_quantity.items()}

        return {
            'product_id': str(item.product_id),
            'ts': datetime.utcnow().isoformat(),
            'store_id_quantity': store_id_quantity
        }
    
    else:
        return {
            'product_id': str(product_id), #product_id,
            'ts': datetime.utcnow().isoformat(),
            'store_id_quantity': {str(product_id): quantity}
        }

if __name__ == '__main__':
    
    init_stores()
    init_products()
    
    while True:
        item = gen_inventory()
        if not item:
            continue
        
        producer.send(TOPIC, item)
        # print(f"→ produced {item}")
        producer.flush()
        time.sleep(0.05) 
