import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

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

users = {uuid.uuid4() : 'user_1', 
         uuid.uuid4() : 'user_2', 
         uuid.uuid4() : 'user_3', 
         uuid.uuid4() : 'user_4'}

def init_stores():
    insert_stmt = session.prepare("""
    INSERT INTO stores (store_id, store_name)
    VALUES (?, ?)
    """)

    for store_id, store_name in stores.items():
        session.execute(insert_stmt, (store_id, store_name))

def init_products():
    insert_stmt = session.prepare("""
    INSERT INTO products (product_id, product_name, product_price)
    VALUES (?, ?, ?)
    """)

    for product_id, product in products.items():
        session.execute(insert_stmt, (product_id, product['product_name'], product['product_price']))

def init_users():
    insert_stmt = session.prepare("""
    INSERT INTO users (user_id, user_name)
    VALUES (?, ?)
    """)

    for user_id, user_name in users.items():
        session.execute(insert_stmt, (user_id, user_name))

def gen_sale():
    return {
        'sale_id': str(uuid.uuid4()),
        'product_id': str(random.choice(list(products.keys()))),
        'quantity': random.randint(1, 5),
        'ts': datetime.utcnow().isoformat(),
        'user_id': str(random.choice(list(users.keys()))),
        'store_id': str(random.choice(list(stores.keys())))
    }

if __name__ == '__main__':
    
    init_stores()
    init_products()
    init_users()
    
    while True:
        sale = gen_sale()
        producer.send('sales', sale)
        print(f"→ produced {sale}")
        time.sleep(1) 