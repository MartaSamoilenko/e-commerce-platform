import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer
from cassandra.cluster import Cluster

cluster = Cluster(['cassandra'])
session = cluster.connect('ecommerce')

TOPIC = 'sales'

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = {uuid.uuid4() : {'product_name': 'product_1', 'product_price': round(random.uniform(5, 100), 2)}, 
            uuid.uuid4(): {'product_name': 'product_2', 'product_price': round(random.uniform(5, 100), 2)}, 
            uuid.uuid4() : {'product_name': 'product_3', 'product_price': round(random.uniform(5, 100), 2)}, 
            uuid.uuid4() : {'product_name': 'product_4', 'product_price': round(random.uniform(5, 100), 2)}}

users = {uuid.uuid4() : 'user_1', 
         uuid.uuid4() : 'user_2', 
         uuid.uuid4() : 'user_3', 
         uuid.uuid4() : 'user_4'}




def init_users():
    insert_stmt = session.prepare("""
    INSERT INTO users (user_id, user_name)
    VALUES (?, ?)
    """)

    for user_id, user_name in users.items():
        session.execute(insert_stmt, (user_id, user_name))

def get_random_item_from_inventory():
    query = session.prepare("""
        SELECT product_id, store_id_quantity, ts
        FROM inventory
        LIMIT 1
    """)
    
    return session.execute(query).one()


def gen_sale():

    item = get_random_item_from_inventory()

    if item :

        store_id = random.choice(list(item.store_id_quantity.keys()))
        quantity = random.randint(1, item.store_id_quantity[store_id])

        return {
            'sale_id': str(uuid.uuid4()),
            'product_id': str(item.product_id), #item.product_id,
            'quantity': quantity,
            'ts': datetime.utcnow().isoformat(),
            'user_id': str(random.choice(list(users.keys()))),
            'store_id': str(store_id)
        }
    
    return None
    

if __name__ == '__main__':
    
    init_users()

    while True:
        sale = gen_sale()
        if not sale:
            continue
        producer.send(TOPIC, sale)
        # print(f"→ produced {sale}")
        producer.flush()
        time.sleep(1) 