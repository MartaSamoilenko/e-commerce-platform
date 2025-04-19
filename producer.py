import json, time, uuid, random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['product_1', 'product_2', 'product_3', 'product_4']

def gen_sale():
    return {
        'sale_id': str(uuid.uuid4()),
        'product_id': random.choice(products),
        'price': round(random.uniform(5, 100), 2),
        'quantity': random.randint(1, 5),
        'ts': datetime.utcnow().isoformat()
    }

if __name__ == '__main__':
    while True:
        sale = gen_sale()
        producer.send('sales', sale)
        print(f"→ produced {sale}")
        time.sleep(1) 