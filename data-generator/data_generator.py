import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from confluent_kafka import Producer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DATA_INTERVAL_MS = int(os.environ.get('DATA_INTERVAL_MS', 1000))  # Default to 1 second

# Topics
TRANSACTION_TOPIC = 'ecommerce.transactions'
USER_ACTIVITY_TOPIC = 'ecommerce.user.activity'
INVENTORY_UPDATE_TOPIC = 'ecommerce.inventory.updates'

# Mock data lists
PRODUCT_CATEGORIES = ['Electronics', 'Clothing', 'Home & Kitchen', 'Books', 'Toys', 'Sports', 'Beauty', 'Grocery']
PRODUCT_IDS = [f'PROD-{i:04d}' for i in range(1, 101)]  # 100 products
USER_IDS = [f'USER-{i:05d}' for i in range(1, 1001)]  # 1000 users
PAYMENT_METHODS = ['Credit Card', 'PayPal', 'Apple Pay', 'Google Pay', 'Bank Transfer']
USER_ACTIONS = ['view_product', 'add_to_cart', 'remove_from_cart', 'checkout', 'search', 'view_category', 'login', 'logout']

def delivery_callback(err, msg):
    """Callback for Kafka producer to log success/failure of message delivery"""
    if err:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def create_kafka_producer():
    """Create and return a Kafka producer instance"""
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'client.id': 'ecommerce-data-generator'
    }
    return Producer(conf)

def generate_transaction():
    """Generate a random e-commerce transaction"""
    # Select random products (1-5 items)
    num_items = random.randint(1, 5)
    selected_products = random.sample(PRODUCT_IDS, num_items)
    
    # Generate order items with quantities and prices
    items = []
    total_amount = 0
    
    for product_id in selected_products:
        quantity = random.randint(1, 3)
        price = round(random.uniform(10.0, 500.0), 2)
        item_total = quantity * price
        total_amount += item_total
        
        category = random.choice(PRODUCT_CATEGORIES)
        
        items.append({
            'product_id': product_id,
            'category': category,
            'quantity': quantity,
            'unit_price': price,
            'item_total': item_total
        })
    
    # Apply random discount (0-20%)
    discount_pct = random.randint(0, 20)
    discount_amount = round((discount_pct / 100) * total_amount, 2)
    final_amount = round(total_amount - discount_amount, 2)
    
    # Generate transaction
    transaction = {
        'transaction_id': str(uuid.uuid4()),
        'user_id': random.choice(USER_IDS),
        'timestamp': datetime.now().isoformat(),
        'payment_method': random.choice(PAYMENT_METHODS),
        'items': items,
        'subtotal': total_amount,
        'discount_pct': discount_pct,
        'discount_amount': discount_amount,
        'total': final_amount,
        'status': 'completed'
    }
    
    return transaction

def generate_user_activity():
    """Generate a random user activity event"""
    action = random.choice(USER_ACTIONS)
    
    # Base activity data
    activity = {
        'event_id': str(uuid.uuid4()),
        'user_id': random.choice(USER_IDS),
        'timestamp': datetime.now().isoformat(),
        'session_id': str(uuid.uuid4()),
        'action': action,
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'ip_address': f'192.168.{random.randint(0, 255)}.{random.randint(0, 255)}'
    }
    
    # Add action-specific data
    if action == 'view_product':
        activity['product_id'] = random.choice(PRODUCT_IDS)
        activity['duration_seconds'] = random.randint(5, 300)
    elif action == 'add_to_cart' or action == 'remove_from_cart':
        activity['product_id'] = random.choice(PRODUCT_IDS)
        activity['quantity'] = random.randint(1, 5)
    elif action == 'search':
        activity['search_query'] = random.choice(['shoes', 'laptop', 'phone', 'dress', 'headphones'])
    elif action == 'view_category':
        activity['category'] = random.choice(PRODUCT_CATEGORIES)
    
    return activity

def generate_inventory_update():
    """Generate a random inventory update event"""
    product_id = random.choice(PRODUCT_IDS)
    operation = random.choice(['restock', 'adjust', 'reserve', 'release'])
    
    # Base inventory update
    update = {
        'update_id': str(uuid.uuid4()),
        'product_id': product_id,
        'timestamp': datetime.now().isoformat(),
        'operation': operation
    }
    
    # Add operation-specific data
    if operation == 'restock':
        update['quantity_change'] = random.randint(10, 100)
        update['supplier_id'] = f'SUP-{random.randint(1000, 9999)}'
    elif operation == 'adjust':
        update['quantity_change'] = random.randint(-10, 10)
        update['reason'] = random.choice(['inventory_count', 'damaged', 'lost', 'found'])
    elif operation == 'reserve':
        update['quantity_change'] = -random.randint(1, 5)
        update['order_id'] = f'ORD-{random.randint(10000, 99999)}'
    elif operation == 'release':
        update['quantity_change'] = random.randint(1, 5)
        update['order_id'] = f'ORD-{random.randint(10000, 99999)}'
        update['reason'] = random.choice(['cancelled', 'modified', 'returned'])
    
    return update

def run_data_generator():
    """Main function to run data generation at specified intervals"""
    producer = create_kafka_producer()
    logger.info(f"Starting data generator, producing to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Data generation interval: {DATA_INTERVAL_MS}ms")
    
    try:
        while True:
            # Generate different types of data with different frequencies
            
            # Always generate user activity (highest frequency)
            user_activity = generate_user_activity()
            producer.produce(
                USER_ACTIVITY_TOPIC,
                key=user_activity['user_id'],
                value=json.dumps(user_activity).encode('utf-8'),
                callback=delivery_callback
            )
            
            # Generate a transaction with 20% probability (lower frequency)
            if random.random() < 0.2:
                transaction = generate_transaction()
                producer.produce(
                    TRANSACTION_TOPIC,
                    key=transaction['transaction_id'],
                    value=json.dumps(transaction).encode('utf-8'),
                    callback=delivery_callback
                )
                
                # For each transaction, generate inventory updates for purchased items
                for item in transaction['items']:
                    inventory_update = {
                        'update_id': str(uuid.uuid4()),
                        'product_id': item['product_id'],
                        'timestamp': datetime.now().isoformat(),
                        'operation': 'reserve',
                        'quantity_change': -item['quantity'],
                        'order_id': transaction['transaction_id']
                    }
                    producer.produce(
                        INVENTORY_UPDATE_TOPIC,
                        key=item['product_id'],
                        value=json.dumps(inventory_update).encode('utf-8'),
                        callback=delivery_callback
                    )
            
            # Generate random inventory update with 10% probability (lowest frequency)
            if random.random() < 0.1:
                inventory_update = generate_inventory_update()
                producer.produce(
                    INVENTORY_UPDATE_TOPIC,
                    key=inventory_update['product_id'],
                    value=json.dumps(inventory_update).encode('utf-8'),
                    callback=delivery_callback
                )
            
            # Flush the producer to ensure messages are sent
            producer.flush()
            
            # Wait for the next interval
            time.sleep(DATA_INTERVAL_MS / 1000)
            
    except KeyboardInterrupt:
        logger.info("Data generator stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Clean up resources
        producer.flush()
        logger.info("Data generator stopped")

if __name__ == "__main__":
    run_data_generator()