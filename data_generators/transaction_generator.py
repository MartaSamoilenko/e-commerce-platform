import os
import time
import random
import uuid
import logging
import threading
from datetime import datetime
from typing import Dict, Any, List
from faker import Faker
import sys

# Add parent directory to path to import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_utils import KafkaProducer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Faker
fake = Faker()

# Constants
TRANSACTION_TOPIC = 'ecommerce-transactions'
USER_ACTIVITY_TOPIC = 'ecommerce-user-activity'

class TransactionGenerator:
    """Generator for e-commerce transactions"""
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize transaction generator"""
        self.kafka_producer = KafkaProducer(bootstrap_servers)
        self.running = False
        self.thread = None
        
        # Sample product catalog
        self.products = self._generate_product_catalog(100)
        
        # Sample users
        self.users = self._generate_users(1000)
        
    def _generate_product_catalog(self, count: int) -> List[Dict[str, Any]]:
        """Generate sample product catalog"""
        products = []
        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Books', 'Toys', 'Sports', 'Beauty', 'Jewelry']
        
        for _ in range(count):
            product_id = str(uuid.uuid4())
            category = random.choice(categories)
            
            product = {
                'product_id': product_id,
                'name': fake.unique.product_name(),
                'category': category,
                'price': round(random.uniform(5.99, 999.99), 2),
                'inventory': random.randint(0, 1000)
            }
            products.append(product)
            
        return products
    
    def _generate_users(self, count: int) -> List[Dict[str, Any]]:
        """Generate sample users"""
        users = []
        
        for _ in range(count):
            user_id = str(uuid.uuid4())
            
            user = {
                'user_id': user_id,
                'name': fake.name(),
                'email': fake.unique.email(),
                'address': fake.address(),
                'created_at': fake.date_time_this_year().isoformat()
            }
            users.append(user)
            
        return users
    
    def _generate_transaction(self) -> Dict[str, Any]:
        """Generate a random transaction"""
        # Choose a random user
        user = random.choice(self.users)
        
        # Generate order items (1-5 items)
        num_items = random.randint(1, 5)
        items = []
        total_amount = 0
        
        for _ in range(num_items):
            product = random.choice(self.products)
            quantity = random.randint(1, 3)
            item_price = product['price']
            item_total = item_price * quantity
            
            items.append({
                'product_id': product['product_id'],
                'product_name': product['name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': item_price,
                'total_price': item_total
            })
            
            total_amount += item_total
        
        # Generate payment information
        payment_method = random.choice(['credit_card', 'paypal', 'bank_transfer', 'crypto'])
        
        # Generate transaction
        transaction = {
            'transaction_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'order_date': datetime.now().isoformat(),
            'items': items,
            'item_count': len(items),
            'total_amount': round(total_amount, 2),
            'payment_method': payment_method,
            'shipping_address': user['address'],
            'status': 'completed'
        }
        
        return transaction
    
    def _generate_user_activity(self) -> Dict[str, Any]:
        """Generate random user activity data"""
        user = random.choice(self.users)
        product = random.choice(self.products)
        
        # Possible actions
        actions = ['view_product', 'add_to_cart', 'remove_from_cart', 'search', 'view_category']
        action = random.choice(actions)
        
        activity = {
            'activity_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'timestamp': datetime.now().isoformat(),
            'action': action,
            'session_id': str(uuid.uuid4()),
            'ip_address': fake.ipv4(),
            'user_agent': fake.user_agent()
        }
        
        # Add action-specific data
        if action == 'view_product':
            activity['product_id'] = product['product_id']
            activity['product_name'] = product['name']
            activity['category'] = product['category']
        elif action == 'add_to_cart' or action == 'remove_from_cart':
            activity['product_id'] = product['product_id']
            activity['product_name'] = product['name']
            activity['quantity'] = random.randint(1, 3)
        elif action == 'search':
            activity['search_query'] = fake.word()
            activity['results_count'] = random.randint(0, 100)
        elif action == 'view_category':
            activity['category'] = product['category']
            
        return activity
    
    def start(self, interval_seconds: float = 1.0):
        """
        Start generating transactions in a separate thread
        
        Args:
            interval_seconds: Interval between transactions in seconds
        """
        if self.running:
            logger.warning("Transaction generator is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(
            target=self._generate_continuously,
            args=(interval_seconds,),
            daemon=True
        )
        self.thread.start()
        logger.info(f"Started transaction generator with interval of {interval_seconds} seconds")
    
    def _generate_continuously(self, interval_seconds: float):
        """Generate transactions continuously"""
        while self.running:
            try:
                # Generate and send transaction
                transaction = self._generate_transaction()
                self.kafka_producer.produce(TRANSACTION_TOPIC, transaction, key=transaction['transaction_id'])
                logger.info(f"Generated transaction {transaction['transaction_id']} with {transaction['item_count']} items")
                
                # Generate and send user activity events (0-3 per transaction)
                num_activities = random.randint(0, 3)
                for _ in range(num_activities):
                    activity = self._generate_user_activity()
                    self.kafka_producer.produce(USER_ACTIVITY_TOPIC, activity, key=activity['activity_id'])
                    logger.info(f"Generated user activity {activity['activity_id']} of type {activity['action']}")
                    
                # Wait for next interval
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error generating transaction: {e}")
                time.sleep(interval_seconds)
    
    def stop(self):
        """Stop generating transactions"""
        if not self.running:
            logger.warning("Transaction generator is not running")
            return
            
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)
            logger.info("Stopped transaction generator")

# Main execution
if __name__ == "__main__":
    # Get bootstrap servers from environment
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    # Create transaction generator
    generator = TransactionGenerator(bootstrap_servers)
    
    try:
        # Start generating transactions every 2 seconds
        generator.start(interval_seconds=2.0)
        
        # Run for specified time or indefinitely
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        # Stop generator on keyboard interrupt
        generator.stop()
        logger.info("Transaction generator stopped by user")