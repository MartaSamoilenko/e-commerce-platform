import os
import sys
import json
import logging
import threading
import time
from datetime import datetime
from typing import Dict, Any, List

# Add parent directory to path to import common modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from common.kafka_utils import KafkaConsumer, KafkaProducer
from common.cassandra_utils import CassandraClient
from common.hazelcast_utils import HazelcastClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
TRANSACTION_TOPIC = 'ecommerce-transactions'
INVENTORY_UPDATE_TOPIC = 'ecommerce-inventory'
KEYSPACE = 'sales_processing'
TRANSACTIONS_TABLE = 'transactions'
ORDER_ITEMS_TABLE = 'order_items'
SALES_BY_PRODUCT_TABLE = 'sales_by_product'
SALES_BY_CATEGORY_TABLE = 'sales_by_category'
SALES_CACHE_MAP = 'sales-cache'

class SalesProcessingService:
    """Service for processing sales transactions"""
    
    def __init__(self):
        """Initialize the sales processing service"""
        # Get configuration from environment
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        cassandra_contact_points = os.environ.get('CASSANDRA_CONTACT_POINTS', 'localhost')
        hazelcast_host = os.environ.get('HAZELCAST_HOST', 'localhost')
        
        # Initialize Kafka consumer
        self.kafka_consumer = KafkaConsumer(
            topics=[TRANSACTION_TOPIC],
            group_id='sales-processing-service',
            bootstrap_servers=bootstrap_servers
        )
        
        # Initialize Kafka producer
        self.kafka_producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers
        )
        
        # Initialize Cassandra client
        self.cassandra = CassandraClient(
            contact_points=[cassandra_contact_points]
        )
        
        # Initialize Hazelcast client
        self.hazelcast = HazelcastClient(
            host=hazelcast_host
        )
        
        # Initialize database
        self._init_database()
        
        # Thread for processing transactions
        self.consumer_thread = None
        self.running = False
        
    def _init_database(self):
        """Initialize Cassandra keyspace and tables"""
        try:
            # Create keyspace
            self.cassandra.create_keyspace(KEYSPACE, replication_factor=1)
            
            # Create transactions table
            self.cassandra.create_table(
                TRANSACTIONS_TABLE,
                """
                transaction_id UUID PRIMARY KEY,
                user_id UUID,
                order_date TIMESTAMP,
                item_count INT,
                total_amount DECIMAL,
                payment_method TEXT,
                shipping_address TEXT,
                status TEXT
                """
            )
            
            # Create order items table
            self.cassandra.create_table(
                ORDER_ITEMS_TABLE,
                """
                transaction_id UUID,
                product_id UUID,
                product_name TEXT,
                category TEXT,
                quantity INT,
                unit_price DECIMAL,
                total_price DECIMAL,
                PRIMARY KEY (transaction_id, product_id)
                """
            )
            
            # Create sales by product table
            self.cassandra.create_table(
                SALES_BY_PRODUCT_TABLE,
                """
                product_id UUID,
                date TEXT,
                total_quantity INT,
                total_sales DECIMAL,
                order_count INT,
                PRIMARY KEY (product_id, date)
                """
            )
            
            # Create sales by category table
            self.cassandra.create_table(
                SALES_BY_CATEGORY_TABLE,
                """
                category TEXT,
                date TEXT,
                total_quantity INT,
                total_sales DECIMAL,
                order_count INT,
                PRIMARY KEY (category, date)
                """
            )
            
            logger.info("Initialized sales processing database")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def process_transaction(self, transaction: Dict[str, Any]):
        """Process a sales transaction"""
        try:
            # Extract transaction data
            transaction_id = transaction.get('transaction_id')
            user_id = transaction.get('user_id')
            order_date = transaction.get('order_date')
            items = transaction.get('items', [])
            
            logger.info(f"Processing transaction: {transaction_id}, items: {len(items)}")
            
            # Store transaction in Cassandra
            self._store_transaction(transaction)
            
            # Store order items
            self._store_order_items(transaction_id, items)
            
            # Update sales aggregations
            self._update_sales_aggregations(order_date, items)
            
            # Generate inventory updates for each item
            self._generate_inventory_updates(transaction_id, items)
            
            # Cache recent sales data
            self._cache_sales_data(transaction)
            
        except Exception as e:
            logger.error(f"Error processing transaction: {e}")
    
    def _store_transaction(self, transaction: Dict[str, Any]):
        """Store transaction in Cassandra"""
        try:
            # Prepare transaction data
            transaction_data = {
                'transaction_id': transaction.get('transaction_id'),
                'user_id': transaction.get('user_id'),
                'order_date': transaction.get('order_date'),
                'item_count': transaction.get('item_count'),
                'total_amount': transaction.get('total_amount'),
                'payment_method': transaction.get('payment_method'),
                'shipping_address': transaction.get('shipping_address'),
                'status': transaction.get('status')
            }
            
            # Insert into Cassandra
            self.cassandra.insert(TRANSACTIONS_TABLE, transaction_data)
            
        except Exception as e:
            logger.error(f"Error storing transaction: {e}")
            raise
    
    def _store_order_items(self, transaction_id: str, items: List[Dict[str, Any]]):
        """Store order items in Cassandra"""
        try:
            for item in items:
                # Prepare item data
                item_data = {
                    'transaction_id': transaction_id,
                    'product_id': item.get('product_id'),
                    'product_name': item.get('product_name'),
                    'category': item.get('category'),
                    'quantity': item.get('quantity'),
                    'unit_price': item.get('unit_price'),
                    'total_price': item.get('total_price')
                }
                
                # Insert into Cassandra
                self.cassandra.insert(ORDER_ITEMS_TABLE, item_data)
                
        except Exception as e:
            logger.error(f"Error storing order items: {e}")
            raise
    
    def _update_sales_aggregations(self, order_date: str, items: List[Dict[str, Any]]):
        """Update sales aggregations by product and category"""
        try:
            # Parse order date
            date = datetime.fromisoformat(order_date).strftime('%Y-%m-%d')
            
            # Group items by product and category
            product_quantities = {}
            product_sales = {}
            category_quantities = {}
            category_sales = {}
            
            for item in items:
                product_id = item.get('product_id')
                category = item.get('category')
                quantity = item.get('quantity')
                total_price = item.get('total_price')
                
                # Aggregate by product
                if product_id in product_quantities:
                    product_quantities[product_id] += quantity
                    product_sales[product_id] += total_price
                else:
                    product_quantities[product_id] = quantity
                    product_sales[product_id] = total_price
                
                # Aggregate by category
                if category in category_quantities:
                    category_quantities[category] += quantity
                    category_sales[category] += total_price
                else:
                    category_quantities[category] = quantity
                    category_sales[category] = total_price
            
            # Update product aggregations
            for product_id in product_quantities:
                # Check if product sales record exists
                existing = self.cassandra.select(
                    SALES_BY_PRODUCT_TABLE,
                    where={'product_id': product_id, 'date': date},
                    limit=1
                )
                
                if existing:
                    # Create new record with updated values
                    updated_record = {
                        'product_id': product_id,
                        'date': date,
                        'total_quantity': existing[0]['total_quantity'] + product_quantities[product_id],
                        'total_sales': existing[0]['total_sales'] + product_sales[product_id],
                        'order_count': existing[0]['order_count'] + 1
                    }
                else:
                    # Create new record
                    updated_record = {
                        'product_id': product_id,
                        'date': date,
                        'total_quantity': product_quantities[product_id],
                        'total_sales': product_sales[product_id],
                        'order_count': 1
                    }
                
                # Insert updated record
                self.cassandra.insert(SALES_BY_PRODUCT_TABLE, updated_record)
            
            # Update category aggregations
            for category in category_quantities:
                # Check if category sales record exists
                existing = self.cassandra.select(
                    SALES_BY_CATEGORY_TABLE,
                    where={'category': category, 'date': date},
                    limit=1
                )
                
                if existing:
                    # Create new record with updated values
                    updated_record = {
                        'category': category,
                        'date': date,
                        'total_quantity': existing[0]['total_quantity'] + category_quantities[category],
                        'total_sales': existing[0]['total_sales'] + category_sales[category],
                        'order_count': existing[0]['order_count'] + 1
                    }
                else:
                    # Create new record
                    updated_record = {
                        'category': category,
                        'date': date,
                        'total_quantity': category_quantities[category],
                        'total_sales': category_sales[category],
                        'order_count': 1
                    }
                
                # Insert updated record
                self.cassandra.insert(SALES_BY_CATEGORY_TABLE, updated_record)
                
        except Exception as e:
            logger.error(f"Error updating sales aggregations: {e}")
            raise
    
    def _generate_inventory_updates(self, transaction_id: str, items: List[Dict[str, Any]]):
        """Generate inventory updates for each item in the transaction"""
        try:
            for item in items:
                product_id = item.get('product_id')
                quantity = item.get('quantity')
                
                # Create inventory update for the sale
                inventory_update = {
                    'update_id': str(transaction_id) + '-' + str(product_id),
                    'product_id': product_id,
                    'location': 'Online Store',  # Default location
                    'timestamp': datetime.now().isoformat(),
                    'update_type': 'sale',
                    'quantity_change': -quantity,  # Negative for sales
                    'reason': f"Sale in transaction {transaction_id}",
                    'operator_id': 'system'
                }
                
                # Send inventory update to Kafka
                self.kafka_producer.produce(
                    INVENTORY_UPDATE_TOPIC,
                    inventory_update,
                    key=product_id
                )
                
                logger.info(f"Generated inventory update for product {product_id} with quantity change of -{quantity}")
                
        except Exception as e:
            logger.error(f"Error generating inventory updates: {e}")
    
    def _cache_sales_data(self, transaction: Dict[str, Any]):
        """Cache recent sales data in Hazelcast"""
        try:
            transaction_id = transaction.get('transaction_id')
            cache_key = f"transaction:{transaction_id}"
            
            # Cache transaction with 1-hour TTL
            self.hazelcast.put(SALES_CACHE_MAP, cache_key, transaction, ttl_seconds=3600)
            
            # Update daily sales cache
            order_date = datetime.fromisoformat(transaction.get('order_date')).strftime('%Y-%m-%d')
            daily_sales_key = f"daily_sales:{order_date}"
            
            daily_sales = self.hazelcast.get(SALES_CACHE_MAP, daily_sales_key) or {
                'date': order_date,
                'transaction_count': 0,
                'total_sales': 0.0,
                'item_count': 0
            }
            
            # Update daily sales
            daily_sales['transaction_count'] += 1
            daily_sales['total_sales'] += transaction.get('total_amount')
            daily_sales['item_count'] += transaction.get('item_count')
            
            # Store updated daily sales with 24-hour TTL
            self.hazelcast.put(SALES_CACHE_MAP, daily_sales_key, daily_sales, ttl_seconds=86400)
            
        except Exception as e:
            logger.error(f"Error caching sales data: {e}")
    
    def start(self):
        """Start the sales processing service"""
        if self.running:
            logger.warning("Sales processing service is already running")
            return
            
        self.running = True
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consume_transactions,
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info("Started sales processing service")
    
    def _consume_transactions(self):
        """Consume transactions from Kafka"""
        try:
            logger.info("Starting to consume transactions from Kafka")
            self.kafka_consumer.consume(self.process_transaction)
        except Exception as e:
            logger.error(f"Error consuming transactions: {e}")
            self.running = False
    
    def stop(self):
        """Stop the sales processing service"""
        if not self.running:
            logger.warning("Sales processing service is not running")
            return
            
        self.running = False
        
        # Close connections
        self.cassandra.close()
        self.hazelcast.close()
        
        logger.info("Stopped sales processing service")

# Main execution
if __name__ == "__main__":
    # Create and start sales processing service
    service = SalesProcessingService()
    
    try:
        service.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        service.stop()
        logger.info("Sales processing service stopped by user")