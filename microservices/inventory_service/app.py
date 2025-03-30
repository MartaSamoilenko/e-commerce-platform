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
from common.kafka_utils import KafkaConsumer
from common.cassandra_utils import CassandraClient
from common.hazelcast_utils import HazelcastClient

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
INVENTORY_TOPIC = 'ecommerce-inventory'
KEYSPACE = 'inventory_management'
PRODUCT_INVENTORY_TABLE = 'product_inventory'
INVENTORY_UPDATES_TABLE = 'inventory_updates'
INVENTORY_BY_LOCATION_TABLE = 'inventory_by_location'
INVENTORY_CACHE_MAP = 'inventory-cache'

class InventoryManagementService:
    """Service for managing inventory"""
    
    def __init__(self):
        """Initialize the inventory management service"""
        # Get configuration from environment
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        cassandra_contact_points = os.environ.get('CASSANDRA_CONTACT_POINTS', 'localhost')
        hazelcast_host = os.environ.get('HAZELCAST_HOST', 'localhost')
        
        # Initialize Kafka consumer
        self.kafka_consumer = KafkaConsumer(
            topics=[INVENTORY_TOPIC],
            group_id='inventory-management-service',
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
        
        # Thread for processing inventory updates
        self.consumer_thread = None
        self.running = False
        
    def _init_database(self):
        """Initialize Cassandra keyspace and tables"""
        try:
            # Create keyspace
            self.cassandra.create_keyspace(KEYSPACE, replication_factor=1)
            
            # Create product inventory table
            self.cassandra.create_table(
                PRODUCT_INVENTORY_TABLE,
                """
                product_id UUID PRIMARY KEY,
                name TEXT,
                category TEXT,
                total_quantity INT,
                available_quantity INT,
                reserved_quantity INT,
                reorder_threshold INT,
                reorder_quantity INT,
                last_updated TIMESTAMP
                """
            )
            
            # Create inventory updates table
            self.cassandra.create_table(
                INVENTORY_UPDATES_TABLE,
                """
                update_id UUID,
                product_id UUID,
                location TEXT,
                timestamp TIMESTAMP,
                update_type TEXT,
                quantity_change INT,
                reason TEXT,
                operator_id TEXT,
                destination TEXT,
                PRIMARY KEY (product_id, timestamp, update_id)
                """
            )
            
            # Create inventory by location table
            self.cassandra.create_table(
                INVENTORY_BY_LOCATION_TABLE,
                """
                location TEXT,
                product_id UUID,
                quantity INT,
                last_updated TIMESTAMP,
                PRIMARY KEY (location, product_id)
                """
            )
            
            logger.info("Initialized inventory management database")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def process_inventory_update(self, update: Dict[str, Any]):
        """Process an inventory update"""
        try:
            # Extract update data
            update_id = update.get('update_id')
            product_id = update.get('product_id')
            location = update.get('location')
            timestamp = update.get('timestamp')
            update_type = update.get('update_type')
            quantity_change = update.get('quantity_change')
            
            logger.info(
                f"Processing inventory update: {update_id}, "
                f"product: {product_id}, "
                f"location: {location}, "
                f"change: {quantity_change}"
            )
            
            # Store update in Cassandra
            self._store_inventory_update(update)
            
            # Update product inventory
            self._update_product_inventory(product_id, quantity_change)
            
            # Update inventory by location
            self._update_inventory_by_location(product_id, location, quantity_change)
            
            # Cache inventory data
            self._cache_inventory_data(product_id)
            
        except Exception as e:
            logger.error(f"Error processing inventory update: {e}")
    
    def _store_inventory_update(self, update: Dict[str, Any]):
        """Store inventory update in Cassandra"""
        try:
            # Prepare update data
            update_data = {
                'update_id': update.get('update_id'),
                'product_id': update.get('product_id'),
                'location': update.get('location'),
                'timestamp': update.get('timestamp'),
                'update_type': update.get('update_type'),
                'quantity_change': update.get('quantity_change'),
                'reason': update.get('reason'),
                'operator_id': update.get('operator_id')
            }
            
            # Add destination for transfers
            if update.get('destination'):
                update_data['destination'] = update.get('destination')
                
            # Insert into Cassandra
            self.cassandra.insert(INVENTORY_UPDATES_TABLE, update_data)
            
        except Exception as e:
            logger.error(f"Error storing inventory update: {e}")
            raise
    
    def _update_product_inventory(self, product_id: str, quantity_change: int):
        """Update overall product inventory"""
        try:
            # Check if product exists
            product = self.cassandra.select(
                PRODUCT_INVENTORY_TABLE,
                where={'product_id': product_id},
                limit=1
            )
            
            now = datetime.now().isoformat()
            
            if product:
                # Get current values
                product_data = product[0]
                
                # Calculate new values
                new_total = max(0, product_data['total_quantity'] + quantity_change)
                new_available = max(0, product_data['available_quantity'] + quantity_change)
                
                # Create updated record
                updated_record = {
                    'product_id': product_id,
                    'name': product_data['name'],
                    'category': product_data['category'],
                    'total_quantity': new_total,
                    'available_quantity': new_available,
                    'reserved_quantity': product_data['reserved_quantity'],
                    'reorder_threshold': product_data['reorder_threshold'],
                    'reorder_quantity': product_data['reorder_quantity'],
                    'last_updated': now
                }
                
                # Insert updated record
                self.cassandra.insert(PRODUCT_INVENTORY_TABLE, updated_record)
            else:
                # Create new product
                # In a real system, you'd want to get product details from a product catalog
                new_quantity = max(0, quantity_change)
                
                new_record = {
                    'product_id': product_id,
                    'name': f"Product {product_id[:8]}",  # Placeholder name
                    'category': 'Unknown',  # Placeholder category
                    'total_quantity': new_quantity,
                    'available_quantity': new_quantity,
                    'reserved_quantity': 0,
                    'reorder_threshold': 10,  # Default threshold
                    'reorder_quantity': 50,   # Default reorder quantity
                    'last_updated': now
                }
                
                # Insert new record
                self.cassandra.insert(PRODUCT_INVENTORY_TABLE, new_record)
                
        except Exception as e:
            logger.error(f"Error updating product inventory: {e}")
            raise
    
    def _update_inventory_by_location(self, product_id: str, location: str, quantity_change: int):
        """Update inventory by location"""
        try:
            # Check if location inventory exists
            location_inventory = self.cassandra.select(
                INVENTORY_BY_LOCATION_TABLE,
                where={'location': location, 'product_id': product_id},
                limit=1
            )
            
            now = datetime.now().isoformat()
            
            if location_inventory:
                # Get current quantity
                current_quantity = location_inventory[0]['quantity']
                
                # Calculate new quantity (ensure it doesn't go below zero)
                new_quantity = max(0, current_quantity + quantity_change)
                
                # Create updated record
                updated_record = {
                    'location': location,
                    'product_id': product_id,
                    'quantity': new_quantity,
                    'last_updated': now
                }
                
                # Insert updated record
                self.cassandra.insert(INVENTORY_BY_LOCATION_TABLE, updated_record)
            else:
                # Create new location inventory
                new_quantity = max(0, quantity_change)
                
                new_record = {
                    'location': location,
                    'product_id': product_id,
                    'quantity': new_quantity,
                    'last_updated': now
                }
                
                # Insert new record
                self.cassandra.insert(INVENTORY_BY_LOCATION_TABLE, new_record)
                
        except Exception as e:
            logger.error(f"Error updating inventory by location: {e}")
            raise
    
    def _cache_inventory_data(self, product_id: str):
        """Cache inventory data in Hazelcast"""
        try:
            # Cache product inventory
            product_cache_key = f"product:{product_id}"
            
            # Get product from Cassandra
            product = self.cassandra.select(
                PRODUCT_INVENTORY_TABLE,
                where={'product_id': product_id},
                limit=1
            )
            
            if product:
                # Cache product with 1-hour TTL
                self.hazelcast.put(INVENTORY_CACHE_MAP, product_cache_key, product[0], ttl_seconds=3600)
                
                # Check if reorder is needed
                if product[0]['available_quantity'] <= product[0]['reorder_threshold']:
                    self._handle_reorder_notification(product[0])
            
            # Cache inventory by location
            locations = self.cassandra.select(
                INVENTORY_BY_LOCATION_TABLE,
                where={'product_id': product_id}
            )
            
            for location_data in locations:
                location_cache_key = f"location:{location_data['location']}:product:{product_id}"
                self.hazelcast.put(INVENTORY_CACHE_MAP, location_cache_key, location_data, ttl_seconds=3600)
                
        except Exception as e:
            logger.error(f"Error caching inventory data: {e}")
    
    def _handle_reorder_notification(self, product: Dict[str, Any]):
        """Handle reorder notification when inventory is low"""
        # In a real system, this would trigger an alert or automatic reorder
        # Here we just log the notification
        logger.warning(
            f"Inventory low for product {product['name']} (ID: {product['product_id']}): "
            f"Current quantity: {product['available_quantity']}, "
            f"Reorder threshold: {product['reorder_threshold']}, "
            f"Suggested reorder quantity: {product['reorder_quantity']}"
        )
        
        # You could also store this in a reorder queue in Hazelcast
        reorder_cache_key = f"reorder:{product['product_id']}"
        
        reorder_data = {
            'product_id': product['product_id'],
            'name': product['name'],
            'current_quantity': product['available_quantity'],
            'reorder_quantity': product['reorder_quantity'],
            'timestamp': datetime.now().isoformat()
        }
        
        # Cache reorder notification with 24-hour TTL
        self.hazelcast.put('reorder-notifications', reorder_cache_key, reorder_data, ttl_seconds=86400)
    
    def start(self):
        """Start the inventory management service"""
        if self.running:
            logger.warning("Inventory management service is already running")
            return
            
        self.running = True
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consume_inventory_updates,
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info("Started inventory management service")
    
    def _consume_inventory_updates(self):
        """Consume inventory updates from Kafka"""
        try:
            logger.info("Starting to consume inventory updates from Kafka")
            self.kafka_consumer.consume(self.process_inventory_update)
        except Exception as e:
            logger.error(f"Error consuming inventory updates: {e}")
            self.running = False
    
    def stop(self):
        """Stop the inventory management service"""
        if not self.running:
            logger.warning("Inventory management service is not running")
            return
            
        self.running = False
        
        # Close connections
        self.cassandra.close()
        self.hazelcast.close()
        
        logger.info("Stopped inventory management service")

# Main execution
if __name__ == "__main__":
    # Create and start inventory management service
    service = InventoryManagementService()
    
    try:
        service.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        service.stop()
        logger.info("Inventory management service stopped by user")