import os
import time
import random
import uuid
import logging
import threading
from datetime import datetime
from typing import Dict, Any, List
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.kafka_utils import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

INVENTORY_TOPIC = 'ecommerce-inventory'

class InventoryGenerator:
    """Generator for inventory updates"""
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize inventory generator"""
        self.kafka_producer = KafkaProducer(bootstrap_servers)
        self.running = False
        self.thread = None
        
        self.product_ids = [str(uuid.uuid4()) for _ in range(100)]
        
        self.locations = ['Warehouse A', 'Warehouse B', 'Store 1', 'Store 2', 'Store 3', 'Distribution Center']
        
    def _generate_inventory_update(self) -> Dict[str, Any]:
        product_id = random.choice(self.product_ids)
        location = random.choice(self.locations)
        
        update_type = random.choice(['restock', 'sale', 'return', 'adjustment', 'transfer'])
        
        if update_type == 'restock':
            quantity_change = random.randint(10, 100)
        elif update_type == 'sale':
            quantity_change = -random.randint(1, 5)
        elif update_type == 'return':
            quantity_change = random.randint(1, 3)
        elif update_type == 'adjustment':
            quantity_change = random.randint(-10, 10)
        else:  # transfer
            quantity_change = -random.randint(5, 20)
            
        update = {
            'update_id': str(uuid.uuid4()),
            'product_id': product_id,
            'location': location,
            'timestamp': datetime.now().isoformat(),
            'update_type': update_type,
            'quantity_change': quantity_change,
            'reason': f"{update_type.capitalize()} operation",
            'operator_id': f"user-{random.randint(1000, 9999)}"
        }
        
        if update_type == 'transfer':
            destinations = [loc for loc in self.locations if loc != location]
            update['destination'] = random.choice(destinations)
            
            transfer_in = update.copy()
            transfer_in['update_id'] = str(uuid.uuid4())
            transfer_in['location'] = update['destination']
            transfer_in['destination'] = update['location']
            transfer_in['update_type'] = 'transfer-in'
            transfer_in['quantity_change'] = abs(quantity_change)
            return [update, transfer_in]
            
        return [update]
    
    def start(self, interval_seconds: float = 2.0):
        """
        Start generating inventory updates in a separate thread
        
        Args:
            interval_seconds: Interval between updates in seconds
        """
        if self.running:
            logger.warning("Inventory generator is already running")
            return
            
        self.running = True
        self.thread = threading.Thread(
            target=self._generate_continuously,
            args=(interval_seconds,),
            daemon=True
        )
        self.thread.start()
        logger.info(f"Started inventory generator with interval of {interval_seconds} seconds")
    
    def _generate_continuously(self, interval_seconds: float):
        """Generate inventory updates continuously"""
        while self.running:
            try:
                updates = self._generate_inventory_update()
                
                for update in updates:
                    self.kafka_producer.produce(
                        INVENTORY_TOPIC, 
                        update, 
                        key=update['product_id']
                    )
                    logger.info(
                        f"Generated inventory update {update['update_id']} "
                        f"for product {update['product_id']} "
                        f"with change of {update['quantity_change']} units"
                    )
                    
                time.sleep(interval_seconds)
                
            except Exception as e:
                logger.error(f"Error generating inventory update: {e}")
                time.sleep(interval_seconds)
    
    def stop(self):
        """Stop generating inventory updates"""
        if not self.running:
            logger.warning("Inventory generator is not running")
            return
            
        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)
            logger.info("Stopped inventory generator")

if __name__ == "__main__":
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    
    generator = InventoryGenerator(bootstrap_servers)
    
    try:
        generator.start(interval_seconds=3.0)
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        generator.stop()
        logger.info("Inventory generator stopped by user")