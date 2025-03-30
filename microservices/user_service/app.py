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
USER_ACTIVITY_TOPIC = 'ecommerce-user-activity'
KEYSPACE = 'user_tracking'
USER_TABLE = 'users'
USER_ACTIVITY_TABLE = 'user_activities'
USER_SESSIONS_TABLE = 'user_sessions'
USER_CACHE_MAP = 'user-cache'

class UserTrackingService:
    """Service for tracking user activities"""
    
    def __init__(self):
        """Initialize the user tracking service"""
        # Get configuration from environment
        bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        cassandra_contact_points = os.environ.get('CASSANDRA_CONTACT_POINTS', 'localhost')
        hazelcast_host = os.environ.get('HAZELCAST_HOST', 'localhost')
        
        # Initialize Kafka consumer
        self.kafka_consumer = KafkaConsumer(
            topics=[USER_ACTIVITY_TOPIC],
            group_id='user-tracking-service',
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
        
        # Thread for processing user activities
        self.consumer_thread = None
        self.running = False
        
    def _init_database(self):
        """Initialize Cassandra keyspace and tables"""
        try:
            # Create keyspace
            self.cassandra.create_keyspace(KEYSPACE, replication_factor=1)
            
            # Create users table
            self.cassandra.create_table(
                USER_TABLE,
                """
                user_id UUID PRIMARY KEY,
                name TEXT,
                email TEXT,
                address TEXT,
                created_at TIMESTAMP,
                last_login TIMESTAMP,
                total_sessions INT,
                total_purchases INT,
                lifetime_value DECIMAL
                """
            )
            
            # Create user activities table
            self.cassandra.create_table(
                USER_ACTIVITY_TABLE,
                """
                activity_id UUID,
                user_id UUID,
                timestamp TIMESTAMP,
                action TEXT,
                session_id UUID,
                ip_address TEXT,
                user_agent TEXT,
                product_id UUID,
                product_name TEXT,
                category TEXT,
                quantity INT,
                search_query TEXT,
                results_count INT,
                PRIMARY KEY (user_id, timestamp, activity_id)
                """
            )
            
            # Create user sessions table
            self.cassandra.create_table(
                USER_SESSIONS_TABLE,
                """
                session_id UUID,
                user_id UUID,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration INT,
                ip_address TEXT,
                user_agent TEXT,
                activity_count INT,
                converted BOOLEAN,
                PRIMARY KEY (user_id, start_time, session_id)
                """
            )
            
            logger.info("Initialized user tracking database")
            
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            raise
    
    def process_user_activity(self, activity: Dict[str, Any]):
        """Process a user activity event"""
        try:
            # Extract activity data
            activity_id = activity.get('activity_id')
            user_id = activity.get('user_id')
            timestamp = activity.get('timestamp')
            action = activity.get('action')
            session_id = activity.get('session_id')
            
            logger.info(f"Processing user activity: {activity_id}, action: {action}, user: {user_id}")
            
            # Store activity in Cassandra
            self._store_activity(activity)
            
            # Update user session data
            self._update_session(activity)
            
            # Cache user data for quick access
            self._cache_user_data(user_id)
            
        except Exception as e:
            logger.error(f"Error processing user activity: {e}")
    
    def _store_activity(self, activity: Dict[str, Any]):
        """Store user activity in Cassandra"""
        try:
            # Prepare activity data for storage
            activity_data = {
                'activity_id': activity.get('activity_id'),
                'user_id': activity.get('user_id'),
                'timestamp': activity.get('timestamp'),
                'action': activity.get('action'),
                'session_id': activity.get('session_id'),
                'ip_address': activity.get('ip_address'),
                'user_agent': activity.get('user_agent')
            }
            
            # Add action-specific fields
            if activity.get('action') == 'view_product':
                activity_data.update({
                    'product_id': activity.get('product_id'),
                    'product_name': activity.get('product_name'),
                    'category': activity.get('category')
                })
            elif activity.get('action') in ['add_to_cart', 'remove_from_cart']:
                activity_data.update({
                    'product_id': activity.get('product_id'),
                    'product_name': activity.get('product_name'),
                    'quantity': activity.get('quantity')
                })
            elif activity.get('action') == 'search':
                activity_data.update({
                    'search_query': activity.get('search_query'),
                    'results_count': activity.get('results_count')
                })
            elif activity.get('action') == 'view_category':
                activity_data.update({
                    'category': activity.get('category')
                })
                
            # Insert into Cassandra
            self.cassandra.insert(USER_ACTIVITY_TABLE, activity_data)
            
        except Exception as e:
            logger.error(f"Error storing user activity: {e}")
            raise
    
    def _update_session(self, activity: Dict[str, Any]):
        """Update user session data"""
        try:
            user_id = activity.get('user_id')
            session_id = activity.get('session_id')
            timestamp = activity.get('timestamp')
            
            # Check if session exists in Hazelcast cache
            cache_key = f"session:{session_id}"
            session_data = self.hazelcast.get('sessions-cache', cache_key)
            
            if session_data:
                # Create updated session data
                updated_session = dict(session_data)  # Make a copy
                updated_session['end_time'] = timestamp
                updated_session['activity_count'] = session_data['activity_count'] + 1
                
                # Check if user converted (made a purchase)
                if activity.get('action') == 'purchase':
                    updated_session['converted'] = True
                    
                # Update session in cache with 30-minute TTL
                self.hazelcast.put('sessions-cache', cache_key, updated_session, ttl_seconds=1800)
                
                # Use the updated session data for further processing
                session_data = updated_session
            else:
                # Create new session
                session_data = {
                    'session_id': session_id,
                    'user_id': user_id,
                    'start_time': timestamp,
                    'end_time': timestamp,
                    'ip_address': activity.get('ip_address'),
                    'user_agent': activity.get('user_agent'),
                    'activity_count': 1,
                    'converted': activity.get('action') == 'purchase'
                }
                
                # Store session in cache with 30-minute TTL
                self.hazelcast.put('sessions-cache', cache_key, session_data, ttl_seconds=1800)
                
            # Periodically save session data to Cassandra
            # In a real system, you'd want to batch these writes or use a scheduled task
            if session_data['activity_count'] % 10 == 0:
                self._persist_session(session_data)
                
        except Exception as e:
            logger.error(f"Error updating session: {e}")
    
    def _persist_session(self, session_data: Dict[str, Any]):
        """Persist session data to Cassandra"""
        try:
            # Calculate session duration
            start_time = datetime.fromisoformat(session_data['start_time'])
            end_time = datetime.fromisoformat(session_data['end_time'])
            duration_seconds = int((end_time - start_time).total_seconds())
            
            # Prepare session data for storage
            cassandra_session = {
                'session_id': session_data['session_id'],
                'user_id': session_data['user_id'],
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration_seconds,
                'ip_address': session_data['ip_address'],
                'user_agent': session_data['user_agent'],
                'activity_count': session_data['activity_count'],
                'converted': session_data['converted']
            }
            
            # Insert into Cassandra
            self.cassandra.insert(USER_SESSIONS_TABLE, cassandra_session)
            
            logger.info(f"Persisted session {session_data['session_id']} to Cassandra")
            
        except Exception as e:
            logger.error(f"Error persisting session: {e}")
    
    def _cache_user_data(self, user_id: str):
        """Cache user data in Hazelcast"""
        try:
            cache_key = f"user:{user_id}"
            
            # Check if user data is already in cache
            if not self.hazelcast.contains(USER_CACHE_MAP, cache_key):
                # Fetch user data from Cassandra
                user_data = self.cassandra.select(
                    USER_TABLE,
                    where={'user_id': user_id},
                    limit=1
                )
                
                if user_data:
                    # Cache user data with 1-hour TTL
                    self.hazelcast.put(USER_CACHE_MAP, cache_key, user_data[0], ttl_seconds=3600)
                    
        except Exception as e:
            logger.error(f"Error caching user data: {e}")
    
    def start(self):
        """Start the user tracking service"""
        if self.running:
            logger.warning("User tracking service is already running")
            return
            
        self.running = True
        
        # Start consumer thread
        self.consumer_thread = threading.Thread(
            target=self._consume_user_activities,
            daemon=True
        )
        self.consumer_thread.start()
        
        logger.info("Started user tracking service")
    
    def _consume_user_activities(self):
        """Consume user activities from Kafka"""
        try:
            logger.info("Starting to consume user activities from Kafka")
            self.kafka_consumer.consume(self.process_user_activity)
        except Exception as e:
            logger.error(f"Error consuming user activities: {e}")
            self.running = False
    
    def stop(self):
        """Stop the user tracking service"""
        if not self.running:
            logger.warning("User tracking service is not running")
            return
            
        self.running = False
        
        # Close connections
        self.cassandra.close()
        self.hazelcast.close()
        
        logger.info("Stopped user tracking service")

# Main execution
if __name__ == "__main__":
    # Create and start user tracking service
    service = UserTrackingService()
    
    try:
        service.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        service.stop()
        logger.info("User tracking service stopped by user")