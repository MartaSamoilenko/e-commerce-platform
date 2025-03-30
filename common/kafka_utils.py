import json
import os
import logging
from typing import Dict, Any, List, Callable
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

logger = logging.getLogger(__name__)

class KafkaProducer:
    """Simplified Kafka Producer wrapper"""
    
    def __init__(self, bootstrap_servers: str = None):
        """Initialize Kafka Producer"""
        if bootstrap_servers is None:
            bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'ecommerce-producer'
        })
        
    def produce(self, topic: str, value: Dict[str, Any], key: str = None) -> None:
        """
        Produce a message to a Kafka topic
        
        Args:
            topic: Kafka topic name
            value: Message value as a dictionary (will be JSON encoded)
            key: Optional message key
        """
        try:
            # Convert value to JSON string
            value_json = json.dumps(value).encode('utf-8')
            
            # Produce message
            self.producer.produce(
                topic=topic,
                key=key.encode('utf-8') if key else None,
                value=value_json,
                callback=self._delivery_report
            )
            # Flush to ensure delivery
            self.producer.flush()
            
        except Exception as e:
            logger.error(f"Error producing message to topic {topic}: {e}")
            raise
            
    def _delivery_report(self, err, msg):
        """Called once for each message produced to indicate delivery result"""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


class KafkaConsumer:
    """Simplified Kafka Consumer wrapper"""
    
    def __init__(self, topics: List[str], group_id: str, bootstrap_servers: str = None):
        """Initialize Kafka Consumer"""
        if bootstrap_servers is None:
            bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            
        self.consumer = Consumer({
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        })
        
        # Subscribe to topics
        self.consumer.subscribe(topics)
        
    def consume(self, process_message: Callable[[Dict[str, Any]], None], timeout: float = 1.0) -> None:
        """
        Consume messages from Kafka topics
        
        Args:
            process_message: Callback function to process each message
            timeout: Polling timeout in seconds
        """
        try:
            while True:
                msg = self.consumer.poll(timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error consuming message: {msg.error()}")
                else:
                    # Decode JSON message
                    try:
                        value = json.loads(msg.value().decode('utf-8'))
                        process_message(value)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to decode JSON message: {msg.value()}")
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            # Close consumer
            self.consumer.close()