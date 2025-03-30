import os
import json
import logging
import hazelcast
from typing import Dict, Any, List, Optional, Union

logger = logging.getLogger(__name__)

class HazelcastClient:
    """Simplified Hazelcast client wrapper for distributed caching"""
    
    def __init__(self, host: str = None, port: int = 5701):
        """Initialize Hazelcast client"""
        if host is None:
            host = os.environ.get('HAZELCAST_HOST', 'localhost')
            
        # Initialize client
        config = hazelcast.ClientConfig()
        config.network_config.addresses.append(f"{host}:{port}")
        
        # Configure cluster name
        config.cluster_name = "ecommerce-cluster"
        
        # Configure connection retry
        config.connection_strategy.connection_retry.cluster_connect_timeout = 30
        
        try:
            self.client = hazelcast.HazelcastClient(config)
            logger.info(f"Connected to Hazelcast at {host}:{port}")
        except Exception as e:
            logger.error(f"Failed to connect to Hazelcast: {e}")
            raise
    
    def get_map(self, name: str):
        """
        Get a distributed map
        
        Args:
            name: Map name
            
        Returns:
            Hazelcast distributed map
        """
        return self.client.get_map(name).blocking()
    
    def put(self, map_name: str, key: str, value: Any, ttl_seconds: int = None) -> None:
        """
        Put a value in a distributed map
        
        Args:
            map_name: Map name
            key: Key to store
            value: Value to store (must be serializable)
            ttl_seconds: Time-to-live in seconds (None for no expiration)
        """
        try:
            distributed_map = self.get_map(map_name)
            
            # Store value
            if ttl_seconds:
                distributed_map.put(key, value, ttl=ttl_seconds)
            else:
                distributed_map.put(key, value)
                
            logger.debug(f"Stored key '{key}' in map '{map_name}'")
        except Exception as e:
            logger.error(f"Error storing value in Hazelcast map '{map_name}': {e}")
            raise
    
    def get(self, map_name: str, key: str) -> Any:
        """
        Get a value from a distributed map
        
        Args:
            map_name: Map name
            key: Key to retrieve
            
        Returns:
            Retrieved value or None if not found
        """
        try:
            distributed_map = self.get_map(map_name)
            return distributed_map.get(key)
        except Exception as e:
            logger.error(f"Error retrieving value from Hazelcast map '{map_name}': {e}")
            return None
    
    def remove(self, map_name: str, key: str) -> None:
        """
        Remove a value from a distributed map
        
        Args:
            map_name: Map name
            key: Key to remove
        """
        try:
            distributed_map = self.get_map(map_name)
            distributed_map.remove(key)
            logger.debug(f"Removed key '{key}' from map '{map_name}'")
        except Exception as e:
            logger.error(f"Error removing value from Hazelcast map '{map_name}': {e}")
            raise
    
    def contains(self, map_name: str, key: str) -> bool:
        """
        Check if a key exists in a distributed map
        
        Args:
            map_name: Map name
            key: Key to check
            
        Returns:
            True if key exists, False otherwise
        """
        try:
            distributed_map = self.get_map(map_name)
            return distributed_map.contains_key(key)
        except Exception as e:
            logger.error(f"Error checking key in Hazelcast map '{map_name}': {e}")
            return False
    
    def get_all_keys(self, map_name: str) -> List[str]:
        """
        Get all keys from a distributed map
        
        Args:
            map_name: Map name
            
        Returns:
            List of keys
        """
        try:
            distributed_map = self.get_map(map_name)
            return list(distributed_map.key_set())
        except Exception as e:
            logger.error(f"Error getting keys from Hazelcast map '{map_name}': {e}")
            return []
    
    def clear_map(self, map_name: str) -> None:
        """
        Clear all entries in a distributed map
        
        Args:
            map_name: Map name
        """
        try:
            distributed_map = self.get_map(map_name)
            distributed_map.clear()
            logger.debug(f"Cleared all entries from map '{map_name}'")
        except Exception as e:
            logger.error(f"Error clearing Hazelcast map '{map_name}': {e}")
            raise
    
    def close(self) -> None:
        """Close the Hazelcast client connection"""
        if self.client:
            self.client.shutdown()
            logger.info("Hazelcast client connection closed")