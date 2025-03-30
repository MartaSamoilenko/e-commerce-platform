import os
import logging
from typing import Dict, Any, List, Optional
from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider

logger = logging.getLogger(__name__)

class CassandraClient:
    """Simplified Cassandra client wrapper"""
    
    def __init__(
        self, 
        contact_points: List[str] = None, 
        keyspace: str = None,
        username: str = None,
        password: str = None
    ):
        """Initialize Cassandra client"""
        # Set default values from environment if not provided
        if contact_points is None:
            contact_points_str = os.environ.get('CASSANDRA_CONTACT_POINTS', 'localhost')
            contact_points = contact_points_str.split(',')
            
        # Setup auth provider if credentials are provided
        auth_provider = None
        if username and password:
            auth_provider = PlainTextAuthProvider(username=username, password=password)
            
        # Connect to cluster
        self.cluster = Cluster(contact_points=contact_points, auth_provider=auth_provider)
        self.session = None
        
        # Connect to keyspace if provided
        if keyspace:
            self.connect_keyspace(keyspace)
    
    def connect_keyspace(self, keyspace: str) -> None:
        """
        Connect to a specific keyspace
        
        Args:
            keyspace: Keyspace name to connect to
        """
        self.session = self.cluster.connect(keyspace)
        logger.info(f"Connected to keyspace {keyspace}")
    
    def create_keyspace(self, keyspace: str, replication_factor: int = 1) -> None:
        """
        Create a keyspace if it doesn't exist
        
        Args:
            keyspace: Keyspace name to create
            replication_factor: Replication factor for the keyspace
        """
        # Connect to system keyspace first
        if self.session is None:
            self.session = self.cluster.connect()
            
        # Create keyspace with SimpleStrategy replication
        query = f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH REPLICATION = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
        """
        self.session.execute(query)
        logger.info(f"Created keyspace {keyspace} with replication factor {replication_factor}")
        
        # Connect to the new keyspace
        self.connect_keyspace(keyspace)
    
    def create_table(self, table_name: str, schema: str) -> None:
        """
        Create a table if it doesn't exist
        
        Args:
            table_name: Table name to create
            schema: Schema definition for the table
        """
        if self.session is None:
            raise ValueError("No keyspace connected. Call connect_keyspace first.")
            
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema})"
        self.session.execute(query)
        logger.info(f"Created table {table_name}")
    
    def insert(self, table_name: str, data: Dict[str, Any]) -> None:
        """
        Insert data into a table
        
        Args:
            table_name: Table name to insert into
            data: Dictionary of column names and values
        """
        if self.session is None:
            raise ValueError("No keyspace connected. Call connect_keyspace first.")
            
        # Filter out None values
        filtered_data = {k: v for k, v in data.items() if v is not None}
        
        # Convert datetime objects to strings if needed
        for key, value in filtered_data.items():
            if isinstance(value, datetime):
                filtered_data[key] = value.isoformat()
                
        columns = ', '.join(filtered_data.keys())
        placeholders = ', '.join(['%s'] * len(filtered_data))
        values = list(filtered_data.values())
        
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        try:
            self.session.execute(query, values)
        except Exception as e:
            logger.error(f"Error executing insert query: {e}")
            logger.error(f"Query: {query}")
            logger.error(f"Values: {values}")
            raise
    
    def select(self, table_name: str, columns: List[str] = None, where: Dict[str, Any] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Select data from a table
        
        Args:
            table_name: Table name to select from
            columns: List of columns to select (None for all)
            where: Dictionary of conditions
            limit: Maximum number of rows to return
            
        Returns:
            List of dictionaries with query results
        """
        if self.session is None:
            raise ValueError("No keyspace connected. Call connect_keyspace first.")
            
        # Prepare column selection
        column_str = '*'
        if columns:
            column_str = ', '.join(columns)
            
        # Prepare WHERE clause
        where_clause = ''
        values = []
        if where:
            conditions = []
            for column, value in where.items():
                conditions.append(f"{column} = %s")
                values.append(value)
            where_clause = "WHERE " + " AND ".join(conditions)
            
        # Prepare query
        query = f"SELECT {column_str} FROM {table_name} {where_clause} LIMIT {limit}"
        
        # Execute query
        rows = self.session.execute(query, values)
        
        # Convert to list of dictionaries
        result = []
        for row in rows:
            result.append(dict(row._asdict()))
            
        return result
    
    def close(self) -> None:
        """Close the Cassandra connection"""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")