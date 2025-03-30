#!/bin/bash

# Wait for Cassandra to be ready
# echo "Waiting for Cassandra to be ready..."
# sleep 30

echo "Creating keyspaces and tables directly..."

# Create user_tracking keyspace
docker-compose exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS user_tracking WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Create user_tracking tables
docker-compose exec cassandra cqlsh -e "USE user_tracking; CREATE TABLE IF NOT EXISTS users (user_id uuid PRIMARY KEY, name text, email text, address text, created_at timestamp, last_login timestamp, total_sessions int, total_purchases int, lifetime_value decimal);"
docker-compose exec cassandra cqlsh -e "USE user_tracking; CREATE TABLE IF NOT EXISTS user_activities (activity_id uuid, user_id uuid, timestamp timestamp, action text, session_id uuid, ip_address text, user_agent text, product_id uuid, product_name text, category text, quantity int, search_query text, results_count int, PRIMARY KEY (user_id, timestamp, activity_id));"
docker-compose exec cassandra cqlsh -e "USE user_tracking; CREATE TABLE IF NOT EXISTS user_sessions (session_id uuid, user_id uuid, start_time timestamp, end_time timestamp, duration int, ip_address text, user_agent text, activity_count int, converted boolean, PRIMARY KEY (user_id, start_time, session_id));"

# Create sales_processing keyspace
docker-compose exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS sales_processing WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Create sales_processing tables
docker-compose exec cassandra cqlsh -e "USE sales_processing; CREATE TABLE IF NOT EXISTS transactions (transaction_id uuid PRIMARY KEY, user_id uuid, order_date timestamp, item_count int, total_amount decimal, payment_method text, shipping_address text, status text);"
docker-compose exec cassandra cqlsh -e "USE sales_processing; CREATE TABLE IF NOT EXISTS order_items (transaction_id uuid, product_id uuid, product_name text, category text, quantity int, unit_price decimal, total_price decimal, PRIMARY KEY (transaction_id, product_id));"
docker-compose exec cassandra cqlsh -e "USE sales_processing; CREATE TABLE IF NOT EXISTS sales_by_product (product_id uuid, date text, total_quantity int, total_sales decimal, order_count int, PRIMARY KEY (product_id, date));"
docker-compose exec cassandra cqlsh -e "USE sales_processing; CREATE TABLE IF NOT EXISTS sales_by_category (category text, date text, total_quantity int, total_sales decimal, order_count int, PRIMARY KEY (category, date));"

# Create inventory_management keyspace
docker-compose exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS inventory_management WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};"

# Create inventory_management tables
docker-compose exec cassandra cqlsh -e "USE inventory_management; CREATE TABLE IF NOT EXISTS product_inventory (product_id uuid PRIMARY KEY, name text, category text, total_quantity int, available_quantity int, reserved_quantity int, reorder_threshold int, reorder_quantity int, last_updated timestamp);"
docker-compose exec cassandra cqlsh -e "USE inventory_management; CREATE TABLE IF NOT EXISTS inventory_updates (update_id uuid, product_id uuid, location text, timestamp timestamp, update_type text, quantity_change int, reason text, operator_id text, destination text, PRIMARY KEY (product_id, timestamp, update_id));"
docker-compose exec cassandra cqlsh -e "USE inventory_management; CREATE TABLE IF NOT EXISTS inventory_by_location (location text, product_id uuid, quantity int, last_updated timestamp, PRIMARY KEY (location, product_id));"

# Verify keyspaces were created
echo "Verifying keyspaces..."
docker-compose exec cassandra cqlsh -e "DESCRIBE KEYSPACES;"

echo "Cassandra initialization completed!"