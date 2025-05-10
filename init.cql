CREATE KEYSPACE IF NOT EXISTS ecommerce
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE ecommerce;

CREATE TABLE IF NOT EXISTS inventory (
  product_id uuid PRIMARY KEY,
  store_id_quantity  map<uuid, int>,
  ts        timestamp
);

CREATE TABLE IF NOT EXISTS sales (
  sale_id     uuid PRIMARY KEY,
  product_id  uuid,
  quantity    int,
  ts          timestamp,
  user_id     uuid,
  store_id    uuid
);

CREATE TABLE IF NOT EXISTS stores (
    store_id uuid   PRIMARY KEY,
    store_name      text 
);

CREATE TABLE IF NOT EXISTS products (
    product_id    uuid PRIMARY KEY,
    product_name  text,
    product_price double
);

CREATE TABLE IF NOT EXISTS users (
    user_id uuid PRIMARY KEY,
    user_name text
);
