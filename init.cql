CREATE KEYSPACE IF NOT EXISTS ecommerce
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};

USE ecommerce;

CREATE TABLE IF NOT EXISTS sales (
  sale_id     uuid PRIMARY KEY,
  product_id  text,
  price       double,
  quantity    int,
  ts          timestamp
);