#!/bin/bash
docker exec -ti postgres psql -h 127.0.0.1 -U postgres -c 'CREATE DATABASE shop;';
docker exec -ti postgres psql -h 127.0.0.1 -U postgres -d shop -c '
CREATE TABLE users (
id SERIAL PRIMARY KEY,
name VARCHAR(100),
email VARCHAR(100),
created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE orders (
id SERIAL PRIMARY KEY,
user_id INT REFERENCES users(id),
product_name VARCHAR(100),
quantity INT,
order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
CREATE PUBLICATION dbz_publication FOR TABLE users, orders;'
