###Создание кластера Kafka + Kafka-connect + Prometheus + Grafana + Debezium###
1. Загружаем файл docker-compose.yaml

2. В директории с файлом docker-compose выполняем команду docker-compose up -d или docker compose up -d в случае естановленного в системе пакета docker-compose-v2

3. Выполняем команду docker ps в выводе должны быть контейнеры со статусом UP

###Регистрация коннектора###
1. Выполняем команду:

curl -X POST -H "Content-Type: application/json"  --data @pg-connector.json http://localhost:8083/connectors

2. Для проверки статуса коннектора выполняем команду:

curl http://localhost:8083/connectors/pg-connector/status | jq

Коннектор, и его задача должны находятся в состоянии RUNNING.

###Добавление тестовых данных в PostgreSQL:

1. Для добавления тестовых данных выполняем команду:

docker exec -i postgres psql -U postgres -d shop <<'SQL'
INSERT INTO users (name, email) VALUES
  ('John Doe', 'john@example.com'),
  ('Jane Smith', 'jane@example.com'),
  ('Alice Johnson', 'alice@example.com'),
  ('Bob Brown', 'bob@example.com');

INSERT INTO orders (user_id, product_name, quantity) VALUES
  (1, 'Product A', 2),
  (1, 'Product B', 1),
  (2, 'Product C', 5),
  (3, 'Product D', 3),
  (4, 'Product E', 4);
SQL