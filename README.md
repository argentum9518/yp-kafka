###Создание кластера Kafka + Kafka-connect + Prometheus + Grafana + Debezium###
1. Загружаем файл docker-compose.yaml

2. В директории с файлом docker-compose выполняем команду docker-compose up -d или docker compose up -d в случае установленного в системе пакета docker-compose-v2

3. Выполняем команду docker ps в выводе должны быть контейнеры со статусом UP

4. Для проверки доступности Grafana в браузере необходимо перейти по адресу http://localhost:3000

в качестве Login/pass используется admin

5. Добавление data source в Grafana

Для добавления источника данных в левом меню кликаем пункт Connections -> Data sources -> Add new data sources

Выбираем тип data source Prometheus

В поле Prometheus server URL вводим адрес нашего Prometheus-server http://localhost:9090 кликаем Save & test

6. Для создания дашборда в Grafana необходимо импортировать JSON-файл debezium_dashboard.json из директории prometheus 

Для этого необходимо в левом меню выбрать вкладку Dashboards -> New -> Import dashboard и указать путь к файлу debezium_dashboard.json

7. Для проверки доступности метрик в браузере необходимо перейти по адресу http://localhost:9090/targets

статус Endpoint'a "kafka-connect" должен быть UP

8.  Для проверки создания топиков в консоли необходимо выполнить команду:

docker exec -ti kafka101 kafka-topics --bootstrap-server localhost:19092 --list

в выводе должны отображаться топики:

__consumer_offsets
connect-configs
connect-offsets
connect-status

9. Для создания БД и таблиц в ней необходимо выполнить Shell-скрипт из директории postgres

./postgres/init.sh


###Регистрация коннектора###
1. Выполняем команду:

curl -X POST -H "Content-Type: application/json"  --data @pg-connector.json http://localhost:8083/connectors

2. Для проверки статуса коннектора выполняем команду:

curl http://localhost:8083/connectors/pg-connector/status | jq

Коннектор, и его задача должны находятся в состоянии RUNNING.

###Добавление тестовых данных в PostgreSQL:

1. Для добавления тестовых данных выполняем Shell-скрипт из директории postgres

./postgres/add_data.sh

2. После добавления тестовых данных проверим наличие cdc-топиков при помощи команды:

docker exec -it kafka101 kafka-topics --bootstrap-server localhost:19092  --list | grep cdc

3. Чтение данных из cdc-топиков

docker exec -it kafka101 kafka-console-consumer --bootstrap-server localhost:19092   --topic cdc.public.orders --from-beginning --max-messages 1

docker exec -it kafka101 kafka-console-consumer --bootstrap-server localhost:19092   --topic cdc.public.users --from-beginning --max-messages 1



