###Создание кластера SASL_SSL_Kafka ###
1. Загружаем файл docker-compose.yaml

2. В директории с файлом docker-compose выполняем команду docker-compose up -d или docker compose up -d в случае установленного в системе пакета docker-compose-v2

3. Выполняем команду docker ps в выводе должны быть контейнеры со статусом UP

4. Создаем УЗ 

docker exec broker-1 kafka-configs   --zookeeper zookeeper:2181   --alter   --add-config 'SCRAM-SHA-512=[password=AGcxQs7JSg3wDn7g]'   --entity-type users --entity-name admin

docker exec broker-1 kafka-configs   --zookeeper zookeeper:2181  --alter   --add-config 'SCRAM-SHA-512=[password=test1]'   --entity-type users --entity-name producer

docker exec broker-1 kafka-configs   --zookeeper zookeeper:2181   --alter   --add-config 'SCRAM-SHA-512=[password=test2]'   --entity-type users --entity-name consumer

5. Создаю topic-1

docker exec -it broker-1 kafka-topics --bootstrap-server broker-1:19093  --command-config /etc/kafka/secrets/client1_ssl.conf --create --topic topic-1 --replication-factor 3 --partitions 1

6. Создаю topic-2
docker exec -it broker-1 kafka-topics --bootstrap-server broker-1:19093  --command-config /etc/kafka/secrets/client1_ssl.conf --create --topic topic-2 --replication-factor 3 --partitions 1

7. Даю права продюсеру на topic-1

docker exec broker-1 kafka-acls --bootstrap-server localhost:19093   --command-config /etc/kafka/secrets/client1_ssl.conf   --add --allow-principal User:producer --producer --topic topic-1

8. Даю права консюмеру на topic-1

docker exec broker-1 kafka-acls --bootstrap-server localhost:19093   --command-config /etc/kafka/secrets/client1_ssl.conf   --add --allow-principal User:consumer --consumer --topic topic-1 --group '*'

9. Даю права продюсеру на topic-2

docker exec broker-1 kafka-acls --bootstrap-server localhost:19093   --command-config /etc/kafka/secrets/client1_ssl.conf   --add --allow-principal User:producer --producer --topic topic-2

10. Проверяю список ACL

docker exec broker-1 kafka-acls --bootstrap-server localhost:19093   --command-config /etc/kafka/secrets/client1_ssl.conf   --list

11. Запись в топик topic-1 

docker exec -it broker-1 kafka-console-producer   --bootstrap-server localhost:19093   --topic topic-1   --producer.config /etc/kafka/secrets/producer_ssl.conf

12. Чтение из топика topic-1

docker exec -it broker-1 kafka-console-consumer   --bootstrap-server localhost:19093   --topic topic-1   --from-beginning   --consumer.config /etc/kafka/secrets/consumer_ssl.conf

13. Проверка запрета чтениия из topic-2

docker exec -it broker-1 kafka-console-consumer   --bootstrap-server localhost:19093   --topic topic-2   --from-beginning   --consumer.config /etc/kafka/secrets/consumer_ssl.conf



