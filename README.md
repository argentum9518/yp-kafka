###Создание кластера Kafka###
1. Загружаем файл docker-compose.yaml

2. В директории с файлом docker-compose выполняем команду docker-compose up -d или docker compose up -d в случае естановленного в системе пакета docker-compose-v2

3. Выполняем команду docker ps в выводе должны быть контейнеры kafka[101..103] со статусом UP

###Создание топиков###

1. docker exec -ti kafka101 kafka-topics --bootstrap-server 172.16.255.242:19092 --create --topic filtered_messages --replication-factor 3 --partitions 1

2. docker exec -ti kafka101 kafka-topics --bootstrap-server 172.16.255.242:19092 --create --topic messages --replication-factor 3 --partitions 1

3. docker exec -ti kafka101 kafka-topics --bootstrap-server 172.16.255.242:19092 --create --topic blocked_users --replication-factor 3 --partitions 1

4. docker exec -ti kafka101 kafka-topics --bootstrap-server 172.16.255.242:19092 --create --topic forbidden_words --replication-factor 3 --partitions 1

###Сборка###
1. Для сборки бинаря выполнить команду go build . в корне проекта

2. В корне появится бинарь с именем msgfilter

###Запуск###

1. Для запуска приложения в корне выполнить команду ./msgfilter

###Тестирование###

1. Команда для блокировки пользователя
docker exec -ti kafka101 bash -c "echo 'root:{\"user_id\":\"root\",\"blocked\":true}' | kafka-console-producer --bootstrap-server 172.16.255.242:19092 --topic blocked_users --property "parse.key=true" --property "key.separator=:""

2. Проверка на отправку заблокированным пользователем (сообщение должно появится только в топике messages, в топик filtered_messages сообщение попадать не должно)
docker exec -ti kafka101 bash -c "echo '{\"from\":\"root\",\"to\":\"test\",\"content\":\"Test\"}' | kafka-console-producer --bootstrap-server 172.16.255.242:19092 --topic messages"

3. Считывание содержимого топика filtered_messages
docker exec -ti kafka101 bash -c "kafka-console-consumer --bootstrap-server 172.16.255.242:19092 --topic filtered_messages --from-beginning" - Вывод должен быть пустым

4. Добавление ключевого слова, которое фильтруем в данном случае важен именно ключ сообщения
 docker exec -ti kafka101 bash -c "echo 'spam:{\"word\":\"spam\",\"remove\":false}' | kafka-console-producer --bootstrap-server 172.16.255.242:19092 --topic forbidden_words --property "parse.key=true" --property "key.separator=:"" 

5. Отправка сообщения с содержанием "нецензурного" слова 
docker exec -ti kafka101 bash -c "echo '{\"from\":\"tor\",\"to\":\"test\",\"content\":\"This is message is a spam\"}' | kafka-console-producer --bootstrap-server 172.16.255.242:19092 --topic messages"  
Для последующей проверки выполняем команду docker exec -ti kafka101 bash -c "kafka-console-consumer --bootstrap-server 172.16.255.242:19092 --topic filtered_messages --from-beginning" в выводе "нецензурное" слово будет заменено "звездочками"

6. Rоманда для разблокировки пользователя
docker exec -ti kafka101 bash -c "echo 'root:{\"user_id\":\"root\",\"blocked\":false}' | kafka-console-producer --bootstrap-server 172.16.255.242:19092 --topic blocked_users --property "parse.key=true" --property "key.separator=:""

7. Проверка разблокировки пользователя (сообщение должно появится только в топике filtered_messages)
docker exec -ti kafka101 bash -c "echo '{\"from\":\"root\",\"to\":\"test\",\"content\":\"Test\"}' | kafka-console-producer --bootstrap-server 172.16.255.242:19092 --topic messages"