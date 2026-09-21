package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
	"github.com/google/uuid"
)

var address string = "localhost:9095, localhost:9098, localhost:9103"

var topic string = "practical"

type payload struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Group int    `json:"group"`
}

func main() {
	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081")) // подключение к SchemaRegistry
	if err != nil {
		log.Printf("Ошибка при подключении к Schema Registry: %v", err) // вывожу информационную ошибку подклюения к SR в логи, не завершая при этом приложение
	}

	serializer, err := jsonschema.NewSerializer(srClient, serde.ValueSerde, jsonschema.NewSerializerConfig()) // создаю сериализатор данных
	if err != nil {
		log.Printf("Ошибка при создании сериализатора: %v", err) // вывожу информационную ошибку сериализации в логи, не завершая при этом приложение
	}

	key := uuid.New().String() // создаю уникальное значение ключа для каждого сообщения при использовании генерации UUID

	order := payload{ // наполняю структуру данных для записи в топик Producer'ом
		ID:    15,
		Name:  "transport",
		Group: 189,
	}

	msg, err := serializer.Serialize(topic, &order) // запускаю сериализацию сообщения
	log.Printf(string(msg))
	if err != nil {
		log.Printf("Ошибка при сериализации сообщения", err)
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{ // создаю нового продюсера
		"acks":                     1,       // подтверждение о доставке 1 раз
		"bootstrap.servers":        address, // задаю адреса брокеров при помощи ранее заданной переменной address
		"message.send.max.retries": 8000,    // задаю кол-во повторных отправок в случае неудачи доставки сообщения
	})
	if err != nil {
		log.Printf("Ошибка при создании продюсера: %v", err) // вывожу в логи ошибку создания продюсера не завершая приложения
	}

	sendChan := make(chan kafka.Event) // создаю канал данных
	count := 10                        // в качестве количества сообщений выбираю 10, для простоты проверки batchconsumer'a
	for i := 0; i < count; i++ {       // запускаю цикл отправки 10 сообщений в топик

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, // опредение записи во все партиции
			Key:            []byte(key),                                                        // рандомный ключ на основе UUID
			Value:          msg,
		}, sendChan)
		if err != nil {
			log.Printf("Не удалось отправить сообщение: %v", err) // вывожу в логи ошибку отправки сообщения не завершая приложения
		}
		e := <-sendChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Ошибка доставки: %v\n", m.TopicPartition.Error) // вывожу в логи сообщение ошибки отправки сообщения в партицию не завершая приложения
		} else {
			fmt.Printf("Сообщение доставлено в %v\n", m.TopicPartition) // вывожу в логи сообщение успешной отправки сообщения не завершая приложения
		}
	}
	close(sendChan)        // закрываю канал kafka.Event
	defer producer.Close() // закрываю соединение с продюсером
}