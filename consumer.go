package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/jsonschema"
)

var address string = "localhost:9095, localhost:9098, localhost:9103"

var topic string = "practical"

type payload struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Group int    `json:"group"`
}

func main() {
	go singleconsumer()                // запуск Go-рутины singleconsumer
	go batchconsumer()                 // запуск Go-рутины batchconsumer
	sigchan := make(chan os.Signal, 1) // создаю канал для ожидания сигнала завершения приложения
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan // посылаю сигнал завершения
	log.Printf("Завершаю работу")
}

func singleconsumer() { // создаю consumer, читающий сообщения по одному
	singconsumer, err := kafka.NewConsumer(&kafka.ConfigMap{ // создаю нового консьюмера
		"bootstrap.servers":  address,        // задаю адреса брокеров при помощи ранее заданной переменной address
		"group.id":           "single_group", // задаю имя consumer группы
		"enable.auto.commit": true,           // включаю автокоммит
	})
	if err != nil {
		log.Printf("Ошибка создания single consumer") // вывожу информационную ошибку сооздания consumer в логи, не завершая при этом приложение
	}

	log.Printf("SingleConsumer создан") // вывожу в логи успешное создание одиночного consumer
	defer singconsumer.Close()          // закрываю соединение с консьюмером

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig("http://localhost:8081")) // подключение к SchemaRegistry
	if err != nil {
		log.Printf("Ошибка при подключении к Schema Registry: %v", err) // вывожу информационную ошибку подклюения к SR в логи, не завершая при этом приложение
	}

	deserializer, err := jsonschema.NewDeserializer(srClient, serde.ValueSerde, jsonschema.NewDeserializerConfig()) // создаю десериализатор данных
	if err != nil {
		log.Printf("Ошибка при создании десериализатора: %v", err) // вывожу информационную ошибку создания десериализатора в логи, не завершая при этом приложение
	}

	err = singconsumer.SubscribeTopics([]string{topic}, nil) // подписываюсь на топик
	if err != nil {
		log.Printf("Ошибка подписки на топик:", topic) // вывожу информационную ошибку подписки на топик в логи, не завершая при этом приложение
	}

	run := true

	for run { // Запускаем бесконечный цикл без использования канала ожидания завершения приложения, т.к он уже создан в функции main
		select {
		default:
			ev := singconsumer.Poll(100) // делаю запрос на считывание сообщений из брокера
			if ev == nil {               // продолжаем выполнять цикл даже при получении пустого значения
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message: //     привожу Events к типу kafka.Message
				value := payload{}
				err := deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, &value) // десериализация сообщения полученного от брокера
				if err != nil {
					log.Printf("Ошибка десериализации", err) // вывожу информационную ошибку десериализации в логи, не завершая при этом приложение
				} else {
					log.Printf("Single consumer %s:\n%+v\n", *e.TopicPartition.Topic, value) // вывожу в логи десериализованное сообщение
				}
				// типу Ошибки брокера
			case kafka.Error: //     привожу Events к типу kafka.Error
				log.Println(os.Stderr, "Ошибка: %v\n", e.Code(), e) // вывожу полученную ошибку брокера в логи, не завершая при этом приложение
			}
		}
	}
}

func batchconsumer() { // создаю consumer выполняющий чтение пачки сообщений(батча) от 10+ сообщений
	batchСonsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  address,       // задаю адреса брокеров при помощи ранее заданной переменной address
		"group.id":           "batch_group", // задаю имя consumer группы
		"enable.auto.commit": false,         // выключаю автокоммит
		"fetch.wait.max.ms":  1000,          // время ожидания запроса
		"fetch.min.bytes":    810,           // опытным путем выяснено 1 сообщение = 81 байт
	})
	if err != nil {
		log.Printf("Ошибка создания batch consumer", err) // вывожу информационную ошибку сооздания consumer в логи, не завершая при этом приложение
	}

	batchСonsumer.SubscribeTopics([]string{topic}, nil) // подписываюсь на топик
	if err != nil {
		log.Printf("Ошибка подписки в batchConsumer %v:", err) // вывожу информационную ошибку подписки на топик в логи, не завершая при этом приложение
	}

	log.Printf("BatchConsumer создан") // вывожу в логи успешное создание одиночного consumer
	defer batchСonsumer.Close()        // закрываю соединение с консьюмером при завершении выполнения приложения

	srClient, err := schemaregistry.NewClient(schemaregistry.NewConfig(("http://localhost:8081")))                  // подключение к SchemaRegistry
	deserializer, err := jsonschema.NewDeserializer(srClient, serde.ValueSerde, jsonschema.NewDeserializerConfig()) // вывожу информационную ошибку подклюения к SR в логи, не завершая при этом приложение

	batch := make([]*kafka.Message, 0, 10) // создаю слайс размером 10

	for {
		ev := batchСonsumer.Poll(1000) // выполняю запрос сообщений от брокера
		if ev != nil {
			switch msg := ev.(type) {
			case *kafka.Message: //     привожу Events к типу kafka.Message
				batch = append(batch, msg)
			case kafka.Error: //     привожу Events к типу kafka.Error
				log.Printf("Ошибка брокера: %v\n", msg)
			}
		}

		for len(batch) < 10 { // при размере батча менее 10 сообщений, отправляю еще раз Poll, для накопления минимум 10 сообшений
			nextEv := batchСonsumer.Poll(0)
			if nextEv == nil {
				break
			}
			if msg, ok := nextEv.(*kafka.Message); ok {
				batch = append(batch, msg)
			}
		}

		if len(batch) >= 10 { // при размере батча  10 и более сообщений, начинаю обрабатывать батч
			log.Printf("Обработка батча из %d сообщений", len(batch)) // вывожу инфо о размере обрабатываемого батча

			for idx, item := range batch { // Обработка сообщений в цикле
				var order payload
				err = deserializer.DeserializeInto(*item.TopicPartition.Topic, item.Value, &order) // создаю десериализатор данных
				if err != nil {
					log.Printf("Ошибка десериализации", err) // вывожу информационную ошибку десериализации в логи, не завершая при этом приложение
				} else {
					log.Printf("Batch consumer: %s %d\n%+v\n", *item.TopicPartition.Topic, idx+1, order) // вывожу в логи десериализованное сообщение
				}
			}

			lastMessage := batch[len(batch)-1]                                  // получаю последнее сообщение из батча
			if _, err := batchСonsumer.CommitMessage(lastMessage); err != nil { // один раз коммичу оффсет после обработки всей пачки (по последнему сообщению)
				log.Printf("Ошибка коммита: %v\n", err) // вывожу информационную ошибку коммита в логи, не завершая при этом приложение
			} else {
				log.Printf("Оффсет успешно закоммичен. Offset: %s\n\n",
					lastMessage.TopicPartition.Offset) // вывожу закоммиченный offset
			}
			batch = batch[:0] // очищаю батч для дальнейшего выполнения цикла
		}
	}
}