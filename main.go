package main

import (
	"log"
	"msgfilter/blocker"
	"msgfilter/censor"
	"msgfilter/filter"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	brokers := []string{"localhost:19092", "localhost:29092", "localhost:39092"}

	blocker.RunBlockProcessor(brokers)
	censor.RunCensorProcessor(brokers)
	filter.RunFilterProcessor(brokers)

	log.Println("Все процессоры запущены. Ожидание сигнала завершения...")

	// Блокируем main горутину до получения сигнала
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	log.Println("Завершение работы.")
}
