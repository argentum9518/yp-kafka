package filter

import (
	"context"
	"log"
	"msgfilter/blocker"
	"msgfilter/censor"
	"msgfilter/common"
	"strings"

	"github.com/lovoo/goka"
)

var (
	Group       goka.Group  = "filter"            // Имя consumer
	InputTopic  goka.Stream = "messages"          // Исходный топик
	OutputTopic goka.Stream = "filtered_messages" // Целевой топик, в который сообщение попадает в результате обработки
)

func filter(ctx goka.Context, msg interface{}) {
	m := msg.(*common.Message)

	// 1. Проверка блокировки пользователя-отправителя
	if v := ctx.Lookup(blocker.Table, m.From); v != nil && v.(*blocker.BlockValue).Blocked {
		log.Printf("message from blocked user %q dropped", m.From)
		return
	}

	// Цензура: маскируем запрещённые слова "звездочками"
	words := strings.Fields(m.Content)
	for i, w := range words {
		if v := ctx.Lookup(censor.Table, w); v != nil && v.(*censor.CensorValue).Forbidden {
			words[i] = "***"
		}
	}
	m.Content = strings.Join(words, " ") // разделитель слов я выбрал пробел, т.к в сообщения часто содердат несколько слов

	// Отправляю в итоговый топик filtered_messages, ключ — получатель
	ctx.Emit(OutputTopic, m.To, m)
}

// Запуск процессора фильтрации.
func RunFilterProcessor(brokers []string) {
	g := goka.DefineGroup(Group,
		goka.Input(InputTopic, new(common.MessageCodec), filter),
		goka.Output(OutputTopic, new(common.MessageCodec)),
		goka.Lookup(blocker.Table, new(blocker.BlockValueCodec)),
		goka.Lookup(censor.Table, new(censor.CensorValueCodec)),
	)
	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		if err := p.Run(context.Background()); err != nil {
			log.Fatal(err)
		}
	}()
}
