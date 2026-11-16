package censor

import (
	"context"
	"encoding/json"
	"log"
	"msgfilter/common"

	"github.com/lovoo/goka"
)

var (
	Group  goka.Group  = "censor"               // Имя consumer
	Table  goka.Table  = goka.GroupTable(Group) // Goka group table
	Stream goka.Stream = "forbidden_words"      // Имя топика, в который попадают заблокированные пользователи
)

// BlockValue — значение в таблице цензуры.
type CensorValue struct {
	Forbidden bool `json:"forbidden"`
}

// Codec - как структура данных
type CensorValueCodec struct{}

func (c *CensorValueCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *CensorValueCodec) Decode(data []byte) (interface{}, error) {
	var v CensorValue
	err := json.Unmarshal(data, &v)
	return &v, err
}

func censor(ctx goka.Context, msg interface{}) {
	event := msg.(*common.ForbiddenWordEvent)
	var v *CensorValue
	if val := ctx.Value(); val != nil {
		v = val.(*CensorValue)
	} else {
		v = new(CensorValue)
	}
	if event.Remove {
		v.Forbidden = false
	} else {
		v.Forbidden = true
	}
	ctx.SetValue(v)
}

// Запускаю процессор цензуры.
func RunCensorProcessor(brokers []string) {
	g := goka.DefineGroup(Group,
		goka.Input(Stream, new(common.ForbiddenWordEventCodec), censor),
		goka.Persist(new(CensorValueCodec)),
	)
	p, err := goka.NewProcessor(brokers, g) // Создаю и конфигурирую процессор
	if err != nil {
		log.Fatal(err) // Вывод ошибки в лог и немедленное завершение процессора
	}
	go func() {
		if err := p.Run(context.Background()); err != nil {
			log.Fatal(err) // Вывод ошибки в лог и немедленное завершение процессора
		}
	}()
}
