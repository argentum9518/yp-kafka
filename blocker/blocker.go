package blocker

import (
	"context"
	"encoding/json"
	"log"
	"msgfilter/common"

	"github.com/lovoo/goka"
)

var (
	Group  goka.Group  = "blocker"              // Имя consumer
	Table  goka.Table  = goka.GroupTable(Group) // Goka group table
	Stream goka.Stream = "blocked_users"        // Имя топика, в который попадают заблокироывнные пользователи
)

// BlockValue — значение в таблице блокировок.
type BlockValue struct {
	Blocked bool `json:"blocked"`
}

type BlockValueCodec struct{}

func (c *BlockValueCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *BlockValueCodec) Decode(data []byte) (interface{}, error) {
	var v BlockValue
	err := json.Unmarshal(data, &v)
	return &v, err
}

func block(ctx goka.Context, msg interface{}) {
	var v *BlockValue
	if val := ctx.Value(); val != nil {
		v = val.(*BlockValue)
	} else {
		v = new(BlockValue)
	}
	event := msg.(*common.BlockEvent) // Использую тип из пакета common
	v.Blocked = event.Blocked
	ctx.SetValue(v)
}

// Запускаю процессор блокировки.
func RunBlockProcessor(brokers []string) {
	g := goka.DefineGroup(Group,
		goka.Input(Stream, new(common.BlockEventCodec), block),
		goka.Persist(new(BlockValueCodec)),
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
