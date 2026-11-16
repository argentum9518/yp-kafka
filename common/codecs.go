package common

import "encoding/json"

// Создаю тип данных для кодека MessageCodec
type MessageCodec struct{}

func (c *MessageCodec) Encode(v interface{}) ([]byte, error) { return json.Marshal(v) }
func (c *MessageCodec) Decode(d []byte) (interface{}, error) {
	var m Message
	return &m, json.Unmarshal(d, &m)
}

// Создаю тип данных для кодека BlockEventCodec
type BlockEventCodec struct{}

func (c *BlockEventCodec) Encode(v interface{}) ([]byte, error) { return json.Marshal(v) }
func (c *BlockEventCodec) Decode(d []byte) (interface{}, error) {
	var e BlockEvent
	return &e, json.Unmarshal(d, &e)
}

// Создаю тип данных для кодека ForbiddenWordEventCodec
type ForbiddenWordEventCodec struct{}

func (c *ForbiddenWordEventCodec) Encode(v interface{}) ([]byte, error) { return json.Marshal(v) }
func (c *ForbiddenWordEventCodec) Decode(d []byte) (interface{}, error) {
	var e ForbiddenWordEvent
	return &e, json.Unmarshal(d, &e)
}
