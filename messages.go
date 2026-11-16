package main

import (
	"encoding/json"
	"msgfilter/common"
)

// MessageCodec — кодек для Message.
type MessageCodec struct{}

func (c *MessageCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *MessageCodec) Decode(data []byte) (interface{}, error) {
	var m common.Message
	err := json.Unmarshal(data, &m)
	return &m, err
}

// BlockEventCodec — кодек для BlockEvent.
type BlockEventCodec struct{}

func (c *BlockEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *BlockEventCodec) Decode(data []byte) (interface{}, error) {
	var e common.BlockEvent
	err := json.Unmarshal(data, &e)
	return &e, err
}

// ForbiddenWordEventCodec — кодек для ForbiddenWordEvent.
type ForbiddenWordEventCodec struct{}

func (c *ForbiddenWordEventCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *ForbiddenWordEventCodec) Decode(data []byte) (interface{}, error) {
	var e common.ForbiddenWordEvent
	err := json.Unmarshal(data, &e)
	return &e, err
}
