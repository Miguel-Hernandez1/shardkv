package store

import (
	"bytes"

	"github.com/hashicorp/go-msgpack/v2/codec"
)

var handle = &codec.MsgpackHandle{}

func encodeMsgPack(in interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := codec.NewEncoder(&buf, handle)
	if err := enc.Encode(in); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMsgPack(data []byte, out interface{}) error {
	dec := codec.NewDecoderBytes(data, handle)
	return dec.Decode(out)
}
