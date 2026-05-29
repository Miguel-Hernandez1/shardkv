package fsm

import "encoding/json"

type OpType string

const (
	OpSet    OpType = "SET"
	OpDelete OpType = "DELETE"
)

// Command is the unit written to the Raft log.
type Command struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
}

func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	return cmd, json.Unmarshal(data, &cmd)
}
