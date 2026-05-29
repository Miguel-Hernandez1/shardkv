package fsm

import (
	"encoding/json"
	"io"

	"github.com/hashicorp/raft"
)

type fsmSnapshot struct {
	data map[string][]byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.data)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func decodeSnapshot(rc io.ReadCloser) (map[string][]byte, error) {
	defer rc.Close()
	var data map[string][]byte
	return data, json.NewDecoder(rc).Decode(&data)
}
