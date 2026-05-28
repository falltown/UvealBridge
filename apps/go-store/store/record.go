package store

import "encoding/json"

// walOp identifies the kind of mutation recorded in a WAL entry.
type walOp string

const (
	opSet walOp = "set"
	opDel walOp = "del"
)

// walRecord is one logical mutation as stored in the WAL. JSON is used
// for the on-disk encoding because it is trivially human-debuggable
// (`cat wal.log`), produces no literal newlines (every '\n' inside a
// string is escaped), and parses with the standard library. The latter
// matters because the wal package frames records by newline.
type walRecord struct {
	Op    walOp  `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func encodeRecord(r walRecord) ([]byte, error) {
	return json.Marshal(r)
}

func decodeRecord(b []byte) (walRecord, error) {
	var r walRecord
	err := json.Unmarshal(b, &r)
	return r, err
}
