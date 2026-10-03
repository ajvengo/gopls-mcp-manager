package transport

import (
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	segmentjson "github.com/segmentio/encoding/json"
)

// The wire fields and conversion match jsonrpc.DecodeMessage at SDK v1.7.0.
// Any rejected shape falls back to the SDK instead
// of maintaining a second set of protocol errors here. Parse owns raw fields;
// no zero-copy flags are used, so transport buffers may be safely reused.
func decodeScalar(raw []byte) (jsonrpc.Message, bool) {
	var wire struct {
		Version string          `json:"jsonrpc"`
		ID      any             `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   *jsonrpc.Error  `json:"error"`
	}
	remaining, err := segmentjson.Parse(raw, &wire, segmentjson.DontMatchCaseInsensitiveStructFields)
	if err != nil || len(remaining) != 0 || wire.Version != "2.0" {
		return nil, false
	}
	id, err := jsonrpc.MakeID(wire.ID)
	if err != nil {
		return nil, false
	}
	if wire.Method != "" {
		return &jsonrpc.Request{ID: id, Method: wire.Method, Params: wire.Params}, true
	}
	if !id.IsValid() {
		return nil, false
	}
	response := &jsonrpc.Response{ID: id, Result: wire.Result}
	if wire.Error != nil {
		response.Error = wire.Error
	}
	return response, true
}
