package transport

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"
)

// The shipped message ceiling, for the tests that are not about the limit.
const defaultTestLimit = 4 << 20

func TestStdioMessageLimitAndReadAhead(t *testing.T) {
	t.Parallel()
	wire := `{"jsonrpc":"2.0","id":1,"method":"ping"}`
	for _, tc := range []struct {
		name, input  string
		limit, count int
		oversize     bool
	}{
		{"exact", wire + "\n", len(wire), 1, false},
		{"read-ahead", strings.Repeat(wire+"\n", 100), len(wire) + 1, 100, false},
		{"too-large", wire + "\n", len(wire) - 1, 1, true},
		{"incomplete", `{"jsonrpc":"` + strings.Repeat("x", 1024), 128, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn := NewStdio(io.NopCloser(strings.NewReader(tc.input)), io.Discard, tc.limit)
			defer func() { _ = conn.Close() }()
			for range tc.count {
				_, err := conn.Read(t.Context())
				if tc.oversize {
					require.ErrorIsf(t, err, ErrMessageTooLarge, "error = %v, want byte limit", err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestStdioMatchesSDK(t *testing.T) {
	t.Parallel()
	for _, wire := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"text":"hello"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":"x","result":{"a":[1,true,null]}}`,
		`{"jsonrpc":"2.0","id":-12,"error":{"code":-32000,"message":"failed","data":{"why":true}}}`,
		`{"jsonrpc":"2.0","id":9007199254740993,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":1.5,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":1,"METHOD":"ignored"}`,
		`{"JSONRPC":"2.0","id":1,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":{},"method":"tools/list"}`,
		`{"jsonrpc":"1.0","method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":null,"result":1}`,
		`{"jsonrpc":"2.0","id":1,"params":null,"method":"x"}`,
		`null`, `[]`, `{}`, `{"jsonrpc":`,
		`{"jsonrpc":"2.0","id":1,"method":"x"}x`,
		`{"jsonrpc":"2.0","id":1,"result":"` + strings.Repeat("a", 1<<20) + `"}`,
	} {
		t.Run(wire[:min(70, len(wire))], func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			sdk, err := (&mcp.IOTransport{Reader: io.NopCloser(strings.NewReader(wire + "\n")), Writer: writerOnly{io.Discard}}).Connect(ctx)
			require.NoError(t, err)
			defer func() { _ = sdk.Close() }()
			fast := NewStdio(io.NopCloser(strings.NewReader(wire+"\n")), io.Discard, defaultTestLimit)
			defer func() { _ = fast.Close() }()
			want, wantErr := sdk.Read(ctx)
			got, gotErr := fast.Read(ctx)
			require.Truef(t, (wantErr == nil) == (gotErr == nil), "error mismatch: SDK %v, fast %v", wantErr, gotErr)
			if gotErr != nil {
				return
			}
			wantWire, _ := jsonrpc.EncodeMessage(want)
			gotWire, _ := jsonrpc.EncodeMessage(got)
			require.Equalf(t, wantWire, gotWire, "got %s, want %s", gotWire, wantWire)
		})
	}
}

func TestStdioRejectsAmbiguousJSON(t *testing.T) {
	t.Parallel()
	for _, wire := range []string{
		`{"jsonrpc":"2.0","method":"first","method":"second"}`,
		`{"jsonrpc":"2.0","method":"x","params":{"file":"/a","file":"/b"}}`,
		"{\"jsonrpc\":\"2.0\",\"method\":\"\xff\"}",
	} {
		t.Run(wire, func(t *testing.T) {
			t.Parallel()
			conn := NewStdio(io.NopCloser(strings.NewReader(wire+"\n")), io.Discard, defaultTestLimit)
			defer func() { _ = conn.Close() }()
			_, err := conn.Read(t.Context())
			require.Error(t, err)
		})
	}
}

func TestStdioPreservesBatchResponses(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, wire        string
		messages, replies int
	}{
		{name: "requests only", messages: 2, replies: 2,
			wire: `[{"jsonrpc":"2.0","id":"a","method":"tools/list"},{"jsonrpc":"2.0","id":"b","method":"tools/list"}]`},
		{name: "mixed notifications", messages: 3, replies: 1,
			wire: `[{"jsonrpc":"2.0","method":"notifications/initialized"},{"jsonrpc":"2.0","id":"a","method":"tools/list"},{"jsonrpc":"2.0","method":"notifications/progress"}]`},
		{name: "notifications only", messages: 2,
			wire: `[{"jsonrpc":"2.0","method":"notifications/initialized"},{"jsonrpc":"2.0","method":"notifications/progress"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var output bytes.Buffer
			second := strings.NewReplacer(`"a"`, `"a2"`, `"b"`, `"b2"`).Replace(tc.wire)
			c := NewStdio(io.NopCloser(strings.NewReader(tc.wire+"\n"+second+"\n")), &output, defaultTestLimit)
			defer func() { _ = c.Close() }()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			for range 2 {
				var ids []jsonrpc.ID
				for range tc.messages {
					msg, err := c.Read(ctx)
					require.NoError(t, err)
					if id := msg.(*jsonrpc.Request).ID; id.IsValid() {
						ids = append(ids, id)
					}
				}
				require.Len(t, ids, tc.replies)
				for i, id := range slices.Backward(ids) {
					require.NoError(t, c.Write(ctx, &jsonrpc.Response{ID: id, Result: jsontext.Value(`{}`)}))
					if i > 0 {
						require.Zero(t, output.Len(), "partial batch response escaped")
					}
				}
				if tc.replies == 0 {
					require.Zero(t, output.Len())
					continue
				}
				var responses []jsontext.Value
				require.NoError(t, json.Unmarshal(output.Bytes(), &responses), "notifications stranded the batch response")
				require.Len(t, responses, tc.replies)
				output.Reset()
			}
		})
	}
}

func FuzzStdioNotificationBatches(f *testing.F) {
	f.Add(uint8(0), "")
	f.Add(uint8(4), "payload\nwith newlines")
	f.Fuzz(func(t *testing.T, count uint8, payload string) {
		if len(payload) > 1024 || !utf8.ValidString(payload) {
			t.Skip()
		}
		batch := []map[string]any{{"jsonrpc": "2.0", "id": "call", "method": "tools/list"}}
		for range int(count%8) + 1 {
			batch = append(batch, map[string]any{"jsonrpc": "2.0", "method": "notifications/progress", "params": map[string]string{"text": payload}})
		}
		wire, err := json.Marshal(batch)
		require.NoError(t, err)
		var output bytes.Buffer
		c := NewStdio(io.NopCloser(bytes.NewReader(append(wire, '\n'))), &output, defaultTestLimit)
		defer func() { _ = c.Close() }()
		for range len(batch) {
			_, err := c.Read(t.Context())
			require.NoError(t, err)
		}
		id, _ := jsonrpc.MakeID("call")
		require.NoError(t, c.Write(t.Context(), &jsonrpc.Response{ID: id, Result: jsontext.Value(`{}`)}))
		var responses []jsontext.Value
		require.NoError(t, json.Unmarshal(output.Bytes(), &responses))
		require.Len(t, responses, 1)
	})
}

func TestStdioCloseUnblocksRead(t *testing.T) {
	t.Parallel()
	reader, writer := io.Pipe()
	defer func() { _ = writer.Close() }()
	c := NewStdio(reader, io.Discard, defaultTestLimit)
	done := make(chan error, 1)
	go func() { _, err := c.Read(t.Context()); done <- err }()
	require.NoError(t, c.Close())
	select {
	case err := <-done:
		require.Error(t, err, "closed read succeeded")
	case <-time.After(time.Second):
		require.FailNow(t, "closed stdio read remained blocked")
	}
}

func FuzzDecodeScalarMatchesSDK(f *testing.F) {
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":{},"error":null}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"reuse","method":"notifications/cancelled","params":{"requestId":"reuse"}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"result":{"nested":[null,{},[]]}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"x","params":{"file":"/a.go"}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"a","error":{"code":-1,"message":"oops","data":[1]}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, ok := decodeScalar(raw)
		if !ok {
			return
		} // rejected input is delegated to the SDK
		want, err := jsonrpc.DecodeMessage(raw)
		require.NoErrorf(t, err, "fast path accepted rejected input: %v", err)
		wantWire, _ := jsonrpc.EncodeMessage(want)
		// Make buffer ownership part of the comparison.
		for i := range raw {
			raw[i] = 'x'
		}
		gotWire, _ := jsonrpc.EncodeMessage(got)
		require.Equalf(t, wantWire, gotWire, "wire mismatch: %s != %s", gotWire, wantWire)
	})
}

func BenchmarkStdioTransport(b *testing.B) {
	for _, fast := range []bool{false, true} {
		name := "sdk"
		if fast {
			name = "fast"
		}
		b.Run(name, func(b *testing.B) {
			left, right := net.Pipe()
			writer, err := (&mcp.IOTransport{Reader: left, Writer: left}).Connect(b.Context())
			require.NoError(b, err)
			var reader mcp.Connection
			if fast {
				reader = NewStdio(right, right, defaultTestLimit)
			} else {
				reader, err = (&mcp.IOTransport{Reader: right, Writer: right}).Connect(b.Context())
				require.NoError(b, err)
			}
			b.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			id, _ := jsonrpc.MakeID(float64(1))
			req := &jsonrpc.Request{ID: id, Method: "tools/call", Params: jsontext.Value(`{"name":"go_file_context","arguments":{"file":"/repo/main.go"}}`)}
			go func() {
				for b.Context().Err() == nil {
					if writer.Write(b.Context(), req) != nil {
						return
					}
				}
			}()
			b.ReportAllocs()
			for b.Loop() {
				_, err := reader.Read(b.Context())
				require.NoError(b, err)
			}
		})
	}
}
