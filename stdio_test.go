package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

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
		`{"jsonrpc":"2.0","method":"first","method":"second"}`,
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
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sdk.Close() }()
			fast := newStdioConn(io.NopCloser(strings.NewReader(wire+"\n")), io.Discard)
			defer func() { _ = fast.Close() }()
			want, wantErr := sdk.Read(ctx)
			got, gotErr := fast.Read(ctx)
			if (wantErr == nil) != (gotErr == nil) {
				t.Fatalf("error mismatch: SDK %v, fast %v", wantErr, gotErr)
			}
			if gotErr != nil {
				return
			}
			wantWire, _ := jsonrpc.EncodeMessage(want)
			gotWire, _ := jsonrpc.EncodeMessage(got)
			if !bytes.Equal(gotWire, wantWire) {
				t.Fatalf("got %s, want %s", gotWire, wantWire)
			}
		})
	}
}

func TestStdioPreservesBatchResponses(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	wire := `[{"jsonrpc":"2.0","id":"a","method":"tools/list"},{"jsonrpc":"2.0","id":"b","method":"tools/list"}]` + "\n"
	c := newStdioConn(io.NopCloser(strings.NewReader(wire)), &output)
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for range 2 {
		if _, err := c.Read(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"b", "a"} {
		if err := c.Write(ctx, &jsonrpc.Response{ID: mustID(t, id), Result: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if id == "b" && output.Len() != 0 {
			t.Fatal("partial batch response escaped")
		}
	}
	var responses []json.RawMessage
	if err := json.Unmarshal(output.Bytes(), &responses); err != nil || len(responses) != 2 {
		t.Fatalf("batch response: %s, %v", output.Bytes(), err)
	}
}

func TestStdioCloseUnblocksRead(t *testing.T) {
	t.Parallel()
	reader, writer := io.Pipe()
	defer func() { _ = writer.Close() }()
	c := newStdioConn(reader, io.Discard)
	done := make(chan error, 1)
	go func() { _, err := c.Read(t.Context()); done <- err }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := mustRecv(t, done, "closed stdio read"); err == nil {
		t.Fatal("closed read succeeded")
	}
}

func FuzzDecodeScalarMatchesSDK(f *testing.F) {
	f.Add([]byte(`{"jsonrpc":"2.0","id":1,"method":"x","params":{"file":"/a.go"}}`))
	f.Add([]byte(`{"jsonrpc":"2.0","id":"a","error":{"code":-1,"message":"oops","data":[1]}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		got, ok := decodeScalar(raw)
		if !ok {
			return
		} // rejected input is delegated to the SDK
		want, err := jsonrpc.DecodeMessage(raw)
		if err != nil {
			t.Fatalf("fast path accepted rejected input: %v", err)
		}
		wantWire, _ := jsonrpc.EncodeMessage(want)
		// Make buffer ownership part of the comparison.
		for i := range raw {
			raw[i] = 'x'
		}
		gotWire, _ := jsonrpc.EncodeMessage(got)
		if !bytes.Equal(wantWire, gotWire) {
			t.Fatalf("wire mismatch: %s != %s", gotWire, wantWire)
		}
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
			if err != nil {
				b.Fatal(err)
			}
			var reader mcp.Connection
			if fast {
				reader = newStdioConn(right, right)
			} else {
				reader, err = (&mcp.IOTransport{Reader: right, Writer: right}).Connect(b.Context())
				if err != nil {
					b.Fatal(err)
				}
			}
			b.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
			req := &jsonrpc.Request{ID: mustID(b, float64(1)), Method: "tools/call", Params: fileCallParams("/repo/main.go")}
			go func() {
				for b.Context().Err() == nil {
					if writer.Write(b.Context(), req) != nil {
						return
					}
				}
			}()
			b.ReportAllocs()
			for b.Loop() {
				if _, err := reader.Read(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
