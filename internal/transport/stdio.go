// Package transport implements bounded MCP stdio and legacy SSE connections.
// It owns framing and buffer accounting, not routing or shared process lifetime.
package transport

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	segmentjson "github.com/segmentio/encoding/json"
)

// stdioConn avoids the SDK's two streaming-decoder allocations per scalar
// message. Complete buffers use the same case-sensitive parser directly.
// The SDK still owns writes, batch correlation, and all fallback validation.
type stdioConn struct {
	input    io.ReadCloser
	sdk      mcp.Connection
	feed     *io.PipeWriter
	incoming chan decodedMessage
	closed   chan struct{}
	once     sync.Once
	closeErr error
	maxBytes int
}

type decodedMessage struct {
	message jsonrpc.Message
	err     error
}

type writerOnly struct{ io.Writer }

func (writerOnly) Close() error { return nil }

// NewStdio starts a bounded stdio connection. Close closes input, not output.
// An omitted or nonpositive limit uses the shipped default message size.
func NewStdio(input io.ReadCloser, output io.Writer, messageLimit ...int) mcp.Connection {
	reader, writer := io.Pipe()
	// IOTransport.Connect cannot fail: it only constructs the connection.
	sdk, _ := (&mcp.IOTransport{Reader: reader, Writer: writerOnly{output}}).Connect(context.Background())
	c := &stdioConn{input: input, sdk: sdk, feed: writer,
		incoming: make(chan decodedMessage), closed: make(chan struct{}), maxBytes: config.Default().MessageBytes}
	if len(messageLimit) > 0 && messageLimit[0] > 0 {
		c.maxBytes = messageLimit[0]
	}
	go c.read()
	return c
}

func (c *stdioConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.closed:
		return nil, io.EOF
	case result := <-c.incoming:
		return result.message, result.err
	}
}

func (c *stdioConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	return c.sdk.Write(ctx, msg)
}

func (c *stdioConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		_ = c.feed.Close()
		_ = c.sdk.Close()
		c.closeErr = c.input.Close()
	})
	return c.closeErr
}

func (c *stdioConn) SessionID() string { return c.sdk.SessionID() }

func (c *stdioConn) deliver(msg jsonrpc.Message, err error) bool {
	select {
	case c.incoming <- decodedMessage{msg, err}:
		return err == nil
	case <-c.closed:
		return false
	}
}

func (c *stdioConn) read() {
	input := &messageReader{Reader: c.input, limit: int64(c.maxBytes)}
	dec := json.NewDecoder(input)
	for {
		input.offset = dec.InputOffset()
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			c.deliver(nil, err)
			return
		}
		// Match the SDK's framing check on buffered trailing data.
		var next [1]byte
		if n, _ := dec.Buffered().Read(next[:]); n > 0 && next[0] != '\n' && next[0] != '\r' {
			// Let the SDK produce its original framing error.
			raw = append(raw, next[0])
		} else if msg, ok := decodeScalar(raw); ok {
			if !c.deliver(msg, nil) {
				return
			}
			continue
		}
		// The cold path preserves the SDK's batch bookkeeping and error types.
		var batch []json.RawMessage
		count := 1
		if json.Unmarshal(raw, &batch) == nil {
			count = max(1, len(batch))
		}
		if _, err := c.feed.Write(append(raw, '\n')); err != nil {
			c.deliver(nil, err)
			return
		}
		for range count {
			msg, err := c.sdk.Read(context.Background())
			if !c.deliver(msg, err) {
				return
			}
		}
	}
}

// Account for decoder read-ahead when resetting the per-value budget. A large
// or incomplete value is stopped before Decode can allocate an unbounded buffer.
// Whitespace between values counts toward the next value's budget.
type messageReader struct {
	io.Reader
	limit, read, offset int64
}

func (r *messageReader) Read(p []byte) (int, error) {
	remaining := r.limit - (r.read - r.offset)
	if remaining <= 0 {
		return 0, ErrMessageTooLarge
	}
	if int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.Reader.Read(p)
	r.read += int64(n)
	return n, err
}

// The wire fields and conversion match jsonrpc.DecodeMessage at SDK v1.6.0.
// Any rejected shape falls back to that function through IOTransport instead
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
