package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type sseResult struct {
	frame []byte
	err   error
}

// The SDK codec still owns JSON-RPC validation. This transport replaces its
// unbounded SSE framing and 100-event queue with one bounded frame per reader
// and an unbuffered handoff. Connection Close unblocks both network and handoff.
type sseConn struct {
	endpoint *url.URL
	body     io.ReadCloser
	budget   *Budget
	maxBytes int
	incoming chan sseResult
	done     chan struct{}
	once     sync.Once
}

// ConnectSSE opens a legacy SSE connection sharing the non-nil frame budget.
// ctx owns the stream until Close; a nonpositive maxBytes uses the default size.
func ConnectSSE(ctx context.Context, endpoint string, maxBytes int, budget *Budget) (mcp.Connection, error) {
	if maxBytes <= 0 {
		maxBytes = config.Default().MessageBytes
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	c := &sseConn{body: resp.Body, budget: budget, maxBytes: maxBytes,
		incoming: make(chan sseResult), done: make(chan struct{})}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_ = c.Close()
		return nil, fmt.Errorf("connect SSE: %s", resp.Status)
	}
	reader := bufio.NewReader(resp.Body)
	for {
		frame, readErr := c.readFrame(reader)
		err = readErr
		if err != nil {
			break
		}
		var name string
		var data []byte
		name, data, err = parseSSEFrame(frame)
		if err == nil && name == "" && data == nil {
			budget.release(cap(frame))
			continue
		}
		if err == nil && name != "endpoint" {
			err = fmt.Errorf("first SSE event is %q, want endpoint", name)
		}
		if err == nil {
			c.endpoint, err = req.URL.Parse(string(data))
		}
		budget.release(cap(frame))
		break
	}
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	go func() {
		for {
			frame, err := c.readFrame(reader)
			select {
			case c.incoming <- sseResult{frame, err}:
			case <-c.done:
				budget.release(cap(frame))
				return
			}
			if err != nil {
				return
			}
		}
	}()
	return c, nil
}

func (c *sseConn) SessionID() string { return "" }

func (c *sseConn) Read(ctx context.Context) (jsonrpc.Message, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.done:
			return nil, io.EOF
		case result := <-c.incoming:
			if result.err != nil {
				_ = c.Close()
				return nil, result.err
			}
			_, data, err := parseSSEFrame(result.frame)
			var msg jsonrpc.Message
			if err == nil && data != nil {
				msg, err = jsonrpc.DecodeMessage(data)
			}
			c.budget.release(cap(result.frame))
			if err != nil {
				_ = c.Close()
				return nil, err
			}
			if msg != nil {
				return msg, nil
			}
		}
	}
}

func (c *sseConn) Write(ctx context.Context, msg jsonrpc.Message) error {
	select {
	case <-c.done:
		return io.EOF
	default:
	}
	data, err := jsonrpc.EncodeMessage(msg)
	if err != nil {
		return err
	}
	if len(data) > c.maxBytes {
		return ErrMessageTooLarge
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("write SSE: %s", resp.Status)
	}
	return nil
}

func (c *sseConn) Close() error {
	var err error
	c.once.Do(func() { close(c.done); err = c.body.Close() })
	return err
}

func (c *sseConn) readFrame(reader *bufio.Reader) ([]byte, error) {
	var frame []byte
	lineEmpty := true
	for {
		part, err := reader.ReadSlice('\n')
		if len(part) > c.maxBytes-len(frame) {
			c.budget.release(cap(frame))
			return nil, ErrMessageTooLarge
		}
		if needed := len(frame) + len(part); needed > cap(frame) {
			size := min(c.maxBytes, max(needed, max(128, 2*cap(frame))))
			// During growth both buffers are live. Reserve the replacement before
			// allocating, and release the old capacity only after copying.
			if !c.budget.acquire(size) {
				c.budget.release(cap(frame))
				return nil, errors.New("upstream SSE byte budget exhausted")
			}
			next := make([]byte, len(frame), size)
			copy(next, frame)
			c.budget.release(cap(frame))
			frame = next
		}
		frame = append(frame, part...)
		for _, b := range part {
			if b != '\r' && b != '\n' {
				lineEmpty = false
			}
		}
		if err == nil {
			if lineEmpty {
				return frame, nil
			}
			lineEmpty = true
			continue
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) && len(frame) > 0 {
			return frame, nil
		}
		c.budget.release(cap(frame))
		return nil, err
	}
}

// Compact data fields in place. Destination always precedes unread fields, so
// multiline JSON needs no second payload buffer. Whitespace follows SDK v1.7.
func parseSSEFrame(frame []byte) (string, []byte, error) {
	var name, id, retry string
	written, fields := 0, 0
	for start := 0; start < len(frame); {
		end := bytes.IndexByte(frame[start:], '\n')
		if end < 0 {
			end = len(frame)
		} else {
			end += start + 1
		}
		line := bytes.TrimRight(frame[start:end], "\r\n")
		start = end
		if len(line) == 0 {
			continue
		}
		key, value, ok := bytes.Cut(line, []byte{':'})
		if !ok {
			return "", nil, errors.New("malformed SSE field")
		}
		value = bytes.TrimSpace(value)
		switch string(key) {
		case "event":
			name = string(value)
		case "id":
			id = string(value)
		case "retry":
			retry = string(value)
		case "data":
			if fields > 0 {
				frame[written] = '\n'
				written++
			}
			written += copy(frame[written:], value)
			fields++
		}
	}
	if name == "" && id == "" && retry == "" && written == 0 {
		return name, nil, nil
	}
	return name, frame[:written], nil
}
