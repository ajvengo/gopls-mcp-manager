package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajvengo/gopls-mcp-manager/internal/config"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// httpBackend shares bounded legacy SSE lanes across independent HTTP calls.
// Only the routing goroutine owns lanes; one separate coordinator waits for
// filesystem resolution so independent warm HTTP requests can pass it.
// External IDs never enter this backend: different clients can reuse any ID.
type httpBackend struct {
	r        *router
	cancel   context.CancelFunc
	done     chan struct{}
	queue    chan delivery
	sequence atomic.Uint64
	mu       sync.Mutex
	pending  map[jsonrpc.ID]chan *jsonrpc.Response
}

func newHTTPBackend(parent context.Context, m *manager, home string) *httpBackend {
	ctx, cancel := context.WithCancel(parent)
	r := newRouter(ctx, m, home)
	r.stateless = true
	if m != nil && m.limits.Session > 0 {
		r.limits = m.limits
	}
	// The internal legacy session has its own capabilities and lifecycle.
	// HTTP initialization and client roots never mutate this shared session.
	r.initialize.Store(&jsonrpc.Request{Method: "initialize", Params: json.RawMessage(
		`{"protocolVersion":"2025-06-18","capabilities":{"roots":{}},"clientInfo":{"name":"gopls-mcp-manager","version":"1"}}`)})
	return &httpBackend{r: r, cancel: cancel, done: make(chan struct{}),
		queue: make(chan delivery, laneQueue), pending: make(map[jsonrpc.ID]chan *jsonrpc.Response)}
}

func (b *httpBackend) start() {
	cold := make(chan delivery, laneQueue)
	type resolved struct {
		call      delivery
		worktrees []string
		err       error
	}
	ready := make(chan resolved)
	go func() {
		for {
			select {
			case <-b.r.ctx.Done():
				return
			case call := <-cold:
				start := time.Now()
				worktrees, err := b.r.resolveWorktrees(call.ctx, call.req.Params)
				b.r.timed("resolution", start)
				select {
				case ready <- resolved{call, worktrees, err}:
				case <-b.r.ctx.Done():
					call.cancel()
					return
				}
			}
		}
	}()
	go func() {
		defer close(b.done)
		defer b.r.clearRequests()
		defer func() {
			b.r.closeLanes()
			for _, l := range b.r.lanes {
				<-l.done
			}
		}()
		for {
			select {
			case <-b.r.ctx.Done():
				return
			case result := <-ready:
				worktree, err := b.r.targetWorktrees(result.worktrees)
				if result.err != nil {
					err = result.err
				}
				b.r.routeResolved(result.call, worktree, err)
			case call := <-b.queue:
				if call.req.Method != "tools/call" {
					b.r.routePrepared(call)
					continue
				}
				b.r.timed("routing_queue", call.queued)
				if worktrees, complete := b.r.cachedWorktrees(call.req.Params); complete {
					worktree, err := b.r.targetWorktrees(worktrees)
					b.r.routeResolved(call, worktree, err)
					continue
				}
				select {
				case cold <- call:
				default:
					call.cancel()
					b.r.rejected("resolution_queue")
					b.r.failIngress(call, -32000, "resolution queue is full")
				}
			}
		}
	}()
	go func() {
		for {
			select {
			case <-b.r.ctx.Done():
				return
			case <-b.r.errs:
				b.cancel()
				return
			case msg := <-b.r.out:
				// Stateless JSON responses have no unsolicited notification stream.
				if resp, ok := msg.(*jsonrpc.Response); ok {
					b.mu.Lock()
					if reply := b.pending[resp.ID]; reply != nil {
						select {
						case reply <- resp:
						default:
						}
					}
					b.mu.Unlock()
				}
			}
		}
	}()
	if os.Getenv("GOPLS_MANAGER_METRICS") == "1" {
		go b.r.reportUsage()
	}
}

func (b *httpBackend) close() {
	b.cancel()
	<-b.done
}

func (b *httpBackend) call(ctx context.Context, method string, params any, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	id, _ := jsonrpc.MakeID(strconv.FormatUint(b.sequence.Add(1), 10))
	req := &jsonrpc.Request{ID: id, Method: method, Params: raw}
	reply := make(chan *jsonrpc.Response, 1)
	// Bound waiters independently of routing and downstream response writes.
	b.mu.Lock()
	if len(b.pending) >= b.r.limits.Session {
		b.mu.Unlock()
		b.r.rejected("http_outstanding")
		return &jsonrpc.Error{Code: -32000, Message: "HTTP outstanding request limit reached"}
	}
	b.pending[id] = reply
	b.mu.Unlock()
	defer func() {
		// Also cover admission racing backend shutdown, after its final sweep.
		b.r.finish(id, nil, nil)
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
	}()
	if call, ok := b.r.prepare(req); ok {
		select {
		case b.queue <- call:
		default:
			call.cancel()
			b.r.rejected("routing_queue")
			b.r.failIngress(call, -32000, "routing queue is full")
		}
	}
	select {
	case resp := <-reply:
		if resp.Error != nil {
			return resp.Error
		}
		return json.Unmarshal(resp.Result, result)
	case <-ctx.Done():
		cancelled, _ := json.Marshal(mcp.CancelledParams{RequestID: id.Raw(), Reason: "HTTP request ended"})
		b.r.cancelCall(&jsonrpc.Request{Method: "notifications/cancelled", Params: cancelled})
		return ctx.Err()
	case <-b.r.ctx.Done():
		return b.r.ctx.Err()
	}
}

// Initialize home once to retain gopls's instructions. Other lanes replay the
// same private handshake lazily; frontend initialization never reaches them.
func (b *httpBackend) initialize(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, routingBudget+sendBudget)
	defer cancel()
	var result mcp.InitializeResult
	if err := b.call(ctx, "initialize", b.r.initialize.Load().Params, &result); err != nil {
		return "", err
	}
	call, _ := b.r.prepare(&jsonrpc.Request{Method: "notifications/initialized"})
	select {
	case b.queue <- call:
		return result.Instructions, nil
	case <-ctx.Done():
		call.cancel()
		return "", ctx.Err()
	}
}

func newHTTPHandler(b *httpBackend, instructions string) http.Handler {
	s := mcp.NewServer(&mcp.Implementation{Name: "gopls-mcp-manager", Version: "1"}, &mcp.ServerOptions{
		Instructions: instructions + "\n\nTool calls route by absolute file, dir or files arguments. Calls without a usable path use the server's home worktree; HTTP requests do not share sticky routing.",
		Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
	})
	// Preserve upstream tool schemas, pagination, content and JSON-RPC errors.
	// The SDK owns HTTP negotiation and protocol validation; gopls owns tools.
	s.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			// Older protocol versions detach the SDK handler from the POST.
			// Our stateless endpoint has no later response channel, so disconnect
			// must cancel backend work for those versions too.
			if requestCtx, ok := ctx.Value(httpRequestContextKey{}).(context.Context); ok {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				stop := context.AfterFunc(requestCtx, cancel)
				defer stop()
				defer cancel()
			}
			switch method {
			case "tools/list":
				result := new(mcp.ListToolsResult)
				err := b.call(ctx, method, req.GetParams(), result)
				// Answering without calling next opts out of the SDK's result
				// post-processing, including the cache defaults it applies to every
				// result embedding Cacheable. CacheScope has no omitempty, so an unset
				// one marshals as "" and fails the client's "public"/"private" enum,
				// taking the whole tool list with it. "public" is the SDK's own
				// default, and what the protocol reads an absent scope as. Only
				// tools/list is affected because CallToolResult is not Cacheable — a
				// case added to this switch has to answer that question again.
				if result.CacheScope == "" {
					result.CacheScope = "public"
				}
				return result, err
			case "tools/call":
				result := new(mcp.CallToolResult)
				var raw json.RawMessage
				err := b.call(ctx, method, req.GetParams(), &raw)
				if err != nil {
					return result, err
				}
				return result, json.Unmarshal(withCompleteResultType(raw), result)
			default:
				return next(ctx, method, req)
			}
		}
	})
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s },
		&mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true, PropagateRequestCancellation: true,
			MaxRequestBodyBytes: int64(b.r.limits.MessageBytes)})
	// Bound request bodies and concurrent HTTP exchanges, including slow writers.
	// Reserve the maximum body size until the exchange ends, including chunked
	// requests. This bounds admitted wire bytes, not decoded result/heap overhead.
	messageBytes, budget := b.r.limits.MessageBytes, b.r.limits.HTTPBytes
	if messageBytes <= 0 {
		messageBytes = config.Default().MessageBytes
	}
	if budget <= 0 {
		budget = config.Default().HTTPBytes
	}
	slots := make(chan struct{}, min(b.r.limits.Session, budget/messageBytes))
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w = &httpReplyWriter{ResponseWriter: w}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			b.r.rejected("http_capacity")
			http.Error(w, "HTTP request capacity reached", http.StatusServiceUnavailable)
			return
		}
		requestCtx := context.WithValue(req.Context(), httpRequestContextKey{}, req.Context())
		handler.ServeHTTP(w, req.WithContext(requestCtx))
	})
}

// withCompleteResultType is a tools/call result carrying the resultType the SDK
// cannot put there itself. CallToolResult keeps resultType in an unexported
// field and, alone among the results, implements none of the interface
// setCompleteResultType looks for, so the SDK never sets it and its omitempty
// then drops it from the wire — and a client on protocol revision 2026-07-28
// rejects every tool call for the missing field. Unmarshalling is the only door
// into that field, so the value is spliced in before the result is decoded.
//
// "complete" is what an absent resultType means, and what the SDK sets for the
// results it does reach. An upstream that answered input_required keeps its own
// answer, as does a result this cannot parse.
func withCompleteResultType(result json.RawMessage) json.RawMessage {
	rewritten, _ := rewriteObject(result, func(fields map[string]json.RawMessage) bool {
		key := jsonKey(fields, "resultType")
		if !absentJSON(fields[key]) {
			return false
		}
		fields[key] = json.RawMessage(`"complete"`)
		return true
	})
	return rewritten
}

type httpRequestContextKey struct{}

// Start the write budget at the response, not at request arrival: tools may
// legitimately run longer than the delivery budget.
type httpReplyWriter struct {
	http.ResponseWriter
	once sync.Once
}

func (w *httpReplyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *httpReplyWriter) begin() {
	w.once.Do(func() { _ = http.NewResponseController(w.ResponseWriter).SetWriteDeadline(time.Now().Add(sendBudget)) })
}

func (w *httpReplyWriter) WriteHeader(code int) {
	w.begin()
	w.ResponseWriter.WriteHeader(code)
}

func (w *httpReplyWriter) Write(data []byte) (int, error) {
	w.begin()
	return w.ResponseWriter.Write(data)
}

func serveHTTP(ctx context.Context, m *manager, home, address string, output io.Writer) error {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	if !listener.Addr().(*net.TCPAddr).IP.IsLoopback() {
		return fmt.Errorf("HTTP MCP must listen on a loopback address")
	}
	b := newHTTPBackend(ctx, m, home)
	b.start()
	defer b.close()
	instructions, err := b.initialize(ctx)
	if err != nil {
		return fmt.Errorf("initialize home gopls: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", newHTTPHandler(b, instructions))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second,
		BaseContext: func(net.Listener) context.Context { return b.r.ctx }}
	stop := context.AfterFunc(b.r.ctx, func() { _ = server.Close() })
	defer stop()
	if _, err := fmt.Fprintf(output, "MCP endpoint: http://%s/mcp\n", listener.Addr()); err != nil {
		return err
	}
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
