package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// withRootsCapability makes every upstream ask this bridge for its roots. Some
// MCP clients omit the optional capability; without it gopls installs no file
// watcher and newly-created Go files remain invisible until its daemon restarts.
func withRootsCapability(params json.RawMessage) json.RawMessage {
	rewritten, _ := rewriteNested(params, "capabilities", func(capabilities map[string]json.RawMessage) bool {
		rootsKey := jsonKey(capabilities, "roots")
		if !absentJSON(capabilities[rootsKey]) {
			return false
		}
		capabilities[rootsKey] = json.RawMessage(`{}`)
		return true
	})
	return rewritten
}

// rewriteNested is params with the object under key replaced by what edit made
// of it, and reports whether that happened. Messages we cannot rewrite — ones
// that do not parse, whose nested value is not an object, that edit leaves
// alone, or that will not marshal back — are returned untouched and left for
// the far side to read as the client sent them. An absent nested object is
// created; every other key and field survives verbatim. The client's own
// spelling of each key is the one rewritten; see jsonKey.
func rewriteNested(params json.RawMessage, key string, edit func(map[string]json.RawMessage) bool) (json.RawMessage, bool) {
	return rewriteObject(params, func(outer map[string]json.RawMessage) bool {
		nestedKey := jsonKey(outer, key)
		nested := make(map[string]json.RawMessage)
		if raw := outer[nestedKey]; !absentJSON(raw) {
			if err := json.Unmarshal(raw, &nested); err != nil || nested == nil {
				return false
			}
		}
		if !edit(nested) {
			return false
		}
		rawNested, err := json.Marshal(nested)
		if err != nil {
			return false
		}
		outer[nestedKey] = rawNested
		return true
	})
}

// rewriteObject is message with edit applied to its top-level object, and
// reports whether that happened. A message that is not an object, or that edit
// leaves alone or cannot be marshalled back, is returned untouched.
func rewriteObject(message json.RawMessage, edit func(map[string]json.RawMessage) bool) (json.RawMessage, bool) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(message, &object); err != nil || object == nil {
		return message, false
	}
	if !edit(object) {
		return message, false
	}
	rewritten, err := json.Marshal(object)
	if err != nil {
		return message, false
	}
	return rewritten, true
}

// jsonKey is the key in object that a Go decoder would read as name: the exact
// spelling when it is there, and otherwise a case-insensitive match, since
// encoding/json falls back to one. Writing our own spelling beside a client's
// would leave two keys mapping to one field, and which of them gopls took would
// come down to the order they marshalled in — so the capability we add for it
// could silently not be the one it read.
func jsonKey(object map[string]json.RawMessage, name string) string {
	if _, exact := object[name]; exact {
		return name
	}
	for key := range object {
		if strings.EqualFold(key, name) {
			return key
		}
	}
	return name
}

// absentJSON reports whether a client left this value out, spelled either way:
// the key missing entirely, or present and null.
func absentJSON(raw json.RawMessage) bool {
	return len(raw) == 0 || strings.TrimSpace(string(raw)) == "null"
}

// errorResponse builds the one error shape this bridge sends, in both
// directions: refuse and failInFlight forward it to the client,
// answeredUpstream writes it back to a gopls. Shared so they cannot drift into
// answering the same way in different words.
func errorResponse(id jsonrpc.ID, code int64, format string, args ...any) *jsonrpc.Response {
	return &jsonrpc.Response{ID: id, Error: &jsonrpc.Error{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}}
}

func (r *router) readFromUpstream(conn mcp.Connection, worktree string) {
	for {
		msg, err := conn.Read(r.ctx)
		if err != nil {
			// An upstream dying is not the stdio client's problem. Its cached
			// connection fails the next write, which redials and retries once.
			r.failInFlight(conn, worktree, err)
			return
		}
		if answered, err := r.answeredUpstream(r.ctx, conn, worktree, msg); err != nil {
			_ = conn.Close()
			r.failInFlight(conn, worktree, err)
			return
		} else if answered {
			continue
		}
		if resp, ok := msg.(*jsonrpc.Response); ok {
			if _, claimed := r.finish(resp.ID, conn, nil); !claimed {
				// send() got to this id first — its write failed after the upstream
				// had already taken the message — and answered it. Forwarding now
				// would be a second reply to a call the client has closed.
				continue
			}
		}
		r.forward(msg)
	}
}

// answeredUpstream handles a server-initiated request here rather than passing
// it to the client, and reports whether it did.
//
// Every read loop that can see one has to call this — see answerRoots for what
// goes wrong when a roots/list reaches the client. Anything else is refused: an
// upstream holding an unanswerable question waits forever, so a definite error
// is the kinder reply.
//
// ctx is the caller's budget for talking to conn: the handshake answers roots
// inside its deadline (S2); steady-state replies get the delivery ceiling.
func (r *router) answeredUpstream(ctx context.Context, conn mcp.Connection, worktree string, msg jsonrpc.Message) (bool, error) {
	req, ok := msg.(*jsonrpc.Request)
	if !ok || !req.ID.IsValid() {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, sendBudget)
	defer cancel()
	if req.Method == "roots/list" {
		return true, r.answerRoots(ctx, conn, worktree, req.ID)
	}
	return true, conn.Write(ctx, errorResponse(req.ID, jsonrpc.CodeMethodNotFound,
		"gopls-mcp-manager does not relay %q to the client", req.Method))
}

// answerRoots tells an upstream that its one workspace root is the worktree it
// serves, instead of forwarding the question to the client.
//
// This is what makes a gopls notice a file created after it started: its watcher
// only watches the roots the client reports, and it only asks for them once it
// has seen the roots capability in initialize — which is why withRootsCapability
// puts one there. Left to the client, every upstream would hear the same answer,
// the single tree the session was opened in, and every other worktree would watch
// nothing and answer "no package metadata" for a file that is right there on
// disk — indistinguishable from a genuinely broken tree.
func (r *router) answerRoots(ctx context.Context, conn mcp.Connection, worktree string, id jsonrpc.ID) error {
	// Two strings in a fixed shape: marshaling them cannot fail, and there would
	// be nobody to tell anyway — the id belongs to the upstream, so an error
	// reply sent to the client would answer a question it never asked.
	//nolint:staticcheck // Legacy gopls SSE sessions still need roots for file watching.
	result, _ := json.Marshal(mcp.ListRootsResult{Roots: []*mcp.Root{{
		URI:  (&url.URL{Scheme: "file", Path: worktree}).String(),
		Name: filepath.Base(worktree),
	}}})
	return conn.Write(ctx, &jsonrpc.Response{ID: id, Result: result})
}
