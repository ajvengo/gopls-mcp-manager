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
// Parameters we cannot rewrite — invalid ones, or ones that will not marshal
// back — are forwarded untouched and left for gopls to reject. The client's own
// spelling of each key is the one rewritten; see jsonKey for why.
func withRootsCapability(params json.RawMessage) json.RawMessage {
	var initialize map[string]json.RawMessage
	if err := json.Unmarshal(params, &initialize); err != nil || initialize == nil {
		return params
	}

	capabilitiesKey := jsonKey(initialize, "capabilities")
	var capabilities map[string]json.RawMessage
	if raw := initialize[capabilitiesKey]; !absentJSON(raw) {
		if err := json.Unmarshal(raw, &capabilities); err != nil || capabilities == nil {
			return params
		}
	}
	if capabilities == nil {
		capabilities = make(map[string]json.RawMessage)
	}
	if rootsKey := jsonKey(capabilities, "roots"); absentJSON(capabilities[rootsKey]) {
		capabilities[rootsKey] = json.RawMessage(`{}`)
	}

	rawCapabilities, err := json.Marshal(capabilities)
	if err != nil {
		return params
	}
	initialize[capabilitiesKey] = rawCapabilities

	rewritten, err := json.Marshal(initialize)
	if err != nil {
		return params
	}
	return rewritten
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
		if r.answeredUpstream(r.ctx, conn, worktree, msg) {
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
// inside its deadline (S2), a running reader has only the session.
func (r *router) answeredUpstream(ctx context.Context, conn mcp.Connection, worktree string, msg jsonrpc.Message) bool {
	req, ok := msg.(*jsonrpc.Request)
	if !ok || !req.ID.IsValid() {
		return false
	}
	if req.Method == "roots/list" {
		r.answerRoots(ctx, conn, worktree, req.ID)
		return true
	}
	_ = conn.Write(ctx, errorResponse(req.ID, jsonrpc.CodeMethodNotFound,
		"gopls-mcp-manager does not relay %q to the client", req.Method))
	return true
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
func (r *router) answerRoots(ctx context.Context, conn mcp.Connection, worktree string, id jsonrpc.ID) {
	// Two strings in a fixed shape: marshaling them cannot fail, and there would
	// be nobody to tell anyway — the id belongs to the upstream, so an error
	// reply sent to the client would answer a question it never asked.
	result, _ := json.Marshal(mcp.ListRootsResult{Roots: []*mcp.Root{{
		URI:  (&url.URL{Scheme: "file", Path: worktree}).String(),
		Name: filepath.Base(worktree),
	}}})
	// A write failure needs no recovery, and must not touch the lane holding
	// this connection: that state belongs to the lane's own goroutine, and this
	// runs on whichever one is reading the upstream. Left alone, the upstream
	// keeps the file set it started with — today's behaviour — and the next call
	// for it redials anyway.
	_ = conn.Write(ctx, &jsonrpc.Response{ID: id, Result: result})
}
