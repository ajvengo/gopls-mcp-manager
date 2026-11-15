package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// Only the private owner connection uses these frames. Public MCP framing and
// batch correlation remain in the frontend throughout an owner replacement.
const (
	restartData byte = iota + 1
	restartPause
	restartPaused
	restartResume
)

type restartPauseState struct {
	id     uint64
	marker *jsonrpc.Request
	routed chan struct{}
	resume chan struct{}
	cancel context.CancelFunc
}

type restartMessage struct {
	message jsonrpc.Message
	err     error
}

type restartPeer struct {
	conn      net.Conn
	reader    io.Reader
	ctx       context.Context
	cancel    context.CancelFunc
	maxBytes  int
	frontend  bool
	incoming  chan restartMessage
	writes    chan struct{}
	mu        sync.Mutex
	sequence  uint64
	paused    *restartPauseState
	onPause   func() <-chan struct{}
	onResume  func()
	onDrained func()
}

func newRestartPeer(ctx context.Context, conn net.Conn, reader io.Reader, maxBytes int, frontend bool) *restartPeer {
	ctx, cancel := context.WithCancel(ctx)
	return &restartPeer{conn: conn, reader: reader, ctx: ctx, cancel: cancel,
		maxBytes: maxBytes, frontend: frontend, incoming: make(chan restartMessage), writes: make(chan struct{}, 1)}
}

func (p *restartPeer) start() { go p.read() }

func (p *restartPeer) pausedState() *restartPauseState {
	p.mu.Lock()
	pause := p.paused
	p.mu.Unlock()
	return pause
}

func (p *restartPeer) Close() error {
	p.cancel()
	return p.conn.Close()
}

func (*restartPeer) SessionID() string { return "" }

func (p *restartPeer) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case message := <-p.incoming:
		return message.message, message.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, io.EOF
	}
}

func (p *restartPeer) Write(ctx context.Context, message jsonrpc.Message) error {
	data, err := jsonrpc.EncodeMessage(message)
	if err != nil {
		return err
	}
	return p.write(ctx, restartData, data)
}

func (p *restartPeer) write(ctx context.Context, kind byte, data []byte) error {
	if kind == restartData && len(data) > p.maxBytes || kind != restartData && len(data) != 8 {
		return fmt.Errorf("private frame exceeds message limit")
	}
	for {
		select {
		case p.writes <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.ctx.Done():
			return io.EOF
		}
		p.mu.Lock()
		pause := p.paused
		p.mu.Unlock()
		if !p.frontend || kind != restartData || pause == nil {
			err := p.writeLocked(ctx, kind, data)
			<-p.writes
			return err
		}
		<-p.writes
		select {
		case <-pause.resume:
		case <-ctx.Done():
			return ctx.Err()
		case <-p.ctx.Done():
			return io.EOF
		}
	}
}

func (p *restartPeer) writeLocked(ctx context.Context, kind byte, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if kind != restartData {
		// An aborted restart must not leave half a control frame on a live
		// session. Once started, finish it independently of the command.
		controlCtx, cancel := context.WithTimeout(p.ctx, sendBudget)
		defer cancel()
		ctx = controlCtx
	}
	deadline := time.Now().Add(sendBudget)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	if err := p.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); _ = p.conn.SetWriteDeadline(time.Now()) })
	defer func() {
		if !stop() {
			<-done
		}
		_ = p.conn.SetWriteDeadline(time.Time{})
	}()
	var header [5]byte
	header[0] = kind
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
	for _, part := range [][]byte{header[:], data} {
		for len(part) > 0 {
			n, err := p.conn.Write(part)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			part = part[n:]
		}
	}
	return nil
}

func restartToken(id uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, id)
	return data
}

func (p *restartPeer) read() {
	defer p.cancel()
	initialPause := p.pausedState()
	if !p.frontend && initialPause != nil && initialPause.marker == nil {
		select {
		case <-initialPause.resume:
		case <-p.ctx.Done():
			return
		}
	}
	for {
		var header [5]byte
		_, err := io.ReadFull(p.reader, header[:])
		if err != nil {
			p.deliver(nil, err)
			return
		}
		size := binary.BigEndian.Uint32(header[1:])
		if header[0] == restartData && size > uint32(p.maxBytes) || header[0] != restartData && size != 8 {
			p.deliver(nil, fmt.Errorf("invalid private frame"))
			return
		}
		data := make([]byte, int(size))
		if _, err := io.ReadFull(p.reader, data); err != nil {
			p.deliver(nil, err)
			return
		}
		if header[0] == restartData {
			message, err := jsonrpc.DecodeMessage(data)
			if !p.deliver(message, err) || err != nil {
				return
			}
			continue
		}
		id := binary.BigEndian.Uint64(data)
		switch {
		case p.frontend && header[0] == restartPause:
			if err := p.pauseFrontend(id); err != nil {
				p.deliver(nil, err)
				return
			}
		case p.frontend && header[0] == restartResume:
			p.resumeFrontend(id)
		case !p.frontend && header[0] == restartPaused:
			pause := p.pausedState()
			if pause == nil || pause.id != id {
				continue
			}
			if !p.deliver(pause.marker, nil) {
				return
			}
			select {
			case <-pause.resume:
			case <-p.ctx.Done():
				return
			}
		default:
			p.deliver(nil, fmt.Errorf("unexpected private control frame"))
			return
		}
	}
}

func (p *restartPeer) deliver(message jsonrpc.Message, err error) bool {
	select {
	case p.incoming <- restartMessage{message, err}:
		return true
	case <-p.ctx.Done():
		return false
	}
}

func (p *restartPeer) pauseFrontend(id uint64) error {
	select {
	case p.writes <- struct{}{}:
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
	p.mu.Lock()
	if p.paused != nil {
		p.mu.Unlock()
		<-p.writes
		return fmt.Errorf("frontend already paused")
	}
	ctx, cancel := context.WithCancel(p.ctx)
	pause := &restartPauseState{id: id, resume: make(chan struct{}), cancel: cancel}
	p.paused = pause
	p.mu.Unlock()
	drained := make(chan struct{})
	close(drained)
	var ready <-chan struct{} = drained
	if p.onPause != nil {
		ready = p.onPause()
	}
	<-p.writes
	go func() {
		select {
		case <-ready:
		case <-ctx.Done():
			return
		}
		select {
		case p.writes <- struct{}{}:
		case <-ctx.Done():
			return
		}
		defer func() { <-p.writes }()
		if ctx.Err() == nil {
			if p.onDrained != nil {
				p.onDrained()
			}
			if err := p.writeLocked(ctx, restartPaused, restartToken(id)); err != nil {
				_ = p.Close()
			}
		}
	}()
	return nil
}

func (p *restartPeer) resumeFrontend(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused == nil || p.paused.id != id {
		return
	}
	p.paused.cancel()
	if p.onResume != nil {
		p.onResume()
	}
	close(p.paused.resume)
	p.paused = nil
}

func (p *restartPeer) pause(ctx context.Context, guards ...func() error) (*restartPauseState, error) {
	p.mu.Lock()
	if p.paused != nil {
		p.mu.Unlock()
		return nil, fmt.Errorf("owner attachment already paused")
	}
	p.sequence++
	pause := &restartPauseState{id: p.sequence, marker: &jsonrpc.Request{}, routed: make(chan struct{}), resume: make(chan struct{})}
	pause.marker.Extra = pause
	p.paused = pause
	p.mu.Unlock()
	if err := p.write(ctx, restartPause, restartToken(pause.id)); err != nil {
		return pause, err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for routed := false; ; {
		for _, guard := range guards {
			if err := guard(); err != nil {
				return pause, err
			}
		}
		if routed {
			return pause, nil
		}
		select {
		case <-pause.routed:
			routed = true
		case <-ticker.C:
		case <-ctx.Done():
			return pause, ctx.Err()
		case <-p.ctx.Done():
			return pause, io.EOF
		}
	}
}

func (p *restartPeer) resume(ctx context.Context) error {
	pause := p.pausedState()
	if pause == nil {
		return nil
	}
	if err := p.write(ctx, restartResume, restartToken(pause.id)); err != nil {
		// The paused reader would wait for resume forever, hiding EOF and
		// pinning the session; only Close releases it.
		_ = p.Close()
		return err
	}
	p.mu.Lock()
	close(pause.resume)
	p.paused = nil
	p.mu.Unlock()
	return nil
}

func (p *restartPeer) barrier(message jsonrpc.Message) chan struct{} {
	if request, ok := message.(*jsonrpc.Request); ok {
		if barrier, ok := request.Extra.(*restartPauseState); ok {
			return barrier.routed
		}
	}
	return nil
}

// HTTP requests waiting through a pause retain their original contexts. The
// existing listener cap bounds waiting handlers; cancellation never waits for
// the owner replacement.
type restartHTTPGate struct {
	mu      sync.Mutex
	active  int
	paused  bool
	changed chan struct{}
	drained chan struct{}
}

func (g *restartHTTPGate) notify() {
	if g.changed != nil {
		close(g.changed)
	}
	g.changed = make(chan struct{})
}

func (g *restartHTTPGate) enter(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.paused {
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			g.mu.Lock()
			return ctx.Err()
		}
		g.mu.Lock()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	g.active++
	return nil
}

func (g *restartHTTPGate) leave() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.active == 0 && g.drained != nil {
		close(g.drained)
		g.drained = nil
	}
}

func (g *restartHTTPGate) pause() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.paused = true
	g.notify()
	ready := make(chan struct{})
	if g.active == 0 {
		close(ready)
	} else {
		g.drained = ready
	}
	return ready
}

func (g *restartHTTPGate) resume() { g.mu.Lock(); g.paused = false; g.notify(); g.mu.Unlock() }
