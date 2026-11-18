package main

import (
	"testing"
	"testing/synctest"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/stretchr/testify/require"
)

func FuzzRetainedPayloadAccounting(f *testing.F) {
	const (
		admit byte = iota
		cancel
		discard
		consume
		releaseAgain
		actions
	)
	f.Add(uint8(0), []byte{admit, admit + actions, cancel, discard, admit + actions})
	f.Add(uint8(1), []byte{admit, cancel, discard, admit, releaseAgain, consume})
	f.Add(uint8(3), []byte{admit, cancel, admit + actions, cancel + actions, consume, discard + actions})
	f.Fuzz(func(t *testing.T, capacity uint8, input []byte) {
		if len(input) > 128 {
			return
		}
		synctest.Test(t, func(t *testing.T) {
			s := stateSupervisor(t)
			slots := 1 + int(capacity%4)
			bytesPerMessage := s.m.limits.MessageBytes
			s.m.limits.OwnerRetainedBytes = slots * bytesPerMessage
			// These entries represent the objects still owned by the ingress
			// and output queues. Only waiting entries own an operation credit.
			type session struct {
				router   *router
				ingress  *jsonrpc.Request
				waiting  bool
				replies  int
				released jsonrpc.Message
			}
			sessions := [2]session{{router: newRouter(t.Context(), s, testHome)}, {router: newRouter(t.Context(), s, testHome)}}
			id := mustID(t, "call")
			cancelRequest := &jsonrpc.Request{Params: []byte(`{"requestId":"call"}`)}
			occupancy := func() (payloads, queued int) {
				for _, client := range sessions {
					if client.ingress != nil {
						payloads++
					}
					if client.waiting {
						queued++
					}
					payloads += client.replies
				}
				return payloads, queued
			}
			for step, command := range input {
				client := &sessions[(command/actions)%2]
				r := client.router
				used, _ := occupancy()
				budgetError := false
				switch command % actions {
				case admit:
					if client.ingress == nil {
						request := &jsonrpc.Request{ID: id, Method: "tools/list"}
						_, ok := r.prepare(request)
						budgetError = used == slots
						require.Equal(t, !budgetError, ok, "step %d admission", step)
						if ok {
							client.ingress, client.waiting = request, true
						}
					}
				case cancel:
					r.cancelCall(cancelRequest)
					if client.waiting {
						client.waiting = false
						budgetError = used == slots
						if !budgetError {
							client.replies++
						}
					}
				case discard:
					if client.ingress != nil && !client.waiting {
						releaseMessage(client.ingress)
						client.released, client.ingress = client.ingress, nil
					}
				case consume:
					if client.replies > 0 {
						require.NotEmpty(t, r.out, "step %d lost cancellation response", step)
						response := wantResponse(t, <-r.out, id)
						var callError *jsonrpc.Error
						require.ErrorAs(t, response.Error, &callError)
						require.EqualValues(t, -32800, callError.Code)
						releaseMessage(response)
						client.replies--
						client.released = response
					}
				case releaseAgain:
					releaseMessage(client.released)
				}
				if budgetError {
					require.NotEmpty(t, r.errs, "step %d failed to report overload", step)
					require.ErrorContains(t, <-r.errs, "retained message budget")
				}
				require.Empty(t, r.errs, "step %d unexpected error", step)
				require.Len(t, r.out, client.replies, "step %d output ownership", step)
				payloads, queued := occupancy()
				state := s.snapshot()
				require.Equal(t, queued, state.Queued, "step %d operation credits", step)
				require.Equal(t, payloads*bytesPerMessage, state.RetainedMessageBytes, "step %d payload bytes", step)
			}
			for _, client := range sessions {
				client.router.clearRequests()
				if client.ingress != nil {
					releaseMessage(client.ingress)
				}
				client.router.releaseRetainedMessages()
			}
			state := s.snapshot()
			require.Zero(t, state.Queued)
			require.Zero(t, state.RetainedMessageBytes)
		})
	})
}
