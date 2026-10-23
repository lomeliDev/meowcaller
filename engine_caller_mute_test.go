package meowcaller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/purpshell/meowcaller/signaling"
	"github.com/rs/zerolog"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func engineWithIncomingCall(callID string) (*engine, *Call) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	client.eng = eng
	call := &Call{eng: eng, id: callID, peer: peerJID(), phase: CallPhaseRinging}
	eng.calls[callID] = &engineCall{
		call: call, direction: CallDirectionIncoming,
		from: peerJID(), creator: creatorJID(),
	}
	return eng, call
}

func muteV2Received(node waBinary.Node, from types.JID) *waBinary.Node {
	node.Attrs["from"] = from
	return &node
}

// A caller implemented by this library must emit the first mute_v2 after its
// relay arrives. The callee's accept is intentionally deferred until that
// control, so two library endpoints otherwise both wait forever.
func TestCallerRelayEmitsInitialMuteV2(t *testing.T) {
	caller, call := testEngineWithOutgoingCall()
	caller.c.log = zerolog.Nop()
	caller.calls[call.ID()].started = true // exercise signaling, never real media
	var sent []waBinary.Node
	caller.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}

	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})

	if len(sent) != 1 {
		t.Fatalf("caller sent %d controls, want one initial mute_v2", len(sent))
	}
	if got := sent[0].GetChildren(); len(got) != 1 || got[0].Tag != "mute_v2" {
		t.Fatalf("caller control = %#v, want mute_v2", sent[0])
	}
	attrs := sent[0].GetChildren()[0].AttrGetter()
	if attrs.String("call-id") != call.ID() || attrs.JID("call-creator") != creatorJID() || attrs.String("mute-state") != "0" {
		t.Fatalf("initial mute identity = %#v", attrs)
	}
	if to := sent[0].AttrGetter().JID("to"); to != peerJID() {
		t.Fatalf("initial mute target = %s, want %s", to, peerJID())
	}
	if id := sent[0].AttrGetter().String("id"); id == "" {
		t.Fatal("initial mute has no stanza id")
	}
}

func TestInitialMuteRetriesAfterSendFailureWithoutDuplicatingSuccess(t *testing.T) {
	caller, call := testEngineWithOutgoingCall()
	caller.c.log = zerolog.Nop()
	caller.calls[call.ID()].started = true
	attempts := 0
	caller.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient send failure")
		}
		if child := node.GetChildren(); len(child) != 1 || child[0].Tag != "mute_v2" {
			t.Fatalf("retry sent %#v, want mute_v2", node)
		}
		return nil
	}

	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})
	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})
	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})

	if attempts != 2 {
		t.Fatalf("mute attempts = %d, want failed attempt plus one successful retry", attempts)
	}
}

func TestInitialMuteSkipsIncomingAndGroupCalls(t *testing.T) {
	for _, tc := range []struct {
		name      string
		direction CallDirection
		group     bool
	}{
		{"incoming", CallDirectionIncoming, false},
		{"group", CallDirectionOutgoing, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, call := testEngineWithOutgoingCall()
			eng.c.log = zerolog.Nop()
			m := eng.calls[call.ID()]
			m.direction, m.group, m.started = tc.direction, tc.group, true
			sent := 0
			eng.sendCallNode = func(context.Context, waBinary.Node) error {
				sent++
				return nil
			}
			eng.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})
			if sent != 0 {
				t.Fatalf("sent %d initial mutes for %s call", sent, tc.name)
			}
		})
	}
}

func TestInitialMuteDoesNotHoldEngineLockWhileSending(t *testing.T) {
	caller, call := testEngineWithOutgoingCall()
	caller.c.log = zerolog.Nop()
	caller.calls[call.ID()].started = true
	caller.sendCallNode = func(context.Context, waBinary.Node) error {
		done := make(chan struct{})
		go func() {
			caller.mu.Lock()
			caller.mu.Unlock()
			close(done)
		}()
		select {
		case <-done:
			return nil
		case <-time.After(time.Second):
			t.Fatal("initial mute attempted signaling while holding engine lock")
			return nil
		}
	}
	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})
}

func TestDeferredAcceptIsOneShotWhenMuteAndAnswerRace(t *testing.T) {
	const rounds = 64
	for i := 0; i < rounds; i++ {
		callee, incoming := engineWithIncomingCall("CID")
		var mu sync.Mutex
		accepts := 0
		callee.sendCallNode = func(context.Context, waBinary.Node) error {
			mu.Lock()
			accepts++
			mu.Unlock()
			return nil
		}
		mute := signaling.BuildMuteV2("CID", peerJID(), creatorJID(), "0")
		mute.Attrs["from"] = peerJID()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			callee.onCallRaw(&mute)
		}()
		go func() {
			defer wg.Done()
			if err := incoming.Answer(); err != nil {
				t.Errorf("answer: %v", err)
			}
		}()
		wg.Wait()
		mu.Lock()
		got := accepts
		mu.Unlock()
		if got != 1 {
			t.Fatalf("round %d accepted %d times, want one", i, got)
		}
	}
}

func TestDeferredAcceptHandlesMuteBeforeOrAfterAnswer(t *testing.T) {
	for _, answerFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "mute-antes-de-answer", true: "answer-antes-de-mute"}[answerFirst], func(t *testing.T) {
			caller, outgoing := testEngineWithOutgoingCall()
			caller.c.log = zerolog.Nop()
			caller.calls[outgoing.ID()].started = true
			var mute waBinary.Node
			caller.sendCallNode = func(_ context.Context, node waBinary.Node) error {
				mute = node
				return nil
			}

			callee, incoming := engineWithIncomingCall(outgoing.ID())
			var accepts []waBinary.Node
			callee.sendCallNode = func(_ context.Context, node waBinary.Node) error {
				accepts = append(accepts, node)
				return nil
			}

			caller.onRelay(outgoing.ID(), &waBinary.Node{Tag: "relay"})
			if len(mute.GetChildren()) != 1 {
				t.Fatal("caller did not produce mute_v2")
			}
			if answerFirst {
				if err := incoming.Answer(); err != nil {
					t.Fatalf("answer: %v", err)
				}
				callee.onCallRaw(muteV2Received(mute, peerJID()))
			} else {
				callee.onCallRaw(muteV2Received(mute, peerJID()))
				if err := incoming.Answer(); err != nil {
					t.Fatalf("answer: %v", err)
				}
			}
			callee.onCallRaw(muteV2Received(mute, peerJID()))

			if len(accepts) != 1 {
				t.Fatalf("accepts = %d, want exactly one", len(accepts))
			}
			if got := accepts[0].GetChildren(); len(got) != 1 || got[0].Tag != "accept" {
				t.Fatalf("callee control = %#v, want accept", accepts[0])
			}
		})
	}
}
