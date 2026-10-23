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
	"go.mau.fi/whatsmeow/types/events"
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

func preacceptDevice(callID string) *events.CallPreAccept {
	return &events.CallPreAccept{BasicCallMeta: types.BasicCallMeta{
		CallID: callID,
		From:   types.NewADJID("123456789", 0, 1),
	}}
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

	preaccept := preacceptDevice(call.ID())
	caller.onPreAccept(preaccept)
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
	if to := sent[0].AttrGetter().JID("to"); to != preaccept.From {
		t.Fatalf("initial mute target = %s, want device %s", to, preaccept.From)
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

	caller.onPreAccept(preacceptDevice(call.ID()))
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
			eng.onPreAccept(preacceptDevice(call.ID()))
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
	caller.onPreAccept(preacceptDevice(call.ID()))
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

			preaccept := preacceptDevice(outgoing.ID())
			caller.onPreAccept(preaccept)
			caller.onRelay(outgoing.ID(), &waBinary.Node{Tag: "relay"})
			if len(mute.GetChildren()) != 1 {
				t.Fatal("caller did not produce mute_v2")
			}
			mute.GetChildren()[0].Attrs["call-creator"] = types.NewJID("owner-from-mute", types.HiddenUserServer)
			if answerFirst {
				if err := incoming.Answer(); err != nil {
					t.Fatalf("answer: %v", err)
				}
				callee.onCallRaw(muteV2Received(mute, preaccept.From))
			} else {
				callee.onCallRaw(muteV2Received(mute, preaccept.From))
				if err := incoming.Answer(); err != nil {
					t.Fatalf("answer: %v", err)
				}
			}
			callee.onCallRaw(muteV2Received(mute, preaccept.From))

			if len(accepts) != 1 {
				t.Fatalf("accepts = %d, want exactly one", len(accepts))
			}
			if got := accepts[0].GetChildren(); len(got) != 1 || got[0].Tag != "accept" {
				t.Fatalf("callee control = %#v, want accept", accepts[0])
			}
			if got := accepts[0].AttrGetter().JID("to"); got != preaccept.From {
				t.Fatalf("accept target = %s, want mute device %s", got, preaccept.From)
			}
			if got := accepts[0].GetChildren()[0].AttrGetter().JID("call-creator"); got.User != "owner-from-mute" {
				t.Fatalf("accept owner = %s, want owner from mute", got)
			}
		})
	}
}

// Run 3 exposed this ordering: relay arrives before the preaccept from a
// concrete remote device. The mute must wait for that device; the LID bare in
// the original offer is not a safe target for the direct control.
func TestInitialMuteWaitsForRelayAndPreacceptDevice(t *testing.T) {
	caller, call := testEngineWithOutgoingCall()
	caller.c.log = zerolog.Nop()
	caller.calls[call.ID()].started = true
	var sent []waBinary.Node
	caller.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}
	device := types.NewADJID("123456789", 0, 1)

	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})
	if len(sent) != 0 {
		t.Fatalf("sent %d mute controls before a peer device preaccepted", len(sent))
	}
	caller.onPreAccept(&events.CallPreAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: device}})
	if len(sent) != 1 {
		t.Fatalf("sent %d mute controls after relay plus preaccept, want one", len(sent))
	}
	if got := sent[0].AttrGetter().JID("to"); got != device {
		t.Fatalf("initial mute target = %s, want preaccept device %s", got, device)
	}
}

// Whatsmeow prints a primary device (Device=0) as a bare JID. It remains a
// concrete preaccept sender, unlike the bare target stored when the offer was
// placed, so it must also unlock the initial mute.
func TestInitialMuteTargetsPrimaryPreacceptWithDeviceZero(t *testing.T) {
	caller, call := testEngineWithOutgoingCall()
	caller.c.log = zerolog.Nop()
	caller.calls[call.ID()].started = true
	var sent []waBinary.Node
	caller.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}
	primary := types.NewJID("987654321", types.HiddenUserServer)

	caller.onRelay(call.ID(), &waBinary.Node{Tag: "relay"})
	if len(sent) != 0 {
		t.Fatalf("sent %d mute controls before a primary preaccept", len(sent))
	}
	caller.onPreAccept(&events.CallPreAccept{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: primary}})
	if len(sent) != 1 {
		t.Fatalf("sent %d mute controls after primary preaccept, want one", len(sent))
	}
	if got := sent[0].AttrGetter().JID("to"); got != primary {
		t.Fatalf("initial mute target = %s, want primary preaccept %s", got, primary)
	}
}
