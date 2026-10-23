package meowcaller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	waBinary "go.mau.fi/whatsmeow/binary"
)

func preparedOutgoingGroup(t *testing.T, callID string) (*engine, *Call) {
	t.Helper()
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	client.eng = eng
	call := &Call{eng: eng, id: callID, phase: CallPhaseCalling}
	eng.calls[callID] = &engineCall{call: call, group: true, direction: CallDirectionOutgoing}
	return eng, call
}

func TestPrepareOutgoingGroupPublishesIDBeforeOffer(t *testing.T) {
	eng, call := preparedOutgoingGroup(t, "outgoing-group")
	called := false
	sent := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		sent++
		if !called {
			t.Fatal("la oferta salió antes de preparar la correlación")
		}
		return nil
	}
	err := eng.publishOutgoingGroupCall(context.Background(), waBinary.Node{}, call.ID(), call, func(callID string) error {
		called = true
		entry := eng.lookup(callID)
		if entry == nil || entry.call != call || !entry.group {
			t.Fatalf("preparación no vio la llamada de grupo registrada: %#v", entry)
		}
		return nil
	})
	if err != nil || !called || sent != 1 {
		t.Fatalf("publicación = (%v, preparada=%v, señales=%d)", err, called, sent)
	}
}

func TestPrepareOutgoingGroupErrorClosesBeforeSignal(t *testing.T) {
	eng, call := preparedOutgoingGroup(t, "prepare-error")
	want := errors.New("sin worker")
	var ended string
	sent := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sent++; return nil }
	call.OnEnd(func(reason string) { ended = reason })
	err := eng.publishOutgoingGroupCall(context.Background(), waBinary.Node{}, call.ID(), call, func(string) error { return want })
	if !errors.Is(err, want) {
		t.Fatalf("preparación devolvió %v; se esperaba %v", err, want)
	}
	if eng.lookup(call.ID()) != nil || call.State() != CallPhaseEnded || ended == "" || sent != 0 {
		t.Fatalf("error dejó llamada publicable: entry=%#v state=%v ended=%q señales=%d", eng.lookup(call.ID()), call.State(), ended, sent)
	}
}

func TestPrepareOutgoingGroupCloseDuringCallbackPreventsPublication(t *testing.T) {
	eng, call := preparedOutgoingGroup(t, "prepare-closed")
	sent := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sent++; return nil }
	err := eng.publishOutgoingGroupCall(context.Background(), waBinary.Node{}, call.ID(), call, func(string) error {
		eng.finishCall(call.ID(), "terminated during group preparation")
		return nil
	})
	if err == nil {
		t.Fatal("preparación aceptó una llamada cerrada durante el callback")
	}
	if eng.lookup(call.ID()) != nil || call.State() != CallPhaseEnded || sent != 0 {
		t.Fatalf("cierre durante preparación dejó llamada: entry=%#v state=%v señales=%d", eng.lookup(call.ID()), call.State(), sent)
	}
}

func TestPrepareOutgoingGroupConcurrentCallsStayIsolated(t *testing.T) {
	const calls = 32
	eng, _ := preparedOutgoingGroup(t, "seed")
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := 0; i < calls; i++ {
		callID := fmt.Sprintf("concurrent-group-%d", i)
		call := &Call{eng: eng, id: callID, phase: CallPhaseCalling}
		eng.mu.Lock()
		eng.calls[callID] = &engineCall{call: call, group: true, direction: CallDirectionOutgoing}
		eng.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- eng.prepareOutgoingGroupCall(call.ID(), call, func(got string) error {
				entry := eng.lookup(got)
				if got != call.ID() || entry == nil || entry.call != call {
					return fmt.Errorf("preparación cruzada para %q", got)
				}
				return nil
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
