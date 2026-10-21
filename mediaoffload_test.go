package meowcaller

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow/types"
)

func sampleRelayData(t *testing.T) *relayData {
	t.Helper()
	peer, err := types.ParseJID("12345678901@s.whatsapp.net")
	if err != nil {
		t.Fatalf("parse peer jid: %v", err)
	}
	return &relayData{
		relayKeyASCII: []byte("relay-key-bytes"),
		relayTokens:   [][]byte{[]byte("token-zero"), []byte("token-one")},
		peerJID:       peer,
		endpoints: []relayEndpoint{{
			relayID:     7,
			relayName:   "rly-test",
			tokenID:     1,
			authTokenID: 2,
			isFNA:       true,
			addresses:   []relayAddress{{ipv4: "203.0.113.10", port: 3478}},
		}},
	}
}

// TestRelaySetupSurvivesTheRoundTrip guards the only thing the wire format has
// to do. A field dropped here does not fail loudly: the media loop connects to
// the wrong endpoint, or fails MESSAGE-INTEGRITY, and the call is silent.
func TestRelaySetupSurvivesTheRoundTrip(t *testing.T) {
	original := sampleRelayData(t)
	back := relaySetupFrom(original).toRelayData(original.peerJID)

	if !bytes.Equal(back.relayKeyASCII, original.relayKeyASCII) {
		t.Error("relay key did not survive")
	}
	if len(back.relayTokens) != len(original.relayTokens) {
		t.Fatalf("token count = %d, want %d", len(back.relayTokens), len(original.relayTokens))
	}
	for i := range original.relayTokens {
		if !bytes.Equal(back.relayTokens[i], original.relayTokens[i]) {
			t.Errorf("token %d did not survive", i)
		}
	}
	if len(back.endpoints) != 1 {
		t.Fatalf("endpoint count = %d, want 1", len(back.endpoints))
	}
	got, want := back.endpoints[0], original.endpoints[0]
	if got.relayID != want.relayID || got.relayName != want.relayName ||
		got.tokenID != want.tokenID || got.authTokenID != want.authTokenID || got.isFNA != want.isFNA {
		t.Errorf("endpoint metadata drifted: %+v vs %+v", got, want)
	}
	if len(got.addresses) != 1 || got.addresses[0] != want.addresses[0] {
		t.Errorf("addresses drifted: %+v vs %+v", got.addresses, want.addresses)
	}
	if back.peerJID != original.peerJID {
		t.Errorf("peer jid = %s, want %s", back.peerJID, original.peerJID)
	}
}

// TestRelaySetupCopiesInsteadOfAliasing: the setup crosses a process boundary
// and the session clears its own key material after handing it over. Sharing
// the backing array would hand the media loop a zeroed key.
func TestRelaySetupCopiesInsteadOfAliasing(t *testing.T) {
	original := sampleRelayData(t)
	setup := relaySetupFrom(original)

	clear(original.relayKeyASCII)
	clear(original.relayTokens[0])

	if bytes.Equal(setup.KeyASCII, original.relayKeyASCII) {
		t.Error("the setup aliases the session's relay key")
	}
	if bytes.Equal(setup.Tokens[0], original.relayTokens[0]) {
		t.Error("the setup aliases the session's relay token")
	}
}

// TestOffloadedCallRefusesUnusableSetups. Every one of these reaches the media
// loop as a connect timeout or a STUN rejection several seconds in, which reads
// like a network fault rather than a malformed handoff.
func TestOffloadedCallRefusesUnusableSetups(t *testing.T) {
	good := MediaSetup{
		CallID:  "call-1",
		CallKey: []byte("call-key"),
		PeerJID: "12345678901@s.whatsapp.net",
		Relay:   relaySetupFrom(sampleRelayData(t)),
	}

	cases := map[string]func(*MediaSetup){
		"no call id":       func(s *MediaSetup) { s.CallID = "" },
		"no call key":      func(s *MediaSetup) { s.CallKey = nil },
		"no relay key":     func(s *MediaSetup) { s.Relay.KeyASCII = nil },
		"no endpoint":      func(s *MediaSetup) { s.Relay.Endpoints = nil },
		"endpoint no addr": func(s *MediaSetup) { s.Relay.Endpoints[0].Addresses = nil },
		"bad peer jid":     func(s *MediaSetup) { s.PeerJID = "not a jid" },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			setup := good
			setup.Relay = relaySetupFrom(sampleRelayData(t))
			breakIt(&setup)
			if _, err := NewOffloadedCall(setup); err == nil {
				t.Error("accepted a setup that cannot produce media")
			}
		})
	}

	if _, err := NewOffloadedCall(good); err != nil {
		t.Errorf("rejected a usable setup: %v", err)
	}
}

// TestMediaOffloadDivertsInsteadOfRunningLocally is the point of the whole
// seam. Without the diversion the session process would encode MLow itself,
// which is the cost the offload exists to move.
func TestMediaOffloadDivertsInsteadOfRunningLocally(t *testing.T) {
	got := make(chan MediaSetup, 1)
	c := &Client{mediaOffload: func(s MediaSetup) error {
		got <- s
		return nil
	}}
	e := newEngine(c)
	c.eng = e

	rd := sampleRelayData(t)
	call := &Call{eng: e, id: "call-1", peer: rd.peerJID}
	e.calls["call-1"] = &engineCall{
		call:      call,
		callKey:   []byte("call-key"),
		relay:     rd,
		selfLID:   "self@lid",
		peerLID:   "peer@lid",
		direction: CallDirectionIncoming,
	}

	e.maybeStartMedia("call-1")

	setup := <-got
	if setup.CallID != "call-1" {
		t.Errorf("call id = %q", setup.CallID)
	}
	if !setup.Inbound {
		t.Error("direction lost: an outbound setup allocates the wrong relay endpoint")
	}
	if setup.SelfLID != "self@lid" || setup.PeerLID != "peer@lid" {
		t.Errorf("LIDs lost: self=%q peer=%q", setup.SelfLID, setup.PeerLID)
	}
	if !bytes.Equal(setup.CallKey, []byte("call-key")) {
		t.Error("call key lost")
	}
	if setup.PeerJID != rd.peerJID.String() {
		t.Errorf("peer jid = %q", setup.PeerJID)
	}
	if len(setup.Relay.Endpoints) != 1 {
		t.Errorf("relay endpoints = %d", len(setup.Relay.Endpoints))
	}
}

// TestGroupCallsIgnoreTheOffload. Group media keys off the group epoch and a
// participant registry that only the session holds; diverting it would produce
// a worker that cannot decrypt anyone.
func TestGroupCallsIgnoreTheOffload(t *testing.T) {
	offloaded := make(chan struct{}, 1)
	cfg := resolveConfig(nil)
	c := &Client{log: cfg.log, diag: cfg.diag, mediaOffload: func(MediaSetup) error {
		offloaded <- struct{}{}
		return nil
	}}
	e := newEngine(c)
	c.eng = e
	setup := testGroupMediaSetup(t)
	update, err := groupUpdateFromMediaSetup(setup)
	if err != nil {
		t.Fatalf("groupUpdateFromMediaSetup: %v", err)
	}
	e.calls[setup.CallID] = &engineCall{
		call: &Call{eng: e, id: setup.CallID}, selfLID: setup.SelfLID,
		group: true, groupUpdate: &update, groupRawEpoch: bytes.Clone(setup.Epoch.RawKey),
		groupEpochTxID: setup.Epoch.TransactionID, hasGroupEpoch: true,
	}
	e.maybeStartMedia(setup.CallID)
	e.mu.Lock()
	startedLocally := e.calls[setup.CallID].started && !e.calls[setup.CallID].groupOffloaded
	cancel := e.calls[setup.CallID].cancel
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}

	select {
	case <-offloaded:
		t.Error("a group call was handed to the offload target")
	case <-time.After(50 * time.Millisecond):
	}
	if !startedLocally {
		t.Error("a group call without WithGroupMediaOffload did not retain the local media path")
	}
}

func testGroupMediaSetup(t *testing.T) GroupMediaSetup {
	t.Helper()
	return GroupMediaSetup{
		CallID: "group-offload", SelfLID: "100:1@lid",
		Epoch:  GroupMediaEpoch{TransactionID: 17, RawKey: bytes.Repeat([]byte{0x5a}, 32)},
		Roster: []GroupParticipantSetup{{DeviceJID: "200:2@lid", PID: 9, HasPID: true}},
		Relay: GroupRelaySetup{
			ParticipantUUID: "participant", SelfPID: 7, HasSelfPID: true,
			KeyASCII: []byte("relay-key"), HBHKey: []byte("hbh-key"),
			Tokens: [][]byte{[]byte("token")}, AuthTokens: [][]byte{[]byte("auth")},
			WarpMITagLength: 8, AttributePadding: true,
			Endpoints: []GroupRelayEndpointSetup{{RelayID: 1, RelayName: "group", TokenID: 2,
				AuthTokenID: 3, IPv4: "203.0.113.10", Port: 3478}},
		},
	}
}

func TestOffloadedGroupDerivesSameReceiveKeysAsLocalRegistry(t *testing.T) {
	setup := testGroupMediaSetup(t)
	offloaded, err := NewOffloadedGroupCall(setup)
	if err != nil {
		t.Fatalf("NewOffloadedGroupCall: %v", err)
	}
	defer offloaded.eng.calls[setup.CallID].groupReceivers.clear()

	local, err := newParticipantReceiveRegistry(setup.CallID, setup.Epoch.RawKey, setup.SelfLID, setup.SelfLID, nil)
	if err != nil {
		t.Fatalf("new local registry: %v", err)
	}
	defer local.clear()
	update, err := groupUpdateFromMediaSetup(setup)
	if err != nil {
		t.Fatalf("groupUpdateFromMediaSetup: %v", err)
	}
	if err = local.ApplyGroupUpdate(update); err != nil {
		t.Fatalf("local ApplyGroupUpdate: %v", err)
	}
	if err = local.ApplyGroupRawEpoch(setup.Epoch.TransactionID, setup.Epoch.RawKey); err != nil {
		t.Fatalf("local ApplyGroupRawEpoch: %v", err)
	}

	got := offloaded.eng.calls[setup.CallID].groupReceivers.byPID[9]
	want := local.byPID[9]
	if got == nil || want == nil {
		t.Fatal("remote participant was not installed in both registries")
	}
	if got.participantID != want.participantID || got.ssrc != want.ssrc {
		t.Fatalf("derived participant identity differs: got %q/%08x, want %q/%08x",
			got.participantID, got.ssrc, want.participantID, want.ssrc)
	}
	if got.pipe.recvKeys != want.pipe.recvKeys {
		t.Fatal("offloaded receive SRTP keys differ from the in-pod derivation")
	}
}

func TestOffloadedGroupBuffersEpochUntilMatchingRoster(t *testing.T) {
	setup := testGroupMediaSetup(t)
	offloaded, err := NewOffloadedGroupCall(setup)
	if err != nil {
		t.Fatalf("NewOffloadedGroupCall: %v", err)
	}
	defer offloaded.eng.calls[setup.CallID].groupReceivers.clear()

	lateEpoch := bytes.Repeat([]byte{0xa5}, 32)
	if err = offloaded.InstallGroupEpoch(18, lateEpoch); err != nil {
		t.Fatalf("InstallGroupEpoch before roster: %v", err)
	}
	if _, ok := offloaded.eng.calls[setup.CallID].groupReceivers.pendingEpochs[18]; !ok {
		t.Fatal("out-of-order epoch was not parked")
	}
	update := GroupMediaUpdate{
		TransactionID: 18, Roster: setup.Roster, Relay: setup.Relay,
	}
	if err = offloaded.ApplyGroupUpdate(update); err != nil {
		t.Fatalf("ApplyGroupUpdate: %v", err)
	}
	registry := offloaded.eng.calls[setup.CallID].groupReceivers
	if !registry.hasEpoch || registry.installedEpoch.transactionID != 18 {
		t.Fatalf("installed epoch tx = %d, want 18", registry.installedEpoch.transactionID)
	}
	if _, ok := registry.pendingEpochs[18]; ok {
		t.Fatal("matching roster did not consume the pending epoch")
	}
}

func TestGroupOffloadHookForwardsAuthoritativeChanges(t *testing.T) {
	setup := testGroupMediaSetup(t)
	initial, err := groupUpdateFromMediaSetup(setup)
	if err != nil {
		t.Fatalf("groupUpdateFromMediaSetup: %v", err)
	}
	setupReady := make(chan GroupMediaSetup, 1)
	continueOffload := make(chan struct{})
	started := make(chan struct{})
	var gotUpdate GroupMediaUpdate
	var gotEpoch uint32
	var gotRaw []byte
	c := &Client{log: zerolog.Nop(), mediaGroupOffload: func(got GroupMediaSetup) (GroupMediaCallbacks, error) {
		setupReady <- got
		<-continueOffload
		return GroupMediaCallbacks{
			OnGroupUpdate: func(update GroupMediaUpdate) error { gotUpdate = update; return nil },
			OnGroupEpoch: func(txID uint32, raw []byte) error {
				gotEpoch, gotRaw = txID, bytes.Clone(raw)
				return nil
			},
		}, nil
	}}
	e := newEngine(c)
	c.eng = e
	e.calls[setup.CallID] = &engineCall{
		call: &Call{eng: e, id: setup.CallID}, selfLID: setup.SelfLID,
		direction: CallDirectionIncoming,
		group:     true, groupUpdate: &initial, groupRawEpoch: bytes.Clone(setup.Epoch.RawKey),
		groupEpochTxID: setup.Epoch.TransactionID, hasGroupEpoch: true,
	}
	go func() {
		e.maybeStartMedia(setup.CallID)
		close(started)
	}()
	gotSetup := <-setupReady
	if gotSetup.CallID != setup.CallID || gotSetup.SelfPID != setup.Relay.SelfPID || len(gotSetup.Roster) != 1 || !gotSetup.Inbound {
		t.Fatal("offload setup lost call identity, inbound direction, self PID, or participant roster")
	}
	if gotSetup.Epoch.TransactionID != setup.Epoch.TransactionID || !bytes.Equal(gotSetup.Epoch.RawKey, setup.Epoch.RawKey) {
		t.Fatal("offload setup paired the initial epoch key with a different transaction")
	}

	next := initial
	next.TransactionID = 18
	next.RekeyRequested = true
	if !e.applyGroupUpdate(next) {
		t.Fatal("authoritative roster update was not installed")
	}
	nextEpoch := bytes.Repeat([]byte{0xa5}, 32)
	if err = e.installGroupEpoch(setup.CallID, 18, nextEpoch); err != nil {
		t.Fatalf("forward epoch: %v", err)
	}
	close(continueOffload)
	<-started
	if gotUpdate.TransactionID != 18 || !gotUpdate.RekeySeen || len(gotUpdate.Roster) != 1 {
		t.Fatal("offload callback received incomplete roster transaction metadata")
	}
	if gotEpoch != 18 || !bytes.Equal(gotRaw, nextEpoch) {
		t.Fatalf("offload callback epoch = %d/%d bytes, want 18/32", gotEpoch, len(gotRaw))
	}
}

func TestGroupOffloadSetupSnapshotsEpochAndTransactionTogether(t *testing.T) {
	const iterations = 16
	for range iterations {
		setup := testGroupMediaSetup(t)
		initial, err := groupUpdateFromMediaSetup(setup)
		if err != nil {
			t.Fatalf("groupUpdateFromMediaSetup: %v", err)
		}
		setupReady := make(chan GroupMediaSetup, 1)
		c := &Client{log: zerolog.Nop(), mediaGroupOffload: func(got GroupMediaSetup) (GroupMediaCallbacks, error) {
			setupReady <- got
			return GroupMediaCallbacks{
				OnGroupUpdate: func(GroupMediaUpdate) error { return nil },
				OnGroupEpoch:  func(uint32, []byte) error { return nil },
			}, nil
		}}
		e := newEngine(c)
		c.eng = e
		e.calls[setup.CallID] = &engineCall{
			call: &Call{eng: e, id: setup.CallID}, selfLID: setup.SelfLID,
			group: true, groupUpdate: &initial, groupRawEpoch: bytes.Clone(setup.Epoch.RawKey),
			groupEpochTxID: setup.Epoch.TransactionID, hasGroupEpoch: true,
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for txID := uint32(18); txID < 82; txID++ {
				raw := bytes.Repeat([]byte{byte(txID)}, 32)
				if updateErr := e.installGroupEpoch(setup.CallID, txID, raw); updateErr != nil {
					t.Errorf("install epoch %d: %v", txID, updateErr)
					return
				}
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			e.maybeStartMedia(setup.CallID)
		}()
		close(start)
		got := <-setupReady
		wg.Wait()
		wantByte := byte(got.Epoch.TransactionID)
		if got.Epoch.TransactionID == setup.Epoch.TransactionID {
			wantByte = setup.Epoch.RawKey[0]
		}
		if len(got.Epoch.RawKey) != 32 || !bytes.Equal(got.Epoch.RawKey, bytes.Repeat([]byte{wantByte}, 32)) {
			t.Fatalf("setup paired epoch transaction %d with a different key", got.Epoch.TransactionID)
		}
	}
}

func TestOffloadedGroupRunSnapshotsEpochDuringConcurrentRotation(t *testing.T) {
	setup := testGroupMediaSetup(t)
	offloaded, err := NewOffloadedGroupCall(setup)
	if err != nil {
		t.Fatalf("NewOffloadedGroupCall: %v", err)
	}
	defer offloaded.eng.calls[setup.CallID].groupReceivers.clear()

	const iterations = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		for range iterations {
			_ = offloaded.Run(ctx)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for txID := uint32(18); txID < 18+iterations; txID++ {
			raw := bytes.Repeat([]byte{byte(txID)}, 32)
			if installErr := offloaded.InstallGroupEpoch(txID, raw); installErr != nil {
				t.Errorf("InstallGroupEpoch(%d): %v", txID, installErr)
				return
			}
		}
	}()
	close(start)
	wg.Wait()
}

// TestOffloadedVideoCallStartsWithTheSenderActive guards the one thing the
// media process cannot learn on its own: that the call was negotiated with
// video. Without the flag the video sender is born inactive and SendVideo
// drops every access unit silently — no error, no RTP, a peer that never
// asks for a keyframe. And the toggle must stay local: an offloaded engine has
// no signaling, so SetVideoEnabled cannot fail-and-roll-back as if the peer
// had refused.
func TestOffloadedVideoCallStartsWithTheSenderActive(t *testing.T) {
	if got := mediaSetupFrom("call-v", []byte("k"), "self", "peer", sampleRelayData(t), true, true); !got.Video {
		t.Fatal("mediaSetupFrom dropped the video flag; the worker would never send video")
	}
	setup := MediaSetup{
		CallID:  "call-v",
		CallKey: []byte("call-key"),
		PeerJID: "12345678901@s.whatsapp.net",
		Inbound: true,
		Video:   true,
		Relay:   relaySetupFrom(sampleRelayData(t)),
	}
	oc, err := NewOffloadedCall(setup)
	if err != nil {
		t.Fatalf("usable video setup rejected: %v", err)
	}
	if !oc.Call().IsVideo() {
		t.Fatal("offloaded video call does not report IsVideo")
	}
	oc.eng.mu.Lock()
	m := oc.eng.calls["call-v"]
	oc.eng.mu.Unlock()
	if !m.localVideo || !m.remoteVideo {
		t.Fatalf("video call must start with both directions enabled: local=%v remote=%v", m.localVideo, m.remoteVideo)
	}

	// The media loop copies localVideo into the sender when it attaches; here
	// the sender is attached by hand to watch the toggle reach it.
	vs := &videoSender{}
	oc.eng.mu.Lock()
	m.videoTx = vs
	oc.eng.mu.Unlock()

	if err := oc.Call().SetVideoEnabled(false); err != nil {
		t.Fatalf("local video toggle failed on an engine without signaling: %v", err)
	}
	if m.localVideo || vs.active {
		t.Error("SetVideoEnabled(false) did not disable the sender locally")
	}
	if err := oc.Call().SetVideoEnabled(true); err != nil {
		t.Fatalf("local video toggle failed on an engine without signaling: %v", err)
	}
	if !m.localVideo || !vs.active || vs.sendGated {
		t.Errorf("SetVideoEnabled(true) rolled back or gated the sender: local=%v active=%v gated=%v", m.localVideo, vs.active, vs.sendGated)
	}

	audioOnly := setup
	audioOnly.CallID = "call-a"
	audioOnly.Video = false
	oc2, err := NewOffloadedCall(audioOnly)
	if err != nil {
		t.Fatalf("usable audio setup rejected: %v", err)
	}
	if oc2.Call().IsVideo() {
		t.Error("audio-only offloaded call reports video")
	}
}
