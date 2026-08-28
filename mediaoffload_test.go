package meowcaller

import (
	"bytes"
	"testing"

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
	offloaded := false
	c := &Client{mediaOffload: func(MediaSetup) error {
		offloaded = true
		return nil
	}}
	e := newEngine(c)
	c.eng = e

	rd := sampleRelayData(t)
	e.calls["group-1"] = &engineCall{
		call:      &Call{eng: e, id: "group-1", peer: rd.peerJID},
		callKey:   []byte("call-key"),
		relay:     rd,
		group:     true,
		direction: CallDirectionIncoming,
	}

	e.maybeStartMedia("group-1")

	if offloaded {
		t.Error("a group call was handed to the offload target")
	}
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
