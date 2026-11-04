package meowcaller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
)

// TestCallRejectVideoDeclinesPendingPeerUpgrade pins the reject stanza and the one
// local state change it makes: the peer's pending upgrade is consumed and nothing
// else moves — a bare state=5, no decoder advertisement, no orientation.
func TestCallRejectVideoDeclinesPendingPeerUpgrade(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.videoGate = false
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{}
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}

	if err := call.RejectVideo(); err != nil {
		t.Fatalf("RejectVideo: %v", err)
	}
	if len(sent) != 1 {
		t.Fatalf("RejectVideo sent %d stanzas, want 1", len(sent))
	}
	video := sent[0].GetChildren()[0]
	attrs := video.AttrGetter()
	if got := attrs.Int("state"); got != signaling.VideoStateUpgradeReject {
		t.Fatalf("reject state = %d, want %d", got, signaling.VideoStateUpgradeReject)
	}
	if got := attrs.String("call-id"); got != call.ID() {
		t.Fatalf("reject call-id = %q, want %q", got, call.ID())
	}
	if got := attrs.JID("call-creator"); got != creatorJID() {
		t.Fatalf("reject call-creator = %s, want %s", got, creatorJID())
	}
	if _, ok := video.Attrs["dec"]; ok {
		t.Fatal("reject stanza unexpectedly advertises a decoder")
	}
	if _, ok := video.Attrs["device_orientation"]; ok {
		t.Fatal("reject stanza unexpectedly carries device_orientation")
	}
	if m.peerVideoUpgrade {
		t.Fatal("reject left the peer upgrade pending")
	}
	if active, gated := senderVideoState(m.videoTx); active || gated {
		t.Fatalf("reject touched the local sender: active:%v gated:%v, want false,false", active, gated)
	}
	if call.IsSendingVideo() {
		t.Fatal("rejecting the peer's upgrade turned the local camera on")
	}
}

// TestCallRejectVideoKeepsOwnPendingUpgrade: declining the peer's camera is not
// cancelling this client's own upgrade request, which stays pending and gated.
func TestCallRejectVideoKeepsOwnPendingUpgrade(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	m.videoGate = true
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{active: true, sendGated: true}
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return nil }

	if err := call.RejectVideo(); err != nil {
		t.Fatalf("RejectVideo: %v", err)
	}
	if !m.localVideo || !m.videoGate {
		t.Fatalf("own upgrade state = local:%v gate:%v, want true,true", m.localVideo, m.videoGate)
	}
	if m.peerVideoUpgrade {
		t.Fatal("reject left the peer upgrade pending")
	}
	if active, gated := senderVideoState(m.videoTx); !active || !gated {
		t.Fatalf("own sender state = active:%v gated:%v, want true,true", active, gated)
	}
}

// TestCallRejectVideoWithoutPendingUpgradeFails: nothing to reject sends nothing.
func TestCallRejectVideoWithoutPendingUpgradeFails(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.videoTx = &videoSender{}
	var sent int
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		sent++
		return nil
	}

	err := call.RejectVideo()
	if err == nil || !strings.Contains(err.Error(), "no pending peer video upgrade") {
		t.Fatalf("RejectVideo without pending upgrade error = %v, want one naming the missing request", err)
	}
	if sent != 0 {
		t.Fatalf("reject without pending upgrade sent %d stanzas, want 0", sent)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("failed reject touched the local sender")
	}
}

// TestCallRejectVideoRestoresPendingUpgradeWhenSignalingFails: the accept rollback
// applies to the reject too — a failed transmit leaves the request pending so the
// caller can retry instead of silently dropping the peer's request.
func TestCallRejectVideoRestoresPendingUpgradeWhenSignalingFails(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	m.videoGate = true
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{active: true, sendGated: true}
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		return errors.New("network unavailable")
	}

	err := call.RejectVideo()
	if err == nil {
		t.Fatal("RejectVideo returned nil after signaling failure")
	}
	if !m.peerVideoUpgrade {
		t.Fatal("failed reject consumed the pending peer upgrade")
	}
	if !m.localVideo || !m.videoGate {
		t.Fatalf("failed reject touched the local upgrade: local:%v gate:%v, want true,true", m.localVideo, m.videoGate)
	}
	if active, gated := senderVideoState(m.videoTx); !active || !gated {
		t.Fatalf("failed reject touched the local sender: active:%v gated:%v, want true,true", active, gated)
	}
}

// TestInboundVideoRejectUnwindsPendingLocalUpgrade pins the receiving side of the
// same stanza — what the rejecting peer's client does to its own request — so the
// outbound reject has a known effect end to end.
func TestInboundVideoRejectUnwindsPendingLocalUpgrade(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	m.videoGate = true
	m.remoteVideo = false
	m.videoTx = &videoSender{active: true, sendGated: true}

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeReject))

	if m.localVideo || m.videoGate {
		t.Fatalf("rejected upgrade state = local:%v gate:%v, want false,false", m.localVideo, m.videoGate)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("peer reject left the local sender active")
	}
	if call.IsVideo() {
		t.Fatal("a rejected upgrade still reports video")
	}
}
