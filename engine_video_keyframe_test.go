package meowcaller

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/purpshell/meowcaller/rtp"
	"github.com/purpshell/meowcaller/srtp"
	"github.com/rs/zerolog"
)

// capturingFeedbackChannel stands in for the relay transport so the requester can be
// exercised without a live call.
type capturingFeedbackChannel struct {
	mu      sync.Mutex
	packets [][]byte
}

func (c *capturingFeedbackChannel) Send(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.packets = append(c.packets, append([]byte(nil), data...))
	return len(data), nil
}

func (c *capturingFeedbackChannel) take() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.packets
	c.packets = nil
	return out
}

func TestRequestVideoKeyframeSendsPLI(t *testing.T) {
	const (
		selfLID  = "111111111111111:0@lid"
		selfSSRC = uint32(0x11223344)
		peerSSRC = uint32(0x55667788)
	)
	callKey := iota32()
	sender, err := newMediaSrtcpSender(callKey, selfLID, selfSSRC, true)
	if err != nil {
		t.Fatalf("srtcp sender: %v", err)
	}
	ch := &capturingFeedbackChannel{}
	requester := newVideoKeyframeRequester(sender, ch, zerolog.Nop())

	// A request that beats the peer's first video packet has no media SSRC to name, so
	// it is armed instead of dropped: that is the on-answer case.
	if err := requester.request(); err != nil {
		t.Fatalf("request before any peer video: %v", err)
	}
	if got := ch.take(); len(got) != 0 {
		t.Fatalf("sent %d packets before the peer's video SSRC was known", len(got))
	}

	requester.observePeerVideo(peerSSRC)
	sent := ch.take()
	if len(sent) != 1 {
		t.Fatalf("the armed request produced %d packets, want 1", len(sent))
	}

	keys, err := srtp.DeriveE2eSrtcpKeys(callKey, rtp.FormatE2ESrtpParticipantID(selfLID))
	if err != nil {
		t.Fatalf("derive keys: %v", err)
	}
	plain, _, ok := srtp.UnprotectSrtcp(&keys, selfSSRC, sent[0])
	if !ok {
		t.Fatal("the PLI did not authenticate: it was not protected with the video SRTCP sender")
	}
	want := rtp.BuildPictureLossIndication(selfSSRC, peerSSRC, true)
	if !bytes.Equal(plain, want[:]) {
		t.Fatalf("PLI payload = %x, want %x", plain, want)
	}
	if !rtp.RtcpRequestsKeyframe(plain, peerSSRC) {
		t.Fatal("the packet does not read back as keyframe feedback for the peer's stream")
	}

	// The on-demand path shares the loss path's throttle, so a burst of requests does
	// not turn into a burst of PLIs. The throttle is exercised on a clock of its own, so
	// the entry the armed request just left behind is cleared first.
	requester.forgetPeerVideo(peerSSRC)
	now := time.Unix(1000, 0)
	if err := requester.sendPLI(peerSSRC, now); err != nil {
		t.Fatalf("sendPLI: %v", err)
	}
	if got := ch.take(); len(got) != 1 {
		t.Fatalf("first PLI after the interval produced %d packets, want 1", len(got))
	}
	if err := requester.sendPLI(peerSSRC, now.Add(videoPLIInterval-time.Millisecond)); err != nil {
		t.Fatalf("throttled sendPLI reported an error: %v", err)
	}
	if got := ch.take(); len(got) != 0 {
		t.Fatalf("a throttled request still sent %d packets", len(got))
	}
	if err := requester.sendPLI(peerSSRC, now.Add(videoPLIInterval)); err != nil {
		t.Fatalf("sendPLI at the interval boundary: %v", err)
	}
	if got := ch.take(); len(got) != 1 {
		t.Fatalf("the request at the interval boundary produced %d packets, want 1", len(got))
	}

	// A participant leaving frees its throttle entry, so its SSRC is not silently
	// muted for whoever reuses it.
	requester.forgetPeerVideo(peerSSRC)
	if err := requester.sendPLI(peerSSRC, now.Add(videoPLIInterval+time.Millisecond)); err != nil {
		t.Fatalf("sendPLI after forgetting the participant: %v", err)
	}
	if got := ch.take(); len(got) != 1 {
		t.Fatalf("after forgetting the participant the request produced %d packets, want 1", len(got))
	}
}

func TestRequestVideoKeyframeWithoutVideoReceiverFails(t *testing.T) {
	e := &engine{calls: map[string]*engineCall{}}

	err := e.requestVideoKeyframe("NOSUCHCALL")
	if err == nil || !strings.Contains(err.Error(), "no active video receiver") {
		t.Fatalf("unknown call error = %v, want one naming the missing video receiver", err)
	}

	e.calls["CALL"] = &engineCall{}
	err = e.requestVideoKeyframe("CALL")
	if err == nil || !strings.Contains(err.Error(), "no active video receiver") {
		t.Fatalf("call without a media loop error = %v, want one naming the missing video receiver", err)
	}

	sender, senderErr := newMediaSrtcpSender(iota32(), "111111111111111:0@lid", 0x11223344, true)
	if senderErr != nil {
		t.Fatalf("srtcp sender: %v", senderErr)
	}
	requester := newVideoKeyframeRequester(sender, &capturingFeedbackChannel{}, zerolog.Nop())
	requester.close()
	e.calls["CALL"].videoRx = requester
	err = e.requestVideoKeyframe("CALL")
	if err == nil || !strings.Contains(err.Error(), "no active video media") {
		t.Fatalf("closed media loop error = %v, want one naming the dead media", err)
	}
}
