package meowcaller

import (
	"bytes"
	"errors"
	"testing"

	"github.com/purpshell/meowcaller/rtp"
	"github.com/purpshell/meowcaller/stun"
	"go.mau.fi/whatsmeow/types"
)

func TestGroupRelayDataSelectsCaptureAddressAndCredentials(t *testing.T) {
	update := groupCallUpdate{Relay: &groupCallRelay{
		Key:    bytes.Repeat([]byte{0x21}, 16),
		Tokens: [][]byte{bytes.Repeat([]byte{0x42}, 174)},
		Endpoints: []groupCallRelayEndpoint{{
			RelayID: 7, RelayName: "zrh1c01", TokenID: 0,
			IPv4: "157.240.17.62", Port: 3478,
		}},
	}}
	rd, err := groupRelayData(update, false)
	if err != nil {
		t.Fatalf("groupRelayData: %v", err)
	}
	endpoint := getMediaRelayEndpoint(rd, false)
	if endpoint == nil || endpoint.relayName != "zrh1c01" ||
		endpoint.addresses[0].ipv4 != "157.240.17.62" ||
		endpoint.addresses[0].port != 3478 {
		t.Fatalf("selected endpoint = %+v", endpoint)
	}
	if !bytes.Equal(rd.relayKeyASCII, update.Relay.Key) ||
		!bytes.Equal(rd.relayTokens[0], update.Relay.Tokens[0]) {
		t.Fatal("group relay credentials were not copied")
	}
	rd.relayKeyASCII[0] ^= 0xff
	if bytes.Equal(rd.relayKeyASCII, update.Relay.Key) {
		t.Fatal("relay key aliases the signaling snapshot")
	}
}

func TestGroupRelayAllocateStateCarriesParticipantSubscriptions(t *testing.T) {
	initial := []byte{0x00, 0x01, 0x02}
	key := bytes.Repeat([]byte{0x24}, 16)
	token := bytes.Repeat([]byte{0x42}, 174)
	hbhFEC := [2]uint32{0x11223344, 0x55667788}
	state := newGroupRelayAllocateStateWithHBHFEC(initial, key, hbhFEC)
	endpoint := &relayEndpoint{
		relayName: "zrh1c01",
		addresses: []relayAddress{{ipv4: "157.240.17.62", port: 3478}},
	}
	relayUpdate := &groupCallRelay{
		TransactionID: 4, Key: key, Tokens: [][]byte{token},
		Endpoints: []groupCallRelayEndpoint{{RelayName: "zrh1c01", TokenID: 0}},
	}
	streamSSRCs := [9]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	appDataSSRC := uint32(10)
	pids := []uint32{31, 47}
	transactionID := [12]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	var sent []byte
	changed, err := state.ApplyWithSubscriptions(
		endpoint,
		relayUpdate,
		streamSSRCs,
		appDataSSRC,
		pids,
		transactionID,
		func(packet []byte) error {
			sent = bytes.Clone(packet)
			return nil
		},
	)
	if err != nil || !changed {
		t.Fatalf("ApplyWithSubscriptions = (%v, %v), want (true, nil)", changed, err)
	}
	endpointXOR, _ := stun.EncodeXorRelayEndpoint("157.240.17.62", 3478)
	want := stun.BuildWasmStunAllocateRequestWithGroupSubscriptionsAndHBHFEC(
		transactionID, token, endpointXOR, streamSSRCs, appDataSSRC, hbhFEC, pids, key,
	)
	if !bytes.Equal(sent, want) {
		t.Fatalf("group allocate = %x, want %x", sent, want)
	}
}

func TestGroupRelayAllocateSendFailureDoesNotCommitRotation(t *testing.T) {
	initial := []byte{0x00, 0x01, 0x02}
	initialKey := bytes.Repeat([]byte{0x12}, 16)
	state := newGroupRelayAllocateStateWithHBHFEC(initial, initialKey, [2]uint32{})
	endpoint := &relayEndpoint{
		relayName: "zrh1c01",
		addresses: []relayAddress{{ipv4: "157.240.17.62", port: 3478}},
	}
	relayUpdate := &groupCallRelay{
		TransactionID: 1,
		Key:           bytes.Repeat([]byte{0x24}, 16),
		Tokens:        [][]byte{bytes.Repeat([]byte{0x42}, 174)},
		Endpoints:     []groupCallRelayEndpoint{{RelayName: "zrh1c01", TokenID: 0}},
	}
	sendErr := errors.New("relay unavailable")
	changed, err := state.ApplyWithSubscriptions(
		endpoint, relayUpdate, [9]uint32{}, 0, nil, [12]byte{},
		func([]byte) error { return sendErr },
	)
	if !errors.Is(err, sendErr) || changed {
		t.Fatalf("failed ApplyWithSubscriptions = (%v, %v)", changed, err)
	}
	if got := state.Current(); !bytes.Equal(got, initial) {
		t.Fatalf("current allocate after failure = %x, want %x", got, initial)
	}
}

func TestConnectedRemoteParticipantPIDsExcludesSelfAndInactive(t *testing.T) {
	self := types.JID{User: "100", Server: types.HiddenUserServer, Device: 1}
	remote := types.JID{User: "200", Server: types.HiddenUserServer, Device: 2}
	inactive := types.JID{User: "300", Server: types.HiddenUserServer, Device: 3}
	update := groupCallUpdate{Participants: []groupCallParticipant{
		{State: "connected", Devices: []groupCallDevice{{JID: self, PID: 11, HasPID: true}}},
		{State: "connected", Devices: []groupCallDevice{{JID: remote, PID: 22, HasPID: true}}},
		{State: "ringing", Devices: []groupCallDevice{{JID: inactive, PID: 33, HasPID: true}}},
	}}
	got := connectedRemoteParticipantPIDs(update, rtp.FormatE2ESrtpParticipantID(self.String()))
	if len(got) != 1 || got[0] != 22 {
		t.Fatalf("connected remote PIDs = %v, want [22]", got)
	}
}

func TestOffloadedGroupRelayRenewalAddsSecondParticipant(t *testing.T) {
	setup := testGroupMediaSetup(t)
	setup.Relay.KeyASCII = bytes.Repeat([]byte{0x24}, 16)
	setup.Relay.Tokens = [][]byte{bytes.Repeat([]byte{0x42}, 174)}
	setup.Relay.Endpoints[0].TokenID = 0
	offloaded, err := NewOffloadedGroupCall(setup)
	if err != nil {
		t.Fatalf("NewOffloadedGroupCall: %v", err)
	}
	mediaCall := offloaded.eng.calls[setup.CallID]
	defer mediaCall.groupReceivers.clear()

	initialUpdate, err := groupUpdateFromMediaSetup(setup)
	if err != nil {
		t.Fatalf("groupUpdateFromMediaSetup: %v", err)
	}
	state := newGroupRelayAllocateStateWithHBHFEC([]byte{0, 1, 2}, setup.Relay.KeyASCII, [2]uint32{})
	endpoint := getMediaRelayEndpoint(mediaCall.relay, false)
	streamSSRCs := [9]uint32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	appDataSSRC := uint32(10)
	transactionID := [12]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	var sent []byte
	send := func(packet []byte) error {
		sent = bytes.Clone(packet)
		return nil
	}
	changed, err := state.ApplyWithSubscriptions(
		endpoint, initialUpdate.Relay, streamSSRCs, appDataSSRC,
		connectedRemoteParticipantPIDs(initialUpdate, mediaCall.groupReceivers.selfID),
		transactionID, send,
	)
	if err != nil || !changed {
		t.Fatalf("initial ApplyWithSubscriptions = (%v, %v), want (true, nil)", changed, err)
	}

	renewal := cloneGroupCallUpdate(initialUpdate)
	renewal.TransactionID = 18
	renewal.Relay.TransactionID = 23
	renewal.Participants = append(renewal.Participants, groupCallParticipant{
		State: "connected",
		Devices: []groupCallDevice{{
			JID: types.NewADJID("300", types.LIDDomain, 3), PID: 11, HasPID: true,
		}},
	})
	handedOff := groupMediaSetupFrom(
		setup.CallID, setup.SelfLID, false, renewal.TransactionID, setup.Epoch.RawKey, renewal,
	)
	if err = offloaded.ApplyGroupUpdate(GroupMediaUpdate{
		TransactionID: renewal.TransactionID, Roster: handedOff.Roster, Relay: handedOff.Relay,
	}); err != nil {
		t.Fatalf("ApplyGroupUpdate: %v", err)
	}
	if mediaCall.groupReceivers.byPID[11] == nil {
		t.Fatal("second participant was not installed in the receive registry")
	}

	current := cloneGroupCallUpdate(*mediaCall.groupUpdate)
	participantPIDs := connectedRemoteParticipantPIDs(current, mediaCall.groupReceivers.selfID)
	if len(participantPIDs) != 2 || participantPIDs[0] != 9 || participantPIDs[1] != 11 {
		t.Fatalf("renewed remote PIDs = %v, want [9 11]", participantPIDs)
	}
	sent = nil
	changed, err = state.ApplyWithSubscriptions(
		endpoint, current.Relay, streamSSRCs, appDataSSRC, participantPIDs, transactionID, send,
	)
	if err != nil {
		t.Fatalf("renewed ApplyWithSubscriptions: %v", err)
	}
	if !changed {
		t.Fatal("relay renewal was discarded: transaction ID did not survive the offload round trip")
	}
	endpointXOR, ok := stun.EncodeXorRelayEndpoint(endpoint.addresses[0].ipv4, endpoint.addresses[0].port)
	if !ok {
		t.Fatal("encode relay endpoint")
	}
	want := stun.BuildWasmStunAllocateRequestWithGroupSubscriptionsAndHBHFEC(
		transactionID, setup.Relay.Tokens[0], endpointXOR, streamSSRCs, appDataSSRC,
		[2]uint32{}, []uint32{9, 11}, setup.Relay.KeyASCII,
	)
	if !bytes.Equal(sent, want) {
		t.Fatal("renewed allocate does not subscribe to both remote participants")
	}
}
