package meowcaller

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/purpshell/meowcaller/mlow"
	"go.mau.fi/whatsmeow/types"
)

// GroupMediaSetup is the serializable media state for an offloaded group call.
type GroupMediaSetup struct {
	CallID          string
	SelfLID         string
	SelfPID         uint32
	HasSelfPID      bool
	ParticipantUUID string
	Epoch           GroupMediaEpoch
	Roster          []GroupParticipantSetup
	Relay           GroupRelaySetup
}

// GroupMediaEpoch contains one decrypted group media epoch.
type GroupMediaEpoch struct {
	TransactionID uint32
	RawKey        []byte
}

// GroupParticipantSetup identifies one connected group-call device.
type GroupParticipantSetup struct {
	DeviceJID string
	PID       uint32
	HasPID    bool
}

// GroupRelaySetup contains the group relay's credentials and endpoints.
type GroupRelaySetup struct {
	ParticipantUUID  string
	SelfPID          uint32
	HasSelfPID       bool
	KeyASCII         []byte
	HBHKey           []byte
	Tokens           [][]byte
	AuthTokens       [][]byte
	Endpoints        []GroupRelayEndpointSetup
	WarpMITagLength  uint32
	AttributePadding bool
}

// GroupRelayEndpointSetup identifies one group relay endpoint.
type GroupRelayEndpointSetup struct {
	RelayID     uint32
	RelayName   string
	TokenID     uint32
	AuthTokenID uint32
	IsFNA       bool
	IPv4        string
	Port        uint16
}

// GroupMediaUpdate is one authoritative roster and relay transaction.
type GroupMediaUpdate struct {
	TransactionID uint32
	Roster        []GroupParticipantSetup
	Relay         GroupRelaySetup
	RekeySeen     bool
}

// GroupMediaCallbacks carries control-plane changes to the media owner.
type GroupMediaCallbacks struct {
	OnGroupUpdate func(GroupMediaUpdate) error
	OnGroupEpoch  func(uint32, []byte) error
}

// MediaGroupOffloadFunc transfers group media to another process.
type MediaGroupOffloadFunc func(GroupMediaSetup) (GroupMediaCallbacks, error)

// WithGroupMediaOffload diverts group media while keeping signaling in this process.
func WithGroupMediaOffload(fn MediaGroupOffloadFunc) Option {
	return func(c *config) { c.mediaGroupOffload = fn }
}

// NewOffloadedGroupCall prepares group media in a process without a WhatsApp session.
func NewOffloadedGroupCall(setup GroupMediaSetup, opts ...Option) (*OffloadedGroupCall, error) {
	if setup.CallID == "" || setup.SelfLID == "" {
		return nil, errors.New("meowcaller: group media setup has no call id or self lid")
	}
	if setup.Epoch.TransactionID == 0 || len(setup.Epoch.RawKey) != 32 {
		return nil, errors.New("meowcaller: group media setup has invalid epoch")
	}
	update, err := groupUpdateFromMediaSetup(setup)
	if err != nil {
		return nil, err
	}
	if update.Relay == nil || len(update.Relay.Key) == 0 || len(update.Relay.Endpoints) == 0 {
		return nil, errors.New("meowcaller: group media setup has no usable relay")
	}
	cfg := resolveConfig(opts)
	c := &Client{log: cfg.log, diag: cfg.diag}
	e := newEngine(c)
	c.eng = e
	selfJID, err := types.ParseJID(setup.SelfLID)
	if err != nil || selfJID.User == "" || selfJID.Server == "" {
		return nil, fmt.Errorf("meowcaller: invalid group media self lid %q", setup.SelfLID)
	}
	call := &Call{eng: e, id: setup.CallID, peer: selfJID}
	receivers, err := newParticipantReceiveRegistry(
		setup.CallID, setup.Epoch.RawKey, setup.SelfLID, setup.SelfLID,
		func() participantAudioDecoder { return mlow.NewMlowDecoder(mlow.WithLogger(cfg.log)) },
		WithLogger(cfg.log),
	)
	if err != nil {
		return nil, err
	}
	if err = receivers.ApplyGroupUpdate(update); err != nil {
		receivers.clear()
		return nil, fmt.Errorf("meowcaller: initialize offloaded group roster: %w", err)
	}
	if err = receivers.ApplyGroupRawEpoch(setup.Epoch.TransactionID, setup.Epoch.RawKey); err != nil {
		receivers.clear()
		return nil, fmt.Errorf("meowcaller: initialize offloaded group epoch: %w", err)
	}
	groupRelay, err := groupRelayData(update, false)
	if err != nil {
		receivers.clear()
		return nil, err
	}
	e.calls[setup.CallID] = &engineCall{
		call: call, selfLID: setup.SelfLID, peerLID: setup.SelfLID,
		direction: CallDirectionOutgoing, group: true, groupUpdate: &update,
		groupReceivers: receivers, groupRawEpoch: bytes.Clone(setup.Epoch.RawKey),
		groupEpochTxID: setup.Epoch.TransactionID, hasGroupEpoch: true, relay: groupRelay,
	}
	return &OffloadedGroupCall{call: call, eng: e, callID: setup.CallID}, nil
}

// OffloadedGroupCall owns one group's media loop.
type OffloadedGroupCall struct {
	call   *Call
	eng    *engine
	callID string
}

// Call exposes the live group call so a Player and AudioSink can be attached.
func (o *OffloadedGroupCall) Call() *Call { return o.call }

// Run drives group media until ctx is cancelled or the relay ends.
func (o *OffloadedGroupCall) Run(ctx context.Context) error {
	o.eng.mu.Lock()
	m := o.eng.calls[o.callID]
	o.eng.mu.Unlock()
	if m == nil {
		return errors.New("meowcaller: offloaded group call was already torn down")
	}
	o.call.setPhase(CallPhaseConnecting)
	key := bytes.Clone(m.groupRawEpoch)
	defer clear(key)
	return o.eng.runMedia(ctx, o.callID, o.call, key, m.selfLID, m.peerLID, m.relay, false)
}

// ApplyGroupUpdate installs a newer authoritative roster on the media owner.
func (o *OffloadedGroupCall) ApplyGroupUpdate(update GroupMediaUpdate) error {
	internal, err := groupCallUpdateFromMediaUpdate(o.callID, update)
	if err != nil {
		return err
	}
	if !o.eng.applyGroupUpdate(internal) {
		return fmt.Errorf("meowcaller: group update transaction %d was not applied", update.TransactionID)
	}
	return nil
}

// InstallGroupEpoch installs or buffers a decrypted group media epoch.
func (o *OffloadedGroupCall) InstallGroupEpoch(transactionID uint32, raw []byte) error {
	return o.eng.installGroupEpoch(o.callID, transactionID, raw)
}

func groupUpdateFromMediaSetup(setup GroupMediaSetup) (groupCallUpdate, error) {
	return groupCallUpdateFromMediaUpdate(setup.CallID, GroupMediaUpdate{
		TransactionID: setup.Epoch.TransactionID, Roster: setup.Roster, Relay: setup.Relay,
	})
}

func groupCallUpdateFromMediaUpdate(callID string, update GroupMediaUpdate) (groupCallUpdate, error) {
	out := groupCallUpdate{CallID: callID, TransactionID: update.TransactionID, Media: "audio", RekeyRequested: update.RekeySeen}
	for _, participant := range update.Roster {
		jid, err := types.ParseJID(participant.DeviceJID)
		if err != nil || jid.User == "" || jid.Server == "" {
			return groupCallUpdate{}, fmt.Errorf("meowcaller: invalid group device jid %q", participant.DeviceJID)
		}
		out.Participants = append(out.Participants, groupCallParticipant{
			JID: jid.ToNonAD(), State: "connected", Devices: []groupCallDevice{{JID: jid, PID: participant.PID, HasPID: participant.HasPID}},
		})
	}
	relay := update.Relay
	out.Relay = &groupCallRelay{
		SelfPID: relay.SelfPID, HasSelfPID: relay.HasSelfPID,
		ParticipantUUID: relay.ParticipantUUID, AttributePadding: relay.AttributePadding,
		WarpMITagLength: relay.WarpMITagLength, HasWarpMITagLength: true,
		Key: bytes.Clone(relay.KeyASCII), HBHKey: bytes.Clone(relay.HBHKey),
		Tokens: cloneByteSlices(relay.Tokens), AuthTokens: cloneByteSlices(relay.AuthTokens),
		Endpoints: make([]groupCallRelayEndpoint, 0, len(relay.Endpoints)),
	}
	for _, endpoint := range relay.Endpoints {
		out.Relay.Endpoints = append(out.Relay.Endpoints, groupCallRelayEndpoint{
			RelayID: endpoint.RelayID, RelayName: endpoint.RelayName,
			TokenID: endpoint.TokenID, AuthTokenID: endpoint.AuthTokenID,
			IsFNA: endpoint.IsFNA, IPv4: endpoint.IPv4, Port: endpoint.Port,
		})
	}
	return out, nil
}

func groupMediaSetupFrom(callID, selfLID string, epoch uint32, raw []byte, update groupCallUpdate) GroupMediaSetup {
	setup := GroupMediaSetup{CallID: callID, SelfLID: selfLID,
		Epoch: GroupMediaEpoch{TransactionID: epoch, RawKey: bytes.Clone(raw)}}
	if update.Relay != nil {
		setup.SelfPID, setup.HasSelfPID = update.Relay.SelfPID, update.Relay.HasSelfPID
		setup.ParticipantUUID = update.Relay.ParticipantUUID
		setup.Relay = GroupRelaySetup{
			ParticipantUUID: update.Relay.ParticipantUUID,
			SelfPID:         update.Relay.SelfPID, HasSelfPID: update.Relay.HasSelfPID,
			KeyASCII: bytes.Clone(update.Relay.Key), HBHKey: bytes.Clone(update.Relay.HBHKey),
			Tokens: cloneByteSlices(update.Relay.Tokens), AuthTokens: cloneByteSlices(update.Relay.AuthTokens),
			WarpMITagLength: update.Relay.WarpMITagLength, AttributePadding: update.Relay.AttributePadding,
			Endpoints: make([]GroupRelayEndpointSetup, 0, len(update.Relay.Endpoints)),
		}
		for _, ep := range update.Relay.Endpoints {
			setup.Relay.Endpoints = append(setup.Relay.Endpoints, GroupRelayEndpointSetup{
				RelayID: ep.RelayID, RelayName: ep.RelayName, TokenID: ep.TokenID,
				AuthTokenID: ep.AuthTokenID, IsFNA: ep.IsFNA, IPv4: ep.IPv4, Port: ep.Port,
			})
		}
	}
	for _, participant := range update.Participants {
		if participant.State != "connected" {
			continue
		}
		for _, device := range participant.Devices {
			setup.Roster = append(setup.Roster, GroupParticipantSetup{
				DeviceJID: device.JID.String(), PID: device.PID, HasPID: device.HasPID,
			})
		}
	}
	return setup
}
