package meowcaller

import (
	"context"
	"errors"
	"fmt"

	"go.mau.fi/whatsmeow/types"
)

// MediaSetup is everything the media loop needs to run, in a form that survives
// serialization. It carries live key material for the call.
type MediaSetup struct {
	CallID  string
	CallKey []byte
	SelfLID string
	PeerLID string
	PeerJID string
	Inbound bool
	Relay   RelaySetup
}

// RelaySetup is the elected relay's keying and endpoints.
type RelaySetup struct {
	KeyASCII  []byte
	Tokens    [][]byte
	Endpoints []RelayEndpointSetup
}

// RelayEndpointSetup is one relay endpoint offered by the server.
type RelayEndpointSetup struct {
	RelayID     uint32
	RelayName   string
	TokenID     uint32
	AuthTokenID uint32
	IsFNA       bool
	Addresses   []RelayAddressSetup
}

// RelayAddressSetup is one address of a relay endpoint.
type RelayAddressSetup struct {
	IPv4 string
	Port uint16
}

// MediaOffloadFunc receives the setup for a call whose media must run somewhere
// else. Returning an error leaves the call without media.
type MediaOffloadFunc func(MediaSetup) error

// WithMediaOffload diverts the media loop out of this process. The session keeps
// owning signaling; whoever receives the setup owns the relay transport, the
// keying and the codec. Group calls are unaffected and always run locally.
func WithMediaOffload(fn MediaOffloadFunc) Option {
	return func(c *config) { c.mediaOffload = fn }
}

// ErrNoRelayEndpoint reports a setup that carries no usable relay address.
var ErrNoRelayEndpoint = errors.New("meowcaller: media setup has no relay endpoint")

func relaySetupFrom(rd *relayData) RelaySetup {
	if rd == nil {
		return RelaySetup{}
	}
	out := RelaySetup{
		KeyASCII:  append([]byte(nil), rd.relayKeyASCII...),
		Endpoints: make([]RelayEndpointSetup, 0, len(rd.endpoints)),
	}
	for _, t := range rd.relayTokens {
		out.Tokens = append(out.Tokens, append([]byte(nil), t...))
	}
	for _, ep := range rd.endpoints {
		e := RelayEndpointSetup{
			RelayID:     ep.relayID,
			RelayName:   ep.relayName,
			TokenID:     ep.tokenID,
			AuthTokenID: ep.authTokenID,
			IsFNA:       ep.isFNA,
			Addresses:   make([]RelayAddressSetup, 0, len(ep.addresses)),
		}
		for _, a := range ep.addresses {
			e.Addresses = append(e.Addresses, RelayAddressSetup{IPv4: a.ipv4, Port: a.port})
		}
		out.Endpoints = append(out.Endpoints, e)
	}
	return out
}

func (rs RelaySetup) toRelayData(peerJID types.JID) *relayData {
	rd := &relayData{
		relayKeyASCII: append([]byte(nil), rs.KeyASCII...),
		peerJID:       peerJID,
		endpoints:     make([]relayEndpoint, 0, len(rs.Endpoints)),
	}
	for _, t := range rs.Tokens {
		rd.relayTokens = append(rd.relayTokens, append([]byte(nil), t...))
	}
	for _, ep := range rs.Endpoints {
		e := relayEndpoint{
			relayID:     ep.RelayID,
			relayName:   ep.RelayName,
			tokenID:     ep.TokenID,
			authTokenID: ep.AuthTokenID,
			isFNA:       ep.IsFNA,
			addresses:   make([]relayAddress, 0, len(ep.Addresses)),
		}
		for _, a := range ep.Addresses {
			e.addresses = append(e.addresses, relayAddress{ipv4: a.IPv4, port: a.Port})
		}
		rd.endpoints = append(rd.endpoints, e)
	}
	return rd
}

func mediaSetupFrom(callID string, callKey []byte, selfLID, peerLID string, rd *relayData, inbound bool) MediaSetup {
	s := MediaSetup{
		CallID:  callID,
		CallKey: append([]byte(nil), callKey...),
		SelfLID: selfLID,
		PeerLID: peerLID,
		Inbound: inbound,
		Relay:   relaySetupFrom(rd),
	}
	if rd != nil {
		s.PeerJID = rd.peerJID.String()
	}
	return s
}

// OffloadedCall runs one call's media in a process that has no WhatsApp session.
// Attach a Player and an AudioSink to Call before Run.
type OffloadedCall struct {
	call  *Call
	eng   *engine
	setup MediaSetup
}

// NewOffloadedCall prepares the media loop for a setup produced by a session
// elsewhere. It opens nothing until Run.
func NewOffloadedCall(setup MediaSetup, opts ...Option) (*OffloadedCall, error) {
	if setup.CallID == "" {
		return nil, errors.New("meowcaller: media setup has no call id")
	}
	if len(setup.CallKey) == 0 {
		return nil, errors.New("meowcaller: media setup has no call key")
	}
	if len(setup.Relay.KeyASCII) == 0 {
		return nil, errors.New("meowcaller: media setup has no relay key")
	}
	usable := false
	for _, ep := range setup.Relay.Endpoints {
		if len(ep.Addresses) > 0 {
			usable = true
			break
		}
	}
	if !usable {
		return nil, ErrNoRelayEndpoint
	}

	peerJID, err := types.ParseJID(setup.PeerJID)
	if err != nil {
		return nil, fmt.Errorf("meowcaller: media setup peer jid: %w", err)
	}
	// ParseJID does not reject a string without a separator: it returns it whole
	// as the server, with an empty user, and no error.
	if peerJID.User == "" || peerJID.Server == "" {
		return nil, fmt.Errorf("meowcaller: media setup peer jid is not a jid: %q", setup.PeerJID)
	}

	cfg := resolveConfig(opts)
	c := &Client{log: cfg.log, diag: cfg.diag}
	e := newEngine(c)
	c.eng = e

	call := &Call{eng: e, id: setup.CallID, peer: peerJID}
	direction := CallDirectionOutgoing
	if setup.Inbound {
		direction = CallDirectionIncoming
	}
	e.calls[setup.CallID] = &engineCall{
		call:      call,
		callKey:   append([]byte(nil), setup.CallKey...),
		relay:     setup.Relay.toRelayData(peerJID),
		selfLID:   setup.SelfLID,
		peerLID:   setup.PeerLID,
		direction: direction,
		started:   true,
	}
	return &OffloadedCall{call: call, eng: e, setup: setup}, nil
}

// Call exposes the live call so a Player and an AudioSink can be attached.
func (o *OffloadedCall) Call() *Call { return o.call }

// Run drives the media loop until ctx is cancelled or the transport ends.
func (o *OffloadedCall) Run(ctx context.Context) error {
	o.eng.mu.Lock()
	m := o.eng.calls[o.setup.CallID]
	o.eng.mu.Unlock()
	if m == nil {
		return errors.New("meowcaller: offloaded call was already torn down")
	}
	o.call.setPhase(CallPhaseConnecting)
	callKey := append([]byte(nil), m.callKey...)
	defer clear(callKey)
	return o.eng.runMedia(ctx, o.setup.CallID, o.call, callKey, m.selfLID, m.peerLID, m.relay, o.setup.Inbound)
}

// RekeyPeer points the receive path at the device that actually answered. The
// session learns this from signaling; a process running only media cannot.
func (o *OffloadedCall) RekeyPeer(peerLID string) error {
	o.eng.mu.Lock()
	m := o.eng.calls[o.setup.CallID]
	var fn func(string) error
	if m != nil {
		fn = m.rekeyPeer
		m.peerLID = peerLID
	}
	o.eng.mu.Unlock()
	if fn == nil {
		return nil
	}
	return fn(peerLID)
}
