# Datasheet: `voip/group_invite_accept`

Acceptance of a directed invitation into an already-active ad-hoc call, using
the enriched `group_info` embedded in the incoming offer to establish group
state and call-scoped signaling before `preaccept`.

**Status:** implemented; the video-bearing accept is inferred from live evidence
and its live acceptance is pending (see "Video extension").

**Reference pinned at:** capture SHA-256
`9d6463714430c55ddb3ccb95e153f1d06d11a1feea7a153d1ea95f39f48b6889`

The capture covers the audio-only invitation. WhatsApp has no published spec for
the video-bearing variant, and the WhatsApp-Rust reference `build_accept` carries
no `<video>` child at all (video is a meowcaller extension derived from live 1:1
calls). Everything below the audio-only shape is grounded in the live evidence
listed, not in a captured stanza.

## Reference source (verbatim — authoritative)

The verbatim authority is `diag/captures/whatsapp-20260724-222532.jsonl`: 19,285
events and 26,287,992 bytes, sanitized into the immutable two-sided capture
identified above. Only the audio-only invitation appears there.

| Raw boundary | Captured fact |
|---|---|
| line 9,467 | the inviter's directed offer contains the existing connected roster but no group transaction attributes |
| line 9,572 | the added endpoint receives an enriched offer with `joinable="1"` and embedded `group_info` transaction 7; no group JID or encrypted call key is present |
| lines 9,608–9,609 | the added endpoint sends `preaccept` to `CALLID@call`, not directly to the inviter device |
| line 10,227 | the added endpoint invokes `acceptCall` |
| lines 10,231–10,237 | it immediately sends `accept` to `CALLID@call` with exactly `audio`, `net`, and `encopt`; no `metadata` child is present |
| line 10,285 | the server acknowledges that `accept` |
| line 10,684 | the first inbound `mute_v2` arrives after the accept and its acknowledgement |

The vector must preserve these relationships:

```text
offer without group_info               -> no group snapshot; direct signaling
offer with enriched group_info         -> transaction-ordered group snapshot
ordinary 1:1 offer                      -> encrypted call key required
enriched active-call offer              -> accepted without an offer call key
offer joinable == "1"                  -> snapshot Joinable true
ad-hoc group without group-jid         -> valid empty GroupJID
snapshot installed before preaccept    -> preaccept target CALLID@call
active-group AcceptCall                -> immediate accept to CALLID@call
active-group accept children           -> audio, net, encopt; no metadata
active-group accept send failure       -> not connected; retry remains possible
ordinary 1:1 AcceptCall                -> existing deferred accept behavior
pending group call without a call key   -> no media-ready event yet
```

## Video extension (live evidence, no capture)

WhatsApp answers an active-group accept that omits `<video>` for an offer that
advertised video with `accept-ack error_code=431` (invalid or missing field). The
observed run is 2-nov-2026 on `dev`, group video flags on, call IDs
`003146F1CE0DF2F0721B62489180EE9A` and `00025EB23F6A1AD3D9CD5E4C97F4B29E`:

```text
gateway-1  active group invite preaccepted call_id=003146F1… video=true
gateway-1  ignoring accept-ack server error call_id=003146F1… error_code=431
```

What the same day's evidence rules in and out:

- The same audio-only accept on a voice group invitation is accepted (the
  gateway's group path works for audio every day).
- The `<video dec="H264" device_orientation="0"/>` child is accepted by the
  server inside a group-scoped `preaccept` to `CALLID@call` (no error ack is ever
  logged for it), so a `<video>` child in a group-scoped stanza is not by itself
  an invalid field.
- The 1:1 accept with that exact `<video>` child is accepted live
  (`accepted (after mute_v2) video=true`).
- The failing accept was audio-only: the gateway answered through `Call.Answer`,
  and before this change `Answer` never advertised video.

Therefore the missing field is the `<video>` child: an active-group accept must
answer the offer it was invited with. Accepting audio-only would reproduce the
431, so the audio-only fallback is not a working fallback here — it is the
already-observed failure mode.

## Go envelope (signatures only)

```go
func BuildActiveGroupAccept(callID string, callCreator types.JID, requestID string, video ...bool) (waBinary.Node, error)

func (c *Call) Answer() error
func (c *Call) AnswerWithVideo(video bool) error
```

`Answer` now mirrors the offer: for a group call it includes the H.264 `<video>`
child when the offer advertised video, and stays audio-only otherwise.
`AnswerWithVideo(false)` is the explicit decline (audio-only accept) and
`AnswerWithVideo(true)` forces the video child. A direct call keeps ignoring the
flag: its deferred accept still follows the call's local/peer video state.

## Implementation suggestions (guidance, not authoritative)

- Treat an absent `group_info` as an ordinary 1:1 offer, returning
  `(nil, false, nil)`.
- Parse the embedded `group_info` through the same attribute, participant, and
  device parser used by `ParseGroupUpdate`; do not maintain a second roster
  parser.
- Take call ID and creator from the offer envelope and reject conflicting
  non-empty values on `group_info`.
- Copy `joinable="1"` from the offer envelope onto the typed snapshot because
  the captured embedded `group_info` does not repeat it.
- Require and decrypt the existing `<enc>` key for ordinary 1:1 offers. For an
  enriched active-call offer, register the call with a nil key and let the
  outbound group-rekey module provide the first media epoch.
- Install the complete snapshot while registering the inbound call, before
  `BuildEagerPreaccept`.
- Use `signalingTarget()` for eager `preaccept`.
- For an installed active-group snapshot, send `accept` immediately to
  `CALLID@call` with `audio` Opus/16 kHz, `net` medium 2, and `encopt` keygen 2,
  plus the H.264 `<video>` child when the offer advertised video. Do not attach
  direct-call metadata.
- Send outside the call-state lock. Mark only the exact state connected after a
  successful send; a failed send must clear its in-flight reservation and remain
  retryable.
- Coalesce concurrent or repeated active-group acceptance so one state produces
  at most one successful wire accept. Preserve the ordinary 1:1 deferred path.
- Do not dispatch a synthetic `CallGroupUpdate`; later real server snapshots
  remain the membership and media-roster lifecycle source.
- Preserve the existing media-ready nil-key gate so an accepted active-call
  invite cannot start media with fabricated or stale key material.

## Validation

- `TestActiveGroupAcceptVideoStanzaXML` pins the exact XML of both accepts: the
  captured audio-only shape and the video shape (child order audio → video → net
  → encopt, `dec="H264"`, `device_orientation="0"`, envelope id and
  `CALLID@call` target).
- `TestGroupAnswerVideoOptionControlsAcceptChildren` proves the call-state rule:
  `Answer` mirrors an offered video, `AnswerWithVideo(false)` declines it, and
  `AnswerWithVideo(true)` advertises it.
- Pending: a live group video invitation accepted with the video child, proving
  the 431 is gone and that the accepted leg reaches media. Until that run, the
  video-bearing accept stays an inference with the evidence above.
