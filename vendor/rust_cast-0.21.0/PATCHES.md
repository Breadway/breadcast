# Vendoring notes

This is [rust_cast 0.21.0](https://github.com/azasypkin/rust-cast) (MIT
licensed), vendored via `[patch.crates-io]` in the workspace `Cargo.toml`
purely to add one method upstream doesn't have: a generic point-to-point
send on an arbitrary CASTV2 namespace.

## Why

breadcast's Cast Streaming integration (`breadcast-caststream-sys`, wrapping
a vendored `chromium/openscreen`) needs to exchange OFFER/ANSWER JSON with a
launched receiver app on the `urn:x-cast:com.google.cast.webrtc` namespace --
a point-to-point conversation targeting that app's `transport_id`, the same
target `ConnectionChannel`/`MediaChannel` already use. None of rust_cast's
built-in channels expose that: `ReceiverChannel::broadcast_message()` is the
closest, but it hardcodes destination `"*"`, which is a different
conversation than a namespace-specific exchange with one particular app.

Receiving such messages needs no patch -- `CastDevice::receive()` already
returns them as `ChannelMessage::Raw(CastMessage)` whenever no built-in
channel claims the namespace.

## The patch

`src/lib.rs`: added `CastDevice::send_message(&self, namespace: &str,
destination: &str, message: &str)`, built the same way
`ReceiverChannel::broadcast_message()` is internally, but with a
caller-supplied `destination` instead of a hardcoded `"*"`. The payload is
a raw string (already-serialized JSON from the vendored openscreen C++),
not a `Serialize` value -- a generic `M: Serialize` would double-encode
the OFFER. See the doc comment on that method (marked "LOCAL PATCH
(breadcast, not upstream)").

## Rolling the pin

To move to a newer rust_cast release: copy the new version from
`~/.cargo/registry/src/*/rust_cast-<version>/`, re-apply the same method
addition (small enough to redo by hand), swap the version in this
directory's own `Cargo.toml`, and update the `path` if the directory name
changes.
