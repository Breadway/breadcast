# Vendoring notes

This directory is a **pruned subset** of [chromium/openscreen](https://chromium.googlesource.com/openscreen),
pinned at the commit in `PINNED_COMMIT`, licensed BSD-3-Clause (see `LICENSE`).
It contains only the files needed for the Cast Streaming *sender* data path
(RTP/RTCP send, OFFER/ANSWER message types, AES frame encryption) plus the
POSIX platform/util plumbing that path needs: not the receiver, not OSP
discovery, not remoting/RPC, not TLS (breadcast's TLS CASTV2 control channel
is handled by the existing `rust_cast`-based Rust code; this vendored code
only drives the UDP RTP/RTCP data path once `rust_cast` has already
negotiated OFFER/ANSWER and we know where to send packets).

It is compiled directly via the `cc` crate in `../build.rs`, not GN/Ninja;
there is no build system here to regenerate anything from.

## Rolling the pin

There are no release/API-stability guarantees upstream. To update:
1. Re-run the file selection against the new commit's `cast/streaming/BUILD.gn`
   (`:common` + `:sender` targets, minus `public/sender_session.*` and
   `public/rpc_messenger.*`; see "What's excluded" below) plus
   `platform/BUILD.gn`'s `:base`/`:api`/`:standalone_impl` targets.
2. Re-apply the patches below (they're small; check if upstream has since
   fixed the same problem and the patch can be dropped).
3. Update `PINNED_COMMIT` and rebuild.

## What's excluded and why

- **`public/sender_session.{h,cc}`, `public/rpc_messenger.{h,cc}`,
  `public/receiver_session.*`, receiver-side files, `remoting.proto`/
  `input.proto`**: `SenderSession` bundles mirroring negotiation together
  with RPC/remoting/input support, which pulls in protobuf
  (`input.pb.h`/`remoting.pb.h`) for no benefit here (breadcast only ever
  does one-way video mirroring). Instead, `../src/session.cc` drives
  `Offer`/`Answer`/`SessionConfig` directly: logic adapted from
  `sender_session.cc`'s `CreateMirroringOffer`/`StartNegotiation`/
  `SelectSenders`, minus everything RPC/remoting/audio/input-related.
- **All TLS support** (`platform/impl/tls_*`, `platform/impl/stream_socket*`):
  unused; see above.
- **`util/crypto/{certificate_utils,digest_sign,pem_helpers,rsa_private_key,
  secure_hash,sha2}.*`**: only needed for OSP discovery / X.509 certificate
  handling, not the RTP data path.
- **`util/scoped_wake_lock_mac.cc`**: macOS-only.

## Local patches (not upstream)

1. **`build/build_config.h`, `build/buildflag.h`**: stand-ins for Chromium's
   GN-generated versions of these headers. Only define what the 3 callers
   here actually check (`IS_POSIX`, `IS_LINUX`, `IS_APPLE`, `IS_ANDROID`),
   hardcoded for Linux.
2. **`patches/aes_ctr128_compat.cc`**: `frame_crypto.cc` calls
   `AES_ctr128_encrypt()`, a BoringSSL convenience wrapper not in system
   OpenSSL's public headers. Reimplemented from scratch (standard CTR mode
   over `AES_encrypt()`, which OpenSSL does still expose).
3. **`platform/impl/platform_client_posix.{h,cc}`**: stripped the
   `TlsDataRouterPosix` member/accessor (see "All TLS support" above).
4. **`util/crypto/openssl_util.{h,cc}`**: dropped `SSLErrorCodeToError()`/
   `GetSSLError()`, which reference BoringSSL's `SSL_error_description()`
   (not in system OpenSSL). Unused for the same reason as (3).
5. **`util/base64.cc`**: upstream implements this on
   `third_party/modp_b64`, which isn't fetched by a plain shallow clone
   (pulled in separately via gclient/DEPS in a full Chromium checkout).
   Reimplemented on `EVP_EncodeBlock`/`EVP_DecodeBlock` from system OpenSSL
   instead, same public interface.
6. **`cast/streaming/impl/sender_impl.cc`**: raised `kMinSenderInFlight`
   from upstream's 66ms to 200ms. This is a behaviour patch, not a
   portability one, and is the only one here that changes what goes on the
   wire, so unlike the others it should *not* be silently re-applied when
   rolling the pin without re-measuring first.

   `GetMaxInFlightMediaDuration()` sizes the sender's send window as
   `clamp(2*RTT, kMinSenderInFlight, target_playout_delay/3)`. Upstream's
   66ms floor assumes the RTT is negligible, which holds for Chrome's own
   usage but not for breadcast's measured case: instrumenting real
   `Sender::EnqueueFrame` result codes (see `BreadcastEnqueueStats` in
   `../../src/facade.h`) against a Chromecast over Wi-Fi showed RTT of
   57-145ms and a steady 3-40% of frames per second rejected with
   `MAX_DURATION_IN_FLIGHT`. Because breadcast enqueues already-encoded
   frames, each such rejection silently breaks the H.264 reference chain
   rather than merely dropping a frame, which is what the user-visible
   multi-second picture freezes turned out to be.

   Pairs with `kTargetPlayoutDelay` in `../../src/session.cc`, which raises
   the *ceiling* of that same clamp (the ceiling, not this floor, was the
   binding constraint at 400ms playout delay). Both are needed: this floor
   covers RTT dips, that ceiling covers the normal case.
