//! Raw FFI bindings to `src/facade.h`/`src/facade.cc`, which wrap a pruned,
//! vendored subset of `chromium/openscreen`'s Cast Streaming sender (see
//! `vendor/openscreen/PATCHES.md`). This crate is intentionally low-level and
//! unsafe -- see `breadcast-caststream` (not this crate) for the ergonomic,
//! thread-safe wrapper most callers should use instead.
//!
//! # Threading contract
//!
//! `sender_create` spins up openscreen's own TaskRunner + networking threads
//! internally; callers don't manage those. Every `sender_*` function taking
//! a `*mut CastStreamSender` is safe to call from any thread (calls are
//! internally marshaled onto the TaskRunner thread). The callbacks passed to
//! `sender_create`, however, fire FROM that TaskRunner thread, not the
//! caller's thread -- see `facade.h`'s doc comment for the full contract,
//! which mirrors the single-io-thread actor pattern breadcast-core's
//! `CastSession` already uses for CASTV2.

use std::ffi::{c_char, c_void};

#[repr(C)]
pub struct CastStreamSender {
    _private: [u8; 0],
}

// Safety: every function below is documented (facade.h) as safe to call
// from any thread; only the callbacks fire cross-thread, and those are
// plain `extern "C" fn` pointers rather than captured state, so there is no
// non-Send/Sync data hanging off `*mut CastStreamSender` itself.
unsafe impl Send for CastStreamSender {}

pub type PostMessageFn = extern "C" fn(
    user_data: *mut c_void,
    destination_id: *const c_char,
    destination_id_len: usize,
    message_namespace: *const c_char,
    message_namespace_len: usize,
    message: *const c_char,
    message_len: usize,
);

pub type OnNegotiatedFn = extern "C" fn(user_data: *mut c_void);

pub type OnErrorFn =
    extern "C" fn(user_data: *mut c_void, message: *const c_char, message_len: usize);

pub type OnPictureLostFn = extern "C" fn(user_data: *mut c_void);

unsafe extern "C" {
    /// Returns null on failure (e.g. an unparseable `remote_ip`, or the
    /// local UDP socket failed to bind).
    ///
    /// # Safety
    /// `remote_ip`/`local_source_id`/`receiver_id` must each point to
    /// `_len` valid, readable bytes for the duration of this call.
    /// `post_message`/`on_negotiated`/`on_error`/`on_picture_lost` must be
    /// valid to call for as long as the returned sender is alive (i.e.
    /// until `sender_destroy` returns). `user_data` is passed back
    /// unmodified to every callback and may be null.
    pub fn breadcast_caststream_sender_create(
        remote_ip: *const c_char,
        remote_ip_len: usize,
        local_source_id: *const c_char,
        local_source_id_len: usize,
        receiver_id: *const c_char,
        receiver_id_len: usize,
        width: i32,
        height: i32,
        max_bitrate_bps: i32,
        max_frame_rate_numerator: i32,
        max_frame_rate_denominator: i32,
        user_data: *mut c_void,
        post_message: PostMessageFn,
        on_negotiated: OnNegotiatedFn,
        on_error: OnErrorFn,
        on_picture_lost: OnPictureLostFn,
    ) -> *mut CastStreamSender;

    /// # Safety
    /// `sender` must be a live pointer returned by `sender_create` and not
    /// yet passed to `sender_destroy`.
    pub fn breadcast_caststream_sender_negotiate(sender: *mut CastStreamSender);

    /// Delivers an inbound message (e.g. the receiver's ANSWER) received on
    /// the CASTV2 `urn:x-cast:com.google.cast.webrtc` namespace into the
    /// session. All buffers are copied before this returns.
    ///
    /// # Safety
    /// `sender` must be live. `source_id`/`message_namespace`/`message` must
    /// each point to `_len` valid, readable bytes for the duration of this
    /// call only.
    pub fn breadcast_caststream_sender_on_message(
        sender: *mut CastStreamSender,
        source_id: *const c_char,
        source_id_len: usize,
        message_namespace: *const c_char,
        message_namespace_len: usize,
        message: *const c_char,
        message_len: usize,
    );

    /// Enqueues one encoded video access unit (Annex-B H.264) for sending.
    /// Returns 0 if queued, nonzero if not negotiated yet.
    ///
    /// # Safety
    /// `sender` must be live. `data` must point to `data_len` valid,
    /// readable bytes for the duration of this call only (it is copied
    /// before this returns).
    pub fn breadcast_caststream_sender_enqueue_frame(
        sender: *mut CastStreamSender,
        data: *const u8,
        data_len: usize,
        is_key_frame: i32,
        capture_time_us: i64,
    ) -> i32;

    /// # Safety
    /// `sender` must be live.
    pub fn breadcast_caststream_sender_needs_key_frame(sender: *mut CastStreamSender) -> i32;

    /// # Safety
    /// `sender` must be live.
    pub fn breadcast_caststream_sender_estimated_bandwidth_bps(
        sender: *mut CastStreamSender,
    ) -> i32;

    /// Tears down the session and blocks until openscreen's internal
    /// threads stop. `sender` must not be used again after this call.
    ///
    /// # Safety
    /// `sender` must be a live pointer returned by `sender_create`, not
    /// already passed to this function.
    pub fn breadcast_caststream_sender_destroy(sender: *mut CastStreamSender);
}
