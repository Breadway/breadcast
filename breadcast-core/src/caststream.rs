//! Safe wrapper over `breadcast-caststream-sys`'s raw FFI to the vendored
//! openscreen Cast Streaming sender — the same low-latency mirroring
//! protocol Chrome's tab/desktop casting uses (unlike [`crate::cast_sender`]'s
//! HLS approach, which targets the Default Media Receiver instead). See
//! `breadcast-caststream-sys/vendor/openscreen/PATCHES.md` for how the
//! vendored C++ this wraps was built.
//!
//! This does *not* replicate [`CastSession`](crate::cast_sender::CastSession)'s
//! own single-io-thread actor pattern internally — the underlying C++ already
//! runs its own dedicated TaskRunner/networking threads (see `facade.h`'s
//! threading contract), so every method here just marshals across FFI rather
//! than through a Rust-owned loop. What *does* need a Rust-side thread is
//! draining [`CastStreamEvents`] and forwarding [`CastStreamEvent::OutboundMessage`]
//! over the existing CASTV2 connection — see `cast_stream_test.rs` for the
//! intended pattern (pump events on one thread, call `on_message`/
//! `enqueue_frame` from others).

use std::ffi::c_void;
use std::os::raw::c_char;
use std::sync::mpsc;

use anyhow::{Result, bail};
use breadcast_caststream_sys::{
    self as sys, breadcast_caststream_sender_create, breadcast_caststream_sender_destroy,
    breadcast_caststream_sender_enqueue_frame, breadcast_caststream_sender_estimated_bandwidth_bps,
    breadcast_caststream_sender_needs_key_frame, breadcast_caststream_sender_negotiate,
    breadcast_caststream_sender_on_message,
};

/// The Cast Streaming ("Mirroring") receiver app id, pre-installed on every
/// Chromecast/Google TV — distinct from [`rust_cast::channels::receiver::CastDeviceApp::DefaultMediaReceiver`]'s
/// `CC1AD845`, which is what [`crate::cast_sender::CastSession`] launches for
/// the HLS path.
pub const MIRRORING_APP_ID: &str = "0F5096E8";

/// The CASTV2 namespace Cast Streaming's OFFER/ANSWER exchange runs on.
pub const WEBRTC_NAMESPACE: &str = "urn:x-cast:com.google.cast.webrtc";

#[derive(Debug, Clone, Copy)]
pub struct VideoParams {
    pub width: i32,
    pub height: i32,
    pub max_bitrate_bps: i32,
    pub max_frame_rate_numerator: i32,
    pub max_frame_rate_denominator: i32,
}

impl Default for VideoParams {
    fn default() -> Self {
        Self {
            // Must match what `build_video_pipeline_for_streaming` actually
            // encodes (`breadcast-core/src/pipeline/mod.rs`), not just what
            // we'd like to send -- this OFFER's resolution is what the
            // receiver allocates its decoder/output surface for. Advertising
            // 1920x1080 while actually sending 1280x720 frames is a real
            // protocol mismatch that plausibly explains a receiver decoder
            // corrupting/freezing rather than just looking soft.
            width: 1280,
            height: 720,
            max_bitrate_bps: 8_000_000,
            max_frame_rate_numerator: 30,
            max_frame_rate_denominator: 1,
        }
    }
}

/// Events pushed from the underlying C++ TaskRunner thread — see this
/// module's doc comment on why a Rust-owned pump loop is still needed even
/// though the FFI layer runs its own threads.
#[derive(Debug)]
pub enum CastStreamEvent {
    /// The C++ side needs this JSON `message` sent to `destination_id` on
    /// [`WEBRTC_NAMESPACE`] over the existing CASTV2 connection (e.g. the
    /// OFFER). The caller is expected to do that via
    /// `rust_cast::CastDevice::send_message` (see the `send_message` patch
    /// documented in `vendor/rust_cast-0.21.0/PATCHES.md`).
    OutboundMessage { destination_id: String, message: String },
    /// OFFER/ANSWER negotiation succeeded; `enqueue_frame` will now accept
    /// frames.
    Negotiated,
    /// A negotiation or session error occurred.
    Error(String),
    /// The receiver reported picture loss and wants a key frame ASAP (also
    /// obtainable via the pull-style [`CastStreamSender::needs_key_frame`]).
    PictureLost,
}

struct CallbackContext {
    events_tx: mpsc::Sender<CastStreamEvent>,
}

/// A live Cast Streaming sender session. See the module doc comment for the
/// threading model.
pub struct CastStreamSender {
    raw: *mut sys::CastStreamSender,
    // Kept alive for `raw`'s lifetime -- its address is the FFI `user_data`
    // every callback trampoline below casts back. Never read directly
    // through this field; the callbacks access it via the raw pointer, so
    // this exists purely to own the allocation and free it (after
    // `destroy()`, in `Drop`) rather than leak it.
    _context: Box<CallbackContext>,
}

// Safety: the underlying C++ handle has no thread-affinity for the FFI
// entry points themselves (see facade.h's threading contract) -- every
// `breadcast_caststream_sender_*` call internally marshals onto the
// TaskRunner thread via `TaskRunner::PostTask`, which is documented
// thread-safe regardless of caller thread. All methods below take `&self`
// only (no interior mutation outside that marshaling), so concurrent calls
// from multiple threads sharing an `Arc<CastStreamSender>` are as safe as
// they are from a single thread -- hence `Sync` too, not just `Send`.
unsafe impl Send for CastStreamSender {}
unsafe impl Sync for CastStreamSender {}

impl CastStreamSender {
    /// Starts a Cast Streaming session targeting `remote_ip` (the same IP
    /// `rust_cast` already connected to for the CASTV2 control channel).
    /// `local_source_id`/`receiver_id` are the CASTV2 source/destination IDs
    /// to use on [`WEBRTC_NAMESPACE`] -- `receiver_id` should be the
    /// launched Mirroring app's `transport_id` (the same id
    /// `connection`/`media` channels already target), matching how every
    /// other namespace conversation with a launched app is addressed.
    ///
    /// Returns the sender plus a receiver for [`CastStreamEvent`]s -- drain
    /// it on a dedicated thread; `OutboundMessage` events in particular need
    /// prompt forwarding for negotiation to make progress.
    pub fn start(
        remote_ip: &str,
        local_source_id: &str,
        receiver_id: &str,
        params: VideoParams,
    ) -> Result<(Self, mpsc::Receiver<CastStreamEvent>)> {
        let (events_tx, events_rx) = mpsc::channel();
        let context = Box::into_raw(Box::new(CallbackContext { events_tx }));

        let raw = unsafe {
            breadcast_caststream_sender_create(
                remote_ip.as_ptr() as *const c_char,
                remote_ip.len(),
                local_source_id.as_ptr() as *const c_char,
                local_source_id.len(),
                receiver_id.as_ptr() as *const c_char,
                receiver_id.len(),
                params.width,
                params.height,
                params.max_bitrate_bps,
                params.max_frame_rate_numerator,
                params.max_frame_rate_denominator,
                context as *mut c_void,
                post_message_trampoline,
                on_negotiated_trampoline,
                on_error_trampoline,
                on_picture_lost_trampoline,
            )
        };

        if raw.is_null() {
            // SAFETY: `context` was created by the `Box::into_raw` above and
            // has not been handed to any live C++ object (create() failed
            // before storing it anywhere), so reclaiming and dropping it
            // here is the only way to avoid leaking it.
            drop(unsafe { Box::from_raw(context) });
            bail!("breadcast_caststream_sender_create failed (invalid remote_ip?)");
        }

        // SAFETY: `context` was created by `Box::into_raw` immediately
        // above and its address was just handed to the C++ side as
        // `user_data` -- reconstructing the `Box` here doesn't move or free
        // the underlying allocation (only dropping it would), so the
        // pointer C++ holds stays valid for as long as this `Box` lives,
        // i.e. until `Drop` runs (after `destroy()`, see below).
        let context = unsafe { Box::from_raw(context) };

        Ok((Self { raw, _context: context }, events_rx))
    }

    /// Sends the OFFER and begins waiting for an ANSWER (delivered via
    /// [`Self::on_message`]). Completion is reported as a
    /// [`CastStreamEvent::Negotiated`] or [`CastStreamEvent::Error`] on the
    /// event receiver returned by [`Self::start`].
    pub fn negotiate(&self) {
        unsafe { breadcast_caststream_sender_negotiate(self.raw) };
    }

    /// Delivers a message received on [`WEBRTC_NAMESPACE`] (e.g. the
    /// receiver's ANSWER) into the session.
    pub fn on_message(&self, source_id: &str, message_namespace: &str, message: &str) {
        unsafe {
            breadcast_caststream_sender_on_message(
                self.raw,
                source_id.as_ptr() as *const c_char,
                source_id.len(),
                message_namespace.as_ptr() as *const c_char,
                message_namespace.len(),
                message.as_ptr() as *const c_char,
                message.len(),
            );
        }
    }

    /// Enqueues one encoded video access unit (Annex-B H.264) for sending.
    /// `capture_time_us` only needs to be monotonically increasing and
    /// proportional to real elapsed time between frames -- it does not need
    /// to be wall-clock-accurate.
    ///
    /// Returns an error if the session isn't negotiated yet or the frame
    /// was rejected under backpressure; callers should treat the latter as
    /// a dropped frame, not a fatal condition (see
    /// [`Self::needs_key_frame`]/[`Self::estimated_bandwidth_bps`] for how
    /// to react).
    pub fn enqueue_frame(&self, data: &[u8], is_key_frame: bool, capture_time_us: i64) -> Result<()> {
        let result = unsafe {
            breadcast_caststream_sender_enqueue_frame(
                self.raw,
                data.as_ptr(),
                data.len(),
                is_key_frame as i32,
                capture_time_us,
            )
        };
        if result != 0 {
            bail!("frame not enqueued (session not negotiated yet)");
        }
        Ok(())
    }

    /// True if the receiver wants a key frame as soon as possible. Cheap to
    /// poll frequently (e.g. once per captured frame, before encoding it).
    pub fn needs_key_frame(&self) -> bool {
        unsafe { breadcast_caststream_sender_needs_key_frame(self.raw) != 0 }
    }

    /// Best-effort current bandwidth estimate in bits per second, meant to
    /// drive the video encoder's target bitrate -- this vendored subset of
    /// openscreen only does flow control, not congestion control. Cheap to
    /// poll frequently.
    pub fn estimated_bandwidth_bps(&self) -> i32 {
        unsafe { breadcast_caststream_sender_estimated_bandwidth_bps(self.raw) }
    }
}

impl Drop for CastStreamSender {
    fn drop(&mut self) {
        // Blocks until the C++ side's threads stop -- after this returns,
        // no more callbacks will fire, so it's safe for `_context` to be
        // freed right after (implicitly, as this struct finishes dropping).
        unsafe { breadcast_caststream_sender_destroy(self.raw) };
    }
}

unsafe fn context_from_user_data<'a>(user_data: *mut c_void) -> &'a CallbackContext {
    // SAFETY: every callback below is only ever invoked by the C++ facade
    // with the exact `user_data` pointer passed into `sender_create`, which
    // is `_context`'s address for the lifetime of the owning
    // `CastStreamSender` (see its field doc comment) -- and per facade.h's
    // threading contract, no callback fires after `sender_destroy` returns,
    // which is also the last point `_context` could be dropped.
    unsafe { &*(user_data as *const CallbackContext) }
}

unsafe fn str_from_raw_parts<'a>(ptr: *const c_char, len: usize) -> std::borrow::Cow<'a, str> {
    // SAFETY: every callback below documents (matching facade.h) that these
    // buffers are borrowed and valid only for the duration of the call --
    // this is called synchronously within that window, and the result is
    // copied (via `.into_owned()` at each call site) before returning.
    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
    String::from_utf8_lossy(bytes)
}

extern "C" fn post_message_trampoline(
    user_data: *mut c_void,
    destination_id: *const c_char,
    destination_id_len: usize,
    _message_namespace: *const c_char,
    _message_namespace_len: usize,
    message: *const c_char,
    message_len: usize,
) {
    let ctx = unsafe { context_from_user_data(user_data) };
    let destination_id = unsafe { str_from_raw_parts(destination_id, destination_id_len) }.into_owned();
    let message = unsafe { str_from_raw_parts(message, message_len) }.into_owned();
    let _ = ctx.events_tx.send(CastStreamEvent::OutboundMessage { destination_id, message });
}

extern "C" fn on_negotiated_trampoline(user_data: *mut c_void) {
    let ctx = unsafe { context_from_user_data(user_data) };
    let _ = ctx.events_tx.send(CastStreamEvent::Negotiated);
}

extern "C" fn on_error_trampoline(user_data: *mut c_void, message: *const c_char, message_len: usize) {
    let ctx = unsafe { context_from_user_data(user_data) };
    let message = unsafe { str_from_raw_parts(message, message_len) }.into_owned();
    let _ = ctx.events_tx.send(CastStreamEvent::Error(message));
}

extern "C" fn on_picture_lost_trampoline(user_data: *mut c_void) {
    let ctx = unsafe { context_from_user_data(user_data) };
    let _ = ctx.events_tx.send(CastStreamEvent::PictureLost);
}
