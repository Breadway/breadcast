// The extern "C" surface breadcast-caststream-sys's build.rs compiles and
// the Rust side (src/lib.rs) declares `extern "C"` bindings for.
//
// Threading contract: `breadcast_caststream_sender_create` spins up
// openscreen's own TaskRunner + networking threads internally (via
// PlatformClientPosix) -- callers don't manage those. All
// `breadcast_caststream_sender_*` functions taking a `CastStreamSender*` are
// safe to call from any single thread (they internally marshal onto the
// TaskRunner thread via TaskRunner::PostTask, which is documented
// thread-safe) -- but the *callbacks* passed to `_create` fire FROM that
// TaskRunner thread, not the caller's thread. This mirrors the existing
// single-io-thread actor pattern breadcast-core's CastSession already uses
// for CASTV2: the Rust wrapper is expected to run one dedicated thread that
// owns the CastStreamSender and treats callback invocations as arriving from
// a foreign thread (e.g. hands them off over an mpsc channel), exactly like
// CastSession's io loop already does for rust_cast's own callbacks.
#ifndef BREADCAST_CASTSTREAM_FACADE_H_
#define BREADCAST_CASTSTREAM_FACADE_H_

#include <cstddef>
#include <cstdint>

extern "C" {

typedef struct CastStreamSender CastStreamSender;

// Forwards an outbound OFFER/ANSWER-exchange message to Rust for sending
// over the existing CASTV2 urn:x-cast:com.google.cast.webrtc channel. All
// buffers are borrowed for the duration of the call only.
typedef void (*BreadcastPostMessageFn)(void* user_data,
                                       const char* destination_id,
                                       size_t destination_id_len,
                                       const char* message_namespace,
                                       size_t message_namespace_len,
                                       const char* message,
                                       size_t message_len);

// Fired once OFFER/ANSWER negotiation succeeds and the video sender is
// ready to accept frames.
typedef void (*BreadcastOnNegotiatedFn)(void* user_data);

// Fired on a negotiation or session error. `message` is borrowed for the
// duration of the call only.
typedef void (*BreadcastOnErrorFn)(void* user_data,
                                   const char* message,
                                   size_t message_len);

// Fired when the receiver reports picture loss and wants a key frame ASAP
// (a push notification; see also breadcast_caststream_sender_needs_key_frame
// for the pull-style equivalent, which also catches this condition).
typedef void (*BreadcastOnPictureLostFn)(void* user_data);

// Creates a session and starts openscreen's TaskRunner/networking threads.
// `remote_ip` is the receiver's IP address (the same one rust_cast already
// connected to for the CASTV2 control channel); `local_source_id`/
// `receiver_id` are the CASTV2 source/destination IDs to use when sending
// messages over the webrtc namespace (already known to the Rust caller from
// its existing CASTV2 session). Returns null on failure (e.g. invalid IP or
// failure to bind the local UDP socket).
CastStreamSender* breadcast_caststream_sender_create(
    const char* remote_ip,
    size_t remote_ip_len,
    const char* local_source_id,
    size_t local_source_id_len,
    const char* receiver_id,
    size_t receiver_id_len,
    int32_t width,
    int32_t height,
    int32_t max_bitrate_bps,
    int32_t max_frame_rate_numerator,
    int32_t max_frame_rate_denominator,
    void* user_data,
    BreadcastPostMessageFn post_message,
    BreadcastOnNegotiatedFn on_negotiated,
    BreadcastOnErrorFn on_error,
    BreadcastOnPictureLostFn on_picture_lost);

// Sends the OFFER and begins waiting for an ANSWER.
void breadcast_caststream_sender_negotiate(CastStreamSender* sender);

// Delivers a message received on the webrtc namespace (e.g. the ANSWER)
// into the session. All buffers are copied before this returns.
void breadcast_caststream_sender_on_message(CastStreamSender* sender,
                                            const char* source_id,
                                            size_t source_id_len,
                                            const char* message_namespace,
                                            size_t message_namespace_len,
                                            const char* message,
                                            size_t message_len);

// Enqueues one encoded video access unit (Annex-B H.264) for sending.
// `data` is copied before this returns, so the caller may reuse/free its
// buffer immediately after. `capture_time_us` is only used to derive the
// RTP timestamp's relative spacing between frames (it does not need to be
// wall-clock-accurate, just monotonically increasing and proportional to
// real elapsed time between frames). Returns 0 if queued, nonzero if the
// session isn't negotiated yet or the frame was rejected (e.g. too large,
// or the in-flight queue is full -- the caller should back off encoding
// when this happens rather than treating it as fatal).
int32_t breadcast_caststream_sender_enqueue_frame(CastStreamSender* sender,
                                                  const uint8_t* data,
                                                  size_t data_len,
                                                  int32_t is_key_frame,
                                                  int64_t capture_time_us);

// True (nonzero) if the receiver wants a key frame as soon as possible.
// Safe to poll frequently; cheap, non-blocking, lock-free.
int32_t breadcast_caststream_sender_needs_key_frame(CastStreamSender* sender);

// Best-effort current bandwidth estimate in bits per second. Safe to poll
// frequently; cheap, non-blocking, lock-free. Intended to drive the video
// encoder's target bitrate (this vendored subset of openscreen only does
// flow control, not congestion control -- see Sender's class comment in
// vendor/openscreen/cast/streaming/public/sender.h).
int32_t breadcast_caststream_sender_estimated_bandwidth_bps(CastStreamSender* sender);

// Tears down the session and stops openscreen's internal threads. Blocks
// until shutdown completes.
void breadcast_caststream_sender_destroy(CastStreamSender* sender);

}  // extern "C"

#endif  // BREADCAST_CASTSTREAM_FACADE_H_
