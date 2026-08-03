#include "facade.h"

#include <atomic>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "cast/streaming/public/encoded_frame.h"
#include "cast/streaming/public/environment.h"
#include "cast/streaming/rtp_time.h"
#include "message_port_bridge.h"
#include "platform/api/time.h"
#include "platform/base/ip_address.h"
#include "platform/impl/platform_client_posix.h"
#include "platform/impl/task_runner.h"
#include "session.h"

namespace {

// How often to refresh the atomics polled by
// breadcast_caststream_sender_{needs_key_frame,estimated_bandwidth_bps}.
// These are advisory (encoder bitrate/keyframe hints), so a small amount of
// staleness is fine -- this just needs to be fast enough not to be the
// bottleneck in reacting to network conditions.
constexpr std::chrono::milliseconds kPollInterval(100);

// Used as the pre-negotiation default so the very first frames (sent before
// any RTCP feedback exists to compute a real estimate from) aren't wildly
// over-encoded. Conservative middle-of-the-road value; real estimates from
// BandwidthEstimator::ComputeNetworkBandwidth() take over once available.
constexpr int32_t kDefaultBandwidthEstimateBps = 2 * 1000 * 1000;

}  // namespace

struct CastStreamSender {
  std::unique_ptr<openscreen::cast::Environment> environment;
  std::unique_ptr<breadcast_caststream::MessagePortBridge> message_port;
  std::unique_ptr<breadcast_caststream::MirroringSenderSession> session;

  // Set once, the first time a frame is enqueued after negotiation
  // completes; used to compute monotonically-increasing RTP timestamps
  // relative to session start.
  bool have_origin = false;
  int64_t origin_capture_time_us = 0;

  // negotiated uses acquire/release so that once
  // breadcast_caststream_sender_enqueue_frame observes it true (from an
  // arbitrary caller thread), `session->video_sender()` is guaranteed
  // already set -- see OnNegotiatedTrampoline below.
  std::atomic<bool> negotiated{false};
  std::atomic<bool> needs_key_frame{true};
  std::atomic<int32_t> estimated_bandwidth_bps{kDefaultBandwidthEstimateBps};

  // The caller's own user_data + callbacks, as passed to `_create`. Not
  // called directly -- session.h/message_port_bridge.h are instead given
  // trampolines below (with `this` as their user_data) so this struct can
  // update its own bookkeeping (e.g. `negotiated`) before forwarding.
  void* rust_user_data = nullptr;
  BreadcastOnNegotiatedFn rust_on_negotiated = nullptr;
  BreadcastOnErrorFn rust_on_error = nullptr;
  BreadcastOnPictureLostFn rust_on_picture_lost = nullptr;

  void SchedulePoll() {
    environment->task_runner().PostTaskWithDelay(
        [this] {
          if (session && session->video_sender()) {
            needs_key_frame.store(session->video_sender()->NeedsKeyFrame(),
                                  std::memory_order_relaxed);
            const int bps = session->GetEstimatedBandwidthBps();
            if (bps > 0) {
              estimated_bandwidth_bps.store(bps, std::memory_order_relaxed);
            }
          }
          SchedulePoll();
        },
        kPollInterval);
  }
};

namespace {

void OnNegotiatedTrampoline(void* user_data) {
  auto* sender = static_cast<CastStreamSender*>(user_data);
  sender->negotiated.store(true, std::memory_order_release);
  if (sender->rust_on_negotiated) {
    sender->rust_on_negotiated(sender->rust_user_data);
  }
}

void OnErrorTrampoline(void* user_data, const char* message, size_t message_len) {
  auto* sender = static_cast<CastStreamSender*>(user_data);
  if (sender->rust_on_error) {
    sender->rust_on_error(sender->rust_user_data, message, message_len);
  }
}

void OnPictureLostTrampoline(void* user_data) {
  auto* sender = static_cast<CastStreamSender*>(user_data);
  if (sender->rust_on_picture_lost) {
    sender->rust_on_picture_lost(sender->rust_user_data);
  }
}

}  // namespace

extern "C" {

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
    BreadcastOnPictureLostFn on_picture_lost) {
  auto address_result =
      openscreen::IPAddress::Parse(std::string(remote_ip, remote_ip_len));
  if (!address_result) {
    return nullptr;
  }

  auto handle = std::make_unique<CastStreamSender>();
  handle->rust_user_data = user_data;
  handle->rust_on_negotiated = on_negotiated;
  handle->rust_on_error = on_error;
  handle->rust_on_picture_lost = on_picture_lost;

  // Spins up openscreen's TaskRunner + networking threads. Safe to call more
  // than once process-wide only if ShutDown() was called first -- breadcast
  // only ever has one active cast-streaming session at a time, so this
  // assumption (baked into PlatformClientPosix's own singleton design) holds.
  openscreen::PlatformClientPosix::Create(std::chrono::milliseconds(50));
  openscreen::TaskRunner& task_runner =
      openscreen::PlatformClientPosix::GetInstance()->GetTaskRunner();

  breadcast_caststream::VideoParams params;
  params.width = width;
  params.height = height;
  params.max_bit_rate = max_bitrate_bps;
  params.max_frame_rate_numerator = max_frame_rate_numerator;
  params.max_frame_rate_denominator = max_frame_rate_denominator;

  breadcast_caststream::SessionCallbacks callbacks;
  callbacks.user_data = handle.get();
  callbacks.on_negotiated = &OnNegotiatedTrampoline;
  callbacks.on_error = &OnErrorTrampoline;
  callbacks.on_picture_lost = &OnPictureLostTrampoline;

  const openscreen::IPAddress remote_address = address_result.value();
  std::string local_source_id_str(local_source_id, local_source_id_len);
  std::string receiver_id_str(receiver_id, receiver_id_len);

  // Environment's constructor synchronously creates and binds a UdpSocket,
  // whose posix implementation asserts it's only ever touched from the
  // TaskRunner thread (see udp_socket_posix.cc) -- so construction has to
  // happen there too, not on this (arbitrary caller's) thread.
  CastStreamSender* handle_ptr = handle.get();
  std::promise<void> constructed;
  std::future<void> constructed_future = constructed.get_future();
  task_runner.PostTask([handle_ptr, &task_runner, &params, &callbacks, remote_address,
                        local_source_id_str, receiver_id_str, post_message, user_data,
                        &constructed] {
    handle_ptr->environment =
        std::make_unique<openscreen::cast::Environment>(&openscreen::Clock::now, task_runner);
    handle_ptr->message_port =
        std::make_unique<breadcast_caststream::MessagePortBridge>(user_data, post_message);
    handle_ptr->session = std::make_unique<breadcast_caststream::MirroringSenderSession>(
        *handle_ptr->environment, *handle_ptr->message_port, remote_address,
        local_source_id_str, receiver_id_str, params, callbacks);
    constructed.set_value();
  });
  constructed_future.wait();

  return handle.release();
}

void breadcast_caststream_sender_negotiate(CastStreamSender* sender) {
  sender->environment->task_runner().PostTask([sender] {
    sender->session->Negotiate();
    sender->SchedulePoll();
  });
}

void breadcast_caststream_sender_on_message(CastStreamSender* sender,
                                            const char* source_id,
                                            size_t source_id_len,
                                            const char* message_namespace,
                                            size_t message_namespace_len,
                                            const char* message,
                                            size_t message_len) {
  auto source = std::make_shared<std::string>(source_id, source_id_len);
  auto ns = std::make_shared<std::string>(message_namespace, message_namespace_len);
  auto body = std::make_shared<std::string>(message, message_len);
  sender->environment->task_runner().PostTask([sender, source, ns, body] {
    sender->message_port->DeliverMessage(*source, *ns, *body);
  });
}

int32_t breadcast_caststream_sender_enqueue_frame(CastStreamSender* sender,
                                                  const uint8_t* data,
                                                  size_t data_len,
                                                  int32_t is_key_frame,
                                                  int64_t capture_time_us) {
  if (!sender->negotiated.load(std::memory_order_acquire)) {
    return -1;
  }

  // Copied here (not captured as a borrowed span) because PostTask defers
  // execution -- `data` is only guaranteed valid for the duration of this
  // call, per facade.h's documented contract.
  auto owned_data = std::make_shared<std::vector<uint8_t>>(data, data + data_len);
  const bool is_key = is_key_frame != 0;

  sender->environment->task_runner().PostTask([sender, owned_data, is_key, capture_time_us] {
    using namespace openscreen;
    using namespace openscreen::cast;

    Sender* video_sender = sender->session->video_sender();
    if (!video_sender) {
      return;
    }

    if (!sender->have_origin) {
      sender->have_origin = true;
      sender->origin_capture_time_us = capture_time_us;
    }

    const FrameId frame_id = video_sender->GetNextFrameId();
    const FrameId referenced_frame_id =
        is_key || frame_id == FrameId::first() ? frame_id : frame_id - 1;

    // breadcast's encoder (vah264enc via GStreamer) is assumed to produce a
    // simple linear IPPP GOP structure (no B-frames / no multi-reference),
    // so "depends on the immediately preceding frame" is a correct
    // reference, not just an approximation.
    const int64_t elapsed_us = capture_time_us - sender->origin_capture_time_us;
    const RtpTimeTicks rtp_timestamp = RtpTimeTicks::FromTimeSinceOrigin(
        std::chrono::microseconds(elapsed_us), video_sender->config().rtp_timebase);

    const EncodedFrame::Dependency dependency =
        is_key ? EncodedFrame::Dependency::kKeyFrame : EncodedFrame::Dependency::kDependent;
    EncodedFrame frame(dependency, frame_id, referenced_frame_id, rtp_timestamp, Clock::now(),
                       /*new_playout_delay=*/std::chrono::milliseconds::zero(),
                       ByteView(owned_data->data(), owned_data->size()));

    // EnqueueFrame()'s result (e.g. MAX_DURATION_IN_FLIGHT under backpressure)
    // isn't propagated to the caller: by the time this runs, enqueue_frame()
    // has already returned 0 synchronously (this call is posted, not
    // immediate -- see facade.h's threading contract). Backpressure here just
    // means this one frame is dropped; the encoder finds out indirectly via
    // needs_key_frame()/estimated_bandwidth_bps() polling.
    (void)video_sender->EnqueueFrame(frame);
  });

  return 0;
}

int32_t breadcast_caststream_sender_needs_key_frame(CastStreamSender* sender) {
  return sender->needs_key_frame.load(std::memory_order_relaxed) ? 1 : 0;
}

int32_t breadcast_caststream_sender_estimated_bandwidth_bps(CastStreamSender* sender) {
  return sender->estimated_bandwidth_bps.load(std::memory_order_relaxed);
}

void breadcast_caststream_sender_destroy(CastStreamSender* sender) {
  if (!sender) {
    return;
  }
  // These must be torn down on the TaskRunner thread (they hold raw
  // references into it and into `environment`), so hop over there and block
  // until it's done before shutting the TaskRunner itself down.
  std::promise<void> done;
  std::future<void> done_future = done.get_future();
  sender->environment->task_runner().PostTask([sender, &done] {
    sender->session.reset();
    sender->message_port.reset();
    sender->environment.reset();
    done.set_value();
  });
  done_future.wait();

  openscreen::PlatformClientPosix::ShutDown();
  delete sender;
}

}  // extern "C"
