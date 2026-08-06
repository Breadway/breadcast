#include "session.h"

#include <utility>

#include "cast/streaming/impl/rtp_defines.h"
#include "cast/streaming/message_fields.h"
#include "cast/streaming/public/constants.h"
#include "cast/streaming/public/session_config.h"
#include "cast/streaming/sender_message.h"
#include "util/crypto/random_bytes.h"

namespace breadcast_caststream {

namespace {

using openscreen::Error;
using openscreen::ErrorOr;
using openscreen::GenerateRandomBytes16;
using openscreen::cast::AudioStream;
using openscreen::cast::CastMode;
using openscreen::cast::GenerateSsrc;
using openscreen::cast::GetPayloadType;
using openscreen::cast::kMinVideoHeight;
using openscreen::cast::kMinVideoWidth;
using openscreen::cast::kRtpVideoTimebase;
using openscreen::cast::Offer;
using openscreen::cast::ReceiverMessage;
using openscreen::cast::Resolution;
using openscreen::cast::SenderMessage;
using openscreen::cast::SessionConfig;
using openscreen::cast::Stream;
using openscreen::cast::ToStreamType;
using openscreen::cast::VideoCodec;
using openscreen::cast::VideoStream;

// breadcast always mirrors exactly one video stream at index 0 -- there is
// no audio stream in this integration (breadcast's capture pipeline is
// video-only), so the index scheme sender_session.cc uses for interleaving
// audio-then-video streams collapses to just "index 0 is the video stream."
constexpr int kVideoStreamIndex = 0;

// The playout delay breadcast asks the receiver for -- the window between
// capture here and presentation there. Deliberately *not*
// openscreen::cast::kDefaultTargetPlayoutDelay (400ms), because that value
// turned out to be the binding constraint on throughput, not just on latency.
//
// SenderImpl::GetMaxInFlightMediaDuration() computes the sender's send window
// as clamp(2*RTT, kMinSenderInFlight, target_playout_delay/3). At a 400ms
// target that ceiling is 133ms -- about four frames at 30 FPS. Instrumented
// measurement against real hardware (see BreadcastEnqueueStats in facade.h)
// found the round-trip time to a Chromecast over Wi-Fi sitting at 57-145ms,
// i.e. 2*RTT of 114-290ms: consistently *above* that 133ms ceiling. The
// window was therefore pinned at the ceiling and 3-40% of frames were being
// rejected with MAX_DURATION_IN_FLIGHT every second, each one silently
// breaking the H.264 reference chain and freezing the picture until the next
// key frame.
//
// Raising this to 1200ms lifts the ceiling to 400ms, so 2*RTT lands inside
// the clamp and the window tracks measured network conditions the way
// openscreen intended, instead of being capped below one round trip. The cost
// is ~800ms of additional end-to-end latency, which is unnoticeable for
// screen mirroring to a TV and a straight trade against multi-second freezes.
//
// Note this is only the *sender's* half of the fix: it must stay paired with
// the kMinSenderInFlight patch in vendor/openscreen (see PATCHES.md), which
// raises the floor of that same clamp for the moments when RTT dips.
constexpr std::chrono::milliseconds kTargetPlayoutDelay(1200);

VideoStream BuildVideoStream(const VideoParams& params,
                             bool use_android_rtp_hack) {
  Stream stream;
  stream.index = kVideoStreamIndex;
  stream.type = Stream::Type::kVideoSource;
  stream.channels = 1;
  stream.rtp_payload_type = GetPayloadType(VideoCodec::kH264, use_android_rtp_hack);
  stream.ssrc = GenerateSsrc(/*higher_priority=*/false);
  stream.target_delay = kTargetPlayoutDelay;
  stream.aes_key = GenerateRandomBytes16();
  stream.aes_iv_mask = GenerateRandomBytes16();
  stream.receiver_rtcp_event_log = true;
  stream.rtp_timebase = kRtpVideoTimebase;

  VideoStream video_stream;
  video_stream.stream = std::move(stream);
  video_stream.codec = VideoCodec::kH264;
  video_stream.max_frame_rate = openscreen::SimpleFraction{
      params.max_frame_rate_numerator, params.max_frame_rate_denominator};
  video_stream.max_bit_rate = (params.max_bit_rate >= openscreen::cast::kDefaultVideoMinBitRate)
                                  ? params.max_bit_rate
                                  : openscreen::cast::kDefaultVideoMaxBitRate;
  video_stream.resolutions.push_back(Resolution{
      std::max(params.width, kMinVideoWidth), std::max(params.height, kMinVideoHeight)});
  return video_stream;
}

}  // namespace

MirroringSenderSession::MirroringSenderSession(
    openscreen::cast::Environment& environment,
    openscreen::cast::MessagePort& message_port,
    openscreen::IPAddress remote_address,
    std::string local_source_id,
    std::string receiver_id,
    VideoParams params,
    SessionCallbacks callbacks)
    : environment_(environment),
      remote_address_(remote_address),
      receiver_id_(std::move(receiver_id)),
      params_(params),
      callbacks_(callbacks),
      messenger_(
          message_port,
          std::move(local_source_id),
          receiver_id_,
          [this](Error error) { ReportError(error.message()); },
          environment.task_runner()),
      packet_router_(environment) {}

MirroringSenderSession::~MirroringSenderSession() {
  if (video_sender_) {
    video_sender_->SetObserver(nullptr);
  }
}

void MirroringSenderSession::Negotiate() {
  // use_android_rtp_hack defaults on upstream too (crbug.com/631828) --
  // Google TV devices are Android TV under the hood, so this is left on.
  constexpr bool kUseAndroidRtpHack = true;

  Offer offer;
  offer.cast_mode = CastMode::kMirroring;
  offer.video_streams.push_back(BuildVideoStream(params_, kUseAndroidRtpHack));
  pending_offer_ = offer;

  const Error result = messenger_.SendRequest(
      SenderMessage{SenderMessage::Type::kOffer, ++sequence_number_,
                    /*valid=*/true, std::move(offer)},
      ReceiverMessage::Type::kAnswer,
      [this](ErrorOr<ReceiverMessage> message) { OnAnswer(std::move(message)); });
  if (!result.ok()) {
    ReportError(result.message());
  }
}

void MirroringSenderSession::OnAnswer(ErrorOr<ReceiverMessage> message) {
  if (!message) {
    ReportError(message.error().message());
    return;
  }
  if (!message.value().valid || message.value().type != ReceiverMessage::Type::kAnswer) {
    ReportError("Receiver sent an invalid or unexpected ANSWER response");
    return;
  }

  const auto& answer = std::get<openscreen::cast::Answer>(message.value().body);
  if (answer.send_indexes.empty() || answer.ssrcs.empty()) {
    ReportError("ANSWER selected no streams");
    return;
  }

  environment_.set_remote_endpoint(
      openscreen::IPEndpoint{remote_address_, static_cast<uint16_t>(answer.udp_port)});

  const openscreen::cast::VideoStream& stream = pending_offer_.video_streams[0];
  const openscreen::cast::RtpPayloadType payload_type = stream.stream.rtp_payload_type;

  SessionConfig config{stream.stream.ssrc,
                       answer.ssrcs[0],
                       stream.stream.rtp_timebase,
                       stream.stream.channels,
                       stream.stream.target_delay,
                       stream.stream.aes_key,
                       stream.stream.aes_iv_mask,
                       /*is_pli_enabled=*/true,
                       ToStreamType(payload_type, /*use_android_rtp_hack=*/true)};
  if (!config.IsValid()) {
    ReportError("Derived an invalid SessionConfig from the ANSWER");
    return;
  }

  video_sender_ = std::make_unique<openscreen::cast::SenderImpl>(
      environment_, packet_router_, std::move(config), payload_type);
  video_sender_->SetObserver(this);

  if (callbacks_.on_negotiated) {
    callbacks_.on_negotiated(callbacks_.user_data);
  }
}

int MirroringSenderSession::GetEstimatedBandwidthBps() const {
  return packet_router_.ComputeNetworkBandwidth();
}

void MirroringSenderSession::OnFrameCanceled(openscreen::cast::FrameId) {}

void MirroringSenderSession::OnPictureLost() {
  if (callbacks_.on_picture_lost) {
    callbacks_.on_picture_lost(callbacks_.user_data);
  }
}

void MirroringSenderSession::ReportError(const std::string& message) {
  if (callbacks_.on_error) {
    callbacks_.on_error(callbacks_.user_data, message.data(), message.size());
  }
}

}  // namespace breadcast_caststream
