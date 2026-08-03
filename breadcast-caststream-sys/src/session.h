// A minimal, video-only Cast Streaming sender negotiation driver.
//
// This deliberately does NOT use openscreen's own
// cast::SenderSession -- that class bundles mirroring negotiation together
// with RPC/remoting/input support, which pulls in protobuf
// (input.pb.h/remoting.pb.h) for no benefit here (breadcast only ever does
// one-way video mirroring). Instead, this reimplements just the
// OFFER-building and ANSWER-handling logic SenderSession itself uses
// internally (see CreateMirroringOffer/StartNegotiation/SelectSenders in
// upstream's public/sender_session.cc), built directly on
// SenderSessionMessenger + Offer/Answer/SessionConfig + SenderImpl. See
// ../vendor/openscreen/PATCHES.md.
#ifndef BREADCAST_CASTSTREAM_SESSION_H_
#define BREADCAST_CASTSTREAM_SESSION_H_

#include <cstddef>
#include <memory>
#include <string>

#include "cast/streaming/impl/sender_impl.h"
#include "cast/streaming/public/environment.h"
#include "cast/streaming/public/offer_messages.h"
#include "cast/streaming/public/receiver_message.h"
#include "cast/streaming/public/sender.h"
#include "cast/streaming/public/session_messenger.h"
#include "cast/streaming/sender_packet_router.h"
#include "platform/base/ip_address.h"

namespace breadcast_caststream {

struct VideoParams {
  int width = 1920;
  int height = 1080;
  int max_bit_rate = 8 * 1000 * 1000;
  int max_frame_rate_numerator = 30;
  int max_frame_rate_denominator = 1;
};

struct SessionCallbacks {
  void* user_data = nullptr;
  void (*on_negotiated)(void* user_data) = nullptr;
  void (*on_error)(void* user_data, const char* message, size_t message_len) =
      nullptr;
  void (*on_picture_lost)(void* user_data) = nullptr;
};

// Owns the OFFER/ANSWER exchange and, once negotiated, the resulting video
// Sender. All methods (other than the constructor) must be called on
// `environment`'s TaskRunner thread -- facade.cc is responsible for
// marshaling calls onto it via TaskRunner::PostTask, matching the threading
// contract the rest of openscreen's Environment/SenderPacketRouter/Sender
// classes already assume.
class MirroringSenderSession final : public openscreen::cast::Sender::Observer {
 public:
  MirroringSenderSession(openscreen::cast::Environment& environment,
                         openscreen::cast::MessagePort& message_port,
                         openscreen::IPAddress remote_address,
                         std::string local_source_id,
                         std::string receiver_id,
                         VideoParams params,
                         SessionCallbacks callbacks);
  ~MirroringSenderSession() override;

  MirroringSenderSession(const MirroringSenderSession&) = delete;
  MirroringSenderSession& operator=(const MirroringSenderSession&) = delete;

  // Sends the OFFER and begins waiting for an ANSWER. `callbacks.on_negotiated`
  // or `callbacks.on_error` will be called once the exchange completes.
  void Negotiate();

  // Valid only after `on_negotiated` has fired.
  openscreen::cast::Sender* video_sender() { return video_sender_.get(); }

  // Best-effort current bandwidth estimate in bits per second, or a
  // conservative default before enough RTCP feedback has arrived.
  int GetEstimatedBandwidthBps() const;

  // Sender::Observer implementation.
  void OnFrameCanceled(openscreen::cast::FrameId frame_id) override;
  void OnPictureLost() override;

 private:
  void OnAnswer(openscreen::ErrorOr<openscreen::cast::ReceiverMessage> message);
  void ReportError(const std::string& message);

  openscreen::cast::Environment& environment_;
  const openscreen::IPAddress remote_address_;
  const std::string receiver_id_;
  const VideoParams params_;
  const SessionCallbacks callbacks_;

  openscreen::cast::SenderSessionMessenger messenger_;
  openscreen::cast::SenderPacketRouter packet_router_;

  int sequence_number_ = 0;
  openscreen::cast::Offer pending_offer_;
  std::unique_ptr<openscreen::cast::SenderImpl> video_sender_;
};

}  // namespace breadcast_caststream

#endif  // BREADCAST_CASTSTREAM_SESSION_H_
