// A openscreen::cast::MessagePort implementation that forwards outbound
// messages to a Rust-provided callback (which sends them over the existing
// rust_cast-managed CASTV2 TLS channel, in the
// urn:x-cast:com.google.cast.webrtc namespace) and accepts inbound messages
// via DeliverMessage(), called by Rust when a reply arrives on that
// channel. This is what lets openscreen's own SenderSessionMessenger/
// SenderPacketRouter run OFFER/ANSWER negotiation without this crate
// needing its own TLS stack -- see ../vendor/openscreen/PATCHES.md.
#ifndef BREADCAST_CASTSTREAM_MESSAGE_PORT_BRIDGE_H_
#define BREADCAST_CASTSTREAM_MESSAGE_PORT_BRIDGE_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "cast/common/public/message_port.h"

namespace breadcast_caststream {

// Matches the PostMessageFn typedef in facade.h.
using PostMessageCallback = void (*)(void* user_data,
                                     const char* destination_id,
                                     size_t destination_id_len,
                                     const char* message_namespace,
                                     size_t message_namespace_len,
                                     const char* message,
                                     size_t message_len);

class MessagePortBridge final : public openscreen::cast::MessagePort {
 public:
  MessagePortBridge(void* user_data, PostMessageCallback post_message);
  ~MessagePortBridge() override;

  // Called by facade.cc's FFI entry point when Rust has received a message
  // on the webrtc namespace for us. Safe to call from any thread; the
  // caller is responsible for making sure this only actually touches
  // `client_` while running on the Environment's TaskRunner thread (facade.cc
  // marshals this via TaskRunner::PostTask before calling here).
  void DeliverMessage(const std::string& source_id,
                      const std::string& message_namespace,
                      const std::string& message);

  // openscreen::cast::MessagePort implementation.
  void SetClient(Client& client) override;
  void ResetClient() override;
  void PostMessage(const std::string& destination_id,
                   const std::string& message_namespace,
                   const std::string& message) override;

 private:
  void* const user_data_;
  const PostMessageCallback post_message_;
  Client* client_ = nullptr;
};

}  // namespace breadcast_caststream

#endif  // BREADCAST_CASTSTREAM_MESSAGE_PORT_BRIDGE_H_
