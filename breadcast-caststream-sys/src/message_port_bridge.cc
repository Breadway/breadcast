#include "message_port_bridge.h"

namespace breadcast_caststream {

MessagePortBridge::MessagePortBridge(void* user_data,
                                     PostMessageCallback post_message)
    : user_data_(user_data), post_message_(post_message) {}

MessagePortBridge::~MessagePortBridge() = default;

void MessagePortBridge::DeliverMessage(const std::string& source_id,
                                       const std::string& message_namespace,
                                       const std::string& message) {
  if (client_) {
    client_->OnMessage(source_id, message_namespace, message);
  }
}

void MessagePortBridge::SetClient(Client& client) {
  client_ = &client;
}

void MessagePortBridge::ResetClient() {
  client_ = nullptr;
}

void MessagePortBridge::PostMessage(const std::string& destination_id,
                                    const std::string& message_namespace,
                                    const std::string& message) {
  post_message_(user_data_, destination_id.data(), destination_id.size(),
               message_namespace.data(), message_namespace.size(),
               message.data(), message.size());
}

}  // namespace breadcast_caststream
