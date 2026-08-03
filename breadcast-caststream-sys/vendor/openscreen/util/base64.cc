// LOCAL PATCH (breadcast): upstream implements this on top of
// third_party/modp_b64, which isn't fetched by a plain shallow clone of
// openscreen (it's pulled in separately via gclient/DEPS). This is a
// same-interface reimplementation on top of OpenSSL's EVP_Encode/DecodeBlock
// instead, since system OpenSSL is already a build dependency here. See
// vendor/openscreen/PATCHES.md.
#include "util/base64.h"

#include <openssl/evp.h>

#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace openscreen::base64 {

std::string Encode(ByteView input) {
  return Encode(std::string_view(reinterpret_cast<const char*>(input.data()),
                                 input.size()));
}

std::string Encode(std::string_view input) {
  const auto* data = reinterpret_cast<const unsigned char*>(input.data());
  // EVP_EncodeBlock's output is 4*ceil(n/3) bytes plus a NUL terminator it
  // writes but doesn't count in the returned length.
  std::string out((4 * ((input.size() + 2) / 3)) + 1, '\0');
  const int output_size = EVP_EncodeBlock(
      reinterpret_cast<unsigned char*>(out.data()), data,
      static_cast<int>(input.size()));
  out.resize(static_cast<size_t>(output_size));
  return out;
}

bool Decode(std::string_view input, std::vector<uint8_t>* output) {
  if (input.size() % 4 != 0) {
    return false;
  }
  std::vector<uint8_t> out((input.size() / 4) * 3);
  if (!out.empty()) {
    const int decoded_size = EVP_DecodeBlock(
        out.data(), reinterpret_cast<const unsigned char*>(input.data()),
        static_cast<int>(input.size()));
    if (decoded_size < 0) {
      return false;
    }
    // EVP_DecodeBlock doesn't strip padding from the output size -- trim the
    // 1-2 bytes corresponding to trailing '=' padding characters, matching
    // the caller-visible behavior of a normal base64 decoder.
    size_t padding = 0;
    if (input.size() >= 2) {
      if (input[input.size() - 1] == '=') {
        ++padding;
      }
      if (input[input.size() - 2] == '=') {
        ++padding;
      }
    }
    out.resize(static_cast<size_t>(decoded_size) - padding);
  }
  *output = std::move(out);
  return true;
}

}  // namespace openscreen::base64
