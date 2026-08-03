// Copyright 2019 The Chromium Authors
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "util/crypto/openssl_util.h"

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <stddef.h>
#include <stdint.h>

#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include "util/osp_logging.h"

namespace openscreen {

namespace {

// Callback routine for OpenSSL to print error messages. `str` is a
// nullptr-terminated string of length `len` containing diagnostic information
// such as the library, function and reason for the error, the file and line
// where the error originated, plus potentially any context-specific
// information about the error. `context` contains a pointer to user-supplied
// data, which is currently unused.
// If this callback returns a value <= 0, OpenSSL will stop processing the
// error queue and return, otherwise it will continue calling this function
// until all errors have been removed from the queue.
int OpenSSLErrorCallback(const char* str, size_t len, void* context) {
  OSP_DVLOG << "\t" << std::string_view(str, len);
  return 1;
}

}  // namespace

void EnsureOpenSSLInit() {
  // LOCAL PATCH (breadcast): upstream calls OPENSSL_init_ssl() here, but
  // this vendored subset never touches libssl (no TLS -- see
  // ../../../PATCHES.md), so this just does the general libcrypto init
  // instead. Safe to call repeatedly; OpenSSL 3.x makes this optional
  // anyway, but frame_crypto.cc's key setup wants it done up front.
  OPENSSL_init_crypto(0, nullptr);
}

void ClearOpenSSLERRStack(const Location& location) {
  if (OSP_DCHECK_IS_ON()) {
    uint32_t error_num = ERR_peek_error();
    if (error_num == 0) {
      return;
    }

    OSP_DVLOG << "OpenSSL ERR_get_error stack from " << location;
    ERR_print_errors_cb(&OpenSSLErrorCallback, nullptr);
  } else {
    ERR_clear_error();
  }
}

}  // namespace openscreen
