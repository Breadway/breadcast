// Force-included (via -include, see ../build.rs) into every translation
// unit in this vendored openscreen subset. Papers over a few places where
// upstream code either relies on a BoringSSL-only API that system OpenSSL
// doesn't expose, or was written assuming an include that some other header
// in a full Chromium checkout happens to pull in transitively. Not part of
// upstream openscreen -- see ../PATCHES.md.
#ifndef PATCHES_COMPAT_SHIMS_H_
#define PATCHES_COMPAT_SHIMS_H_

// cast/streaming/impl/frame_crypto.cc uses strlen-family functions without
// including <cstring> itself.
#include <cstring>

#include <openssl/aes.h>

// CRYPTO_library_init() was an OpenSSL 1.0.x-era macro/no-op that BoringSSL
// still defines for source compatibility; system OpenSSL 3.x has no such
// symbol (initialization there is automatic). frame_crypto.cc's call to it
// is a no-op on any OpenSSL version this new, so it's shimmed out entirely.
#define CRYPTO_library_init() ((void)0)

// AES_ctr128_encrypt() is a BoringSSL convenience wrapper around AES-CTR
// that system OpenSSL's public headers don't expose. Implemented in
// compat_shims.cc using the standard CTR-mode algorithm (NIST SP 800-38A,
// big-endian 128-bit counter block) over AES_encrypt(), which OpenSSL does
// still expose.
extern "C" void AES_ctr128_encrypt(const unsigned char* in,
                                   unsigned char* out,
                                   size_t length,
                                   const AES_KEY* key,
                                   unsigned char ivec[AES_BLOCK_SIZE],
                                   unsigned char ecount_buf[AES_BLOCK_SIZE],
                                   unsigned int* num);

#endif  // PATCHES_COMPAT_SHIMS_H_
