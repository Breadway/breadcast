#include "patches/compat_shims.h"

void AES_ctr128_encrypt(const unsigned char* in,
                        unsigned char* out,
                        size_t length,
                        const AES_KEY* key,
                        unsigned char ivec[AES_BLOCK_SIZE],
                        unsigned char ecount_buf[AES_BLOCK_SIZE],
                        unsigned int* num) {
  unsigned int n = *num;
  size_t l = 0;
  while (l < length) {
    if (n == 0) {
      AES_encrypt(ivec, ecount_buf, key);
      for (int i = AES_BLOCK_SIZE - 1; i >= 0; --i) {
        if (++ivec[i]) {
          break;
        }
      }
    }
    out[l] = static_cast<unsigned char>(in[l] ^ ecount_buf[n]);
    ++l;
    n = (n + 1) % AES_BLOCK_SIZE;
  }
  *num = n;
}
