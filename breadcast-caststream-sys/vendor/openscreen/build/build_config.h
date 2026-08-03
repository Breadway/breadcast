// Minimal stand-in for Chromium's build/build_config.h, providing just the
// BUILDFLAG(IS_*) values this vendored subset of openscreen actually checks
// (see ip_address.cc, logging_posix.cc, udp_socket_posix.cc). breadcast only
// ever builds this for Linux, so everything else is hardcoded off. Not part
// of upstream openscreen.
#ifndef BUILD_BUILD_CONFIG_H_
#define BUILD_BUILD_CONFIG_H_

#include "build/buildflag.h"

#define BUILDFLAG_IS_LINUX() (1)
#define BUILDFLAG_IS_POSIX() (1)
#define BUILDFLAG_IS_APPLE() (0)
#define BUILDFLAG_IS_ANDROID() (0)
#define BUILDFLAG_IS_WIN() (0)
#define BUILDFLAG_IS_CHROMEOS() (0)
#define BUILDFLAG_IS_IOS() (0)

#endif  // BUILD_BUILD_CONFIG_H_
