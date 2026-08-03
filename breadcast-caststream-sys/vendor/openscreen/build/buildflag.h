// Minimal stand-in for Chromium's generated build/buildflag.h.
//
// The real header is produced per-target by GN's buildflag_header() rule,
// which generates a `BUILDFLAG_<FLAG>()` macro per flag; BUILDFLAG(flag)
// then token-pastes "BUILDFLAG_" with the flag name and calls it. Since this
// vendored subset is compiled directly with the `cc` crate rather than
// GN/Ninja, build_config.h defines those BUILDFLAG_<FLAG>() macros directly
// for the Linux target breadcast always builds for. Not part of upstream
// openscreen.
#ifndef BUILD_BUILDFLAG_H_
#define BUILD_BUILDFLAG_H_

#define BUILDFLAG_CAT_INDIRECT(a, b) a##b
#define BUILDFLAG_CAT(a, b) BUILDFLAG_CAT_INDIRECT(a, b)
#define BUILDFLAG(flag) (BUILDFLAG_CAT(BUILDFLAG_, flag)())

#endif  // BUILD_BUILDFLAG_H_
