# breadcast

Cast your screen to any Chromecast/Google TV, for Hyprland. It consists of
two binaries:

- **`breadcastd`** — a background daemon that discovers Cast devices on the
  LAN and (once built — see Status) owns the screen-capture/encode/serve
  pipeline and the live Cast V2 session.
- **`breadcast`** — a GTK4 Layer Shell popup for picking a device and
  starting/stopping a cast.

## Status

This is early: device discovery (mDNS) and the Cast V2 sender (real device
control — connect, launch the receiver, load media) are built and validated
against real Chromecast/Google TV hardware. The actual screen-mirroring
pipeline (portal-based screen capture → GPU-accelerated encode → HLS →
local HTTP server) and the GTK4 device-picker UI are not built yet.
`breadcastd` today only does discovery and optional breadd event
publishing; `breadcast` is a stub binary. See `CLAUDE.md`'s Status section
for more detail.

## Requirements

- Rust toolchain (edition 2021)
- GTK 4.12+ and `gtk4-layer-shell` (once the UI lands)
- GStreamer + `gst-plugin-pipewire`, `gst-plugins-bad` (VA-API `va` plugin),
  `gst-plugin-hlssink3` (once the capture pipeline lands)
- Hyprland (or any Wayland compositor with Layer Shell and the
  `xdg-desktop-portal` ScreenCast interface)

## Build

```sh
git clone https://git.breadway.dev/breadway/breadcast
cd breadcast
cargo build --release
```

The compiled binaries are at `target/release/breadcast` and
`target/release/breadcastd`.

## Try device discovery today

```sh
cargo run -p breadcast-core --example discover
```

Prints Chromecast/Google TV devices as they appear/disappear on the LAN.

## Install

Copy the binaries to somewhere on your `$PATH`, e.g.:

```sh
cp target/release/breadcast target/release/breadcastd ~/.local/bin/
```

### systemd user service

A unit file is provided in `contrib/`:

```sh
cp contrib/breadcastd.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now breadcastd
```

### Hyprland keybind

For stock Hyprland, add the contents of `contrib/hyprland.conf` to your
`hyprland.conf`:

```
layerrule = blur, breadcast
layerrule = ignorezero, breadcast

bind = $mainMod, C, exec, breadcast
```

On BOS, Hyprland config is Lua+JSON-driven instead — see
`contrib/binds.json` for the equivalent keybind entry to merge into your
`binds.json` by hand (there's no per-app self-registration mechanism yet).
There is currently no BOS-native equivalent for the `layerrule` blur lines.

## bread event integration

`breadcastd` optionally publishes into the shared bread automation fabric
(`bread.cast.*`) when `breadd` is running, and works identically without
it. See `EVENTS.md` for the full, honest-about-scope contract — most of the
eventually-planned events (mirroring start/stop) aren't implemented yet,
since the mirroring pipeline itself isn't built yet.

## Theming

`breadcast` will inherit its colour palette from `bread-theme`, matching
the rest of the ecosystem, once its GTK4 UI is built.
