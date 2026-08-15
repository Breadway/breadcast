# breadcast

Cast your screen to a Chromecast, Google TV, or DLNA renderer on the LAN.
Two binaries:

- **`breadcastd`** — background daemon. Discovers Cast (mDNS) and DLNA/UPnP
  (SSDP) devices, owns the portal screen-capture / encode pipeline, and
  runs the live session: Cast Streaming (vendored openscreen) or
  DLNA/AVTransport. One active session at a time.
- **`breadcast`** — GTK4 Layer Shell popup. Thin IPC client of
  `breadcastd`: pick a device, start or stop a cast. Closing the popup
  does not interrupt an active session.

This is a **bakery product**. It is **not** shipped on the BOS ISO and is
**not** part of the default desktop — install it yourself if you want it.

## Install

```sh
bakery install breadcast
```

That puts `breadcast` and `breadcastd` on `$PATH` (usually `~/.local/bin`),
installs `contrib/breadcastd.service` as a systemd user unit, enables it,
and starts the daemon. `bakery doctor breadcast` checks the system
packages listed in `bakery.toml` first.

## Requirements

- A Wayland compositor with Layer Shell and `xdg-desktop-portal`
  ScreenCast (Hyprland is the primary target)
- GTK 4.12+ and `gtk4-layer-shell`
- GStreamer plus `gst-plugin-pipewire`, `gst-plugins-bad`,
  `gst-plugin-va` (`vah264enc`), and `gst-plugin-hlssink3`
- `jsoncpp` and `openssl` (runtime deps of the vendored Cast Streaming
  code)

From source you also need a Rust toolchain (edition 2021).

## Build from source

```sh
git clone https://git.breadway.dev/Breadway/breadcast
cd breadcast
cargo build --release
```

Binaries land at `target/release/breadcast` and `target/release/breadcastd`.

```sh
cp target/release/breadcast target/release/breadcastd ~/.local/bin/
cp contrib/breadcastd.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now breadcastd
```

## Usage

Start the daemon (or let the systemd unit handle it):

```sh
breadcastd
```

Open the device picker:

```sh
breadcast
```

Running `breadcast` again while it is open closes it (toggle). Click a
device to start mirroring — the portal picker asks which screen to share.
**Stop mirroring** ends the session. Status is Idle or Casting.

Discovery examples (optional, for debugging the LAN):

```sh
cargo run -p breadcast-core --example discover
cargo run -p breadcast-core --example dlna_discover
```

### Hyprland keybind

On stock Hyprland, add the contents of `contrib/hyprland.conf` to your
`hyprland.conf` if you want Super+C and the frosted-glass blur:

```
layerrule = blur, breadcast
layerrule = ignorezero, breadcast

bind = $mainMod, C, exec, breadcast
```

BOS does not ship this app or a default keybind for it. Add one yourself
if you install breadcast on a BOS machine.

## bread event integration

`breadcastd` works the same with or without `breadd`. When `breadd` is
running, it publishes `bread.cast.*` and honors `bread.command.cast.*`.
See [EVENTS.md](EVENTS.md) for the bus contract. `bread` is not a bakery
dependency.

## Theming

`breadcast` inherits its colour palette from `bread-theme`.
