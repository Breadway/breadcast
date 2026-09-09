# breadcast: bread event integration

breadcast is a standalone screen-mirroring app: it works exactly the same
with or without `breadd` running. When breadd *is* present, `breadcastd`
publishes events into the shared bread automation fabric and listens for a
small set of commands. See the parent `bread` repo's `Documentation.md`,
specifically its "Namespaces" and "Integrating a bread\* app" sections, for
the general convention this follows.

App id: **`cast`**. Transport: `bread-utils`'s `bread_client` module
(feature `bread-client`); `breadcastd` links it directly, since it's a
long-running process for both the command-subscription half and the
discovery-driven emit half.

## Events published (`bread.cast.*`)

| Event | Data | When |
|-------|------|------|
| `bread.cast.device_found` | `{ "id": "<mdns id or DLNA description URL>", "name": "<friendly name>", "model": "<model>", "protocol": "cast" \| "dlna" }` | A Chromecast/Google TV (mDNS) or DLNA/UPnP media renderer (SSDP) is discovered, or an already-known device changes (new host, new name). Re-resolutions that carry the same payload are **not** re-emitted: treat it as an upsert keyed by `id`, not an append-only log. `id` is protocol-specific and only unique *within* a protocol; a Cast device's mDNS id and a DLNA device's description URL share no namespace. |
| `bread.cast.mirroring_started` | `{ "device_id": "<id>", "device_name": "<name>", "protocol": "cast" \| "dlna" }` | A mirroring session successfully started, whether triggered by `bread.command.cast.start`, the `breadcast` GTK popup, or (once wired) any other IPC client. |
| `bread.cast.mirroring_stopped` | `{}` | A mirroring session ended, whether via an explicit stop (command, IPC, or GTK popup) or unprompted (the portal picker's "stop sharing", the receiver dropping the connection, a DLNA renderer stopping playback from its own remote). There is no separate "stopped by whom" distinction in this event; `breadcastd`'s own logs have that detail if needed. |
| `bread.cast.mirroring_failed` | `{ "device_id": "<id>", "error": "<message>" }` | A `start_cast`/`bread.command.cast.start` attempt failed before a session was established (device unreachable, portal capture denied, negotiation timeout, renderer rejected the stream, etc). |

## Commands honored (`bread.command.cast.*`)

| Command | Data | Effect |
|---------|------|--------|
| `bread.command.cast.start` | `{ "device_id": "<id>" }` | Starts mirroring to the given device (looked up across both the Cast and DLNA device lists, same `device_id` a `list_devices`/`device_list_changed` payload reports). Fire-and-forget: the outcome shows up as `bread.cast.mirroring_started`/`.failed`, not a reply to this command. No-ops (logged) if already casting. |
| `bread.command.cast.stop` | none | Stops the active mirroring session, if any. No-ops if already idle. |

Both commands are handled identically regardless of whether the session was
started from here, the `breadcast` GTK popup, or the IPC socket directly;
`breadcastd`'s daemon actor has exactly one notion of "the active session"
(see `breadcastd/src/daemon.rs`), not one per control surface.

## Fail-safe behavior

- If breadd isn't installed or isn't running, `emit` is a silent no-op
  (`BreadClient::emit` never blocks or errors the caller) and the
  command subscription simply never receives anything: breadcastd's actual
  discovery functionality is entirely unaffected either way.
- If breadd restarts, the command subscription reconnects automatically
  (`BreadClient::subscribe`'s background thread has its own backoff loop);
  no restart of breadcastd is needed.
