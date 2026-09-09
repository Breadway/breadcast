//! Message shapes for breadcastd's private control socket
//! (`$XDG_RUNTIME_DIR/breadcast/breadcastd.sock`, newline-delimited JSON):
//! shared between `breadcastd` (which serves them) and `breadcast` (the
//! GTK4 popup, which is the only client) so the two never drift out of
//! sync with each other. See `breadcastd/src/ipc.rs` for the actual
//! socket-handling code; this crate only holds the wire types, since it's
//! the one both binaries already depend on.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientRequest {
    pub id: u64,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerMessage {
    #[serde(rename = "response")]
    Response {
        id: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    #[serde(rename = "event")]
    Event { event: String, data: Value },
}

impl ServerMessage {
    pub fn ok(id: u64, result: Value) -> Self {
        Self::Response { id, result: Some(result), error: None }
    }

    pub fn err(id: u64, error: impl std::fmt::Display) -> Self {
        Self::Response { id, result: None, error: Some(error.to_string()) }
    }
}

/// Which casting protocol a [`DeviceInfo`]/[`StateInfo::Casting`] refers to:
/// Cast V2/Cast Streaming ([`crate::cast_sender`]/[`crate::caststream`])
/// or DLNA/UPnP AVTransport ([`crate::dlna`]). The two protocols discover
/// disjoint device populations (see `dlna/mod.rs`'s doc comment), so a
/// unified device list needs this to tell them apart: a Cast device id and
/// a DLNA device url share no namespace, but both are opaque strings to the
/// GTK client, which otherwise has no way to know which `start_cast`
/// dispatch path it's picking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    Cast,
    Dlna,
}

/// A discovered device as reported by `"list_devices"`/`"device_list_changed"`,
/// a protocol-agnostic projection of either a [`crate::CastDevice`] (`id`
/// is its mDNS id) or a [`crate::dlna::DlnaDevice`] (`id` is its description URL,
/// the closest thing DLNA has to a stable identifier; see
/// `dlna/device.rs`'s doc comment on [`crate::dlna::DlnaDevice::url`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    pub id: String,
    pub name: String,
    pub model: String,
    pub protocol: Protocol,
}

/// The daemon's current activity: pushed as a `"state_changed"` event and
/// returned by the `"get_state"` request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StateInfo {
    Idle,
    Casting { device_id: String, device_name: String, protocol: Protocol },
}

/// Returns `$XDG_RUNTIME_DIR/breadcast/breadcastd.sock`.
pub fn socket_path() -> anyhow::Result<std::path::PathBuf> {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR").map_err(|_| anyhow::anyhow!("XDG_RUNTIME_DIR is not set"))?;
    Ok(std::path::Path::new(&runtime_dir).join("breadcast").join("breadcastd.sock"))
}
