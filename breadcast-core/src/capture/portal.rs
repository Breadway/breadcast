use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::PathBuf;

use anyhow::{Context, Result};
use ashpd::desktop::PersistMode;
use ashpd::desktop::Session;
use ashpd::desktop::screencast::{CursorMode, Screencast, SelectSourcesOptions, SourceType};

/// Where the portal's restore token is cached, so re-mirroring doesn't
/// require re-clicking the system picker every single time — the token
/// (opaque to us) is what lets a later `SelectSources` call skip straight
/// to "yes, the same source as last time" instead of prompting again.
///
/// Filters out an *empty* `XDG_CACHE_HOME` in addition to an unset one —
/// some environments export it but leave it blank, which would otherwise
/// resolve to a relative path against the current working directory.
fn restore_token_path() -> PathBuf {
    let base = std::env::var("XDG_CACHE_HOME")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string())).join(".cache")
        });
    base.join("breadcast").join("portal-restore-token")
}

/// A live `xdg-desktop-portal` ScreenCast session. Keeping this alive keeps
/// the underlying PipeWire stream(s) open; dropping it without calling
/// [`CaptureSession::close`] leaves the portal to notice the D-Bus
/// connection went away rather than an explicit teardown.
pub struct CaptureSession {
    session: Session<Screencast>,
    video_node_id: u32,
}

impl CaptureSession {
    /// Opens the portal's screen-cast picker (monitor/window selection is
    /// the portal's own native UI — see `xdg-desktop-portal-hyprland`'s own
    /// picker dialog — not anything breadcast draws itself) and returns a
    /// session bound to whatever the user picked.
    ///
    /// `CursorMode::Embedded` bakes the cursor into the captured frames,
    /// which is what you want for "mirror my screen" (as opposed to
    /// `Metadata`, meant for apps that composite their own cursor).
    pub async fn start() -> Result<Self> {
        let proxy = Screencast::new()
            .await
            .context("failed to connect to the ScreenCast portal (is xdg-desktop-portal running?)")?;

        let session = proxy
            .create_session(Default::default())
            .await
            .context("failed to create a portal screencast session")?;

        // Everything past this point can fail (denied/cancelled picker, no
        // streams, etc.) — ashpd's `Session` has no `Drop` impl, so on any
        // of those paths the session would otherwise leak for the rest of
        // this process's life (and in a long-running daemon, potentially a
        // leaked PipeWire node per cancelled picker). Route every error
        // through an explicit close instead of an early `?` return.
        match Self::negotiate(&proxy, &session).await {
            Ok(video_node_id) => Ok(Self { session, video_node_id }),
            Err(e) => {
                let _ = session.close().await;
                Err(e)
            }
        }
    }

    async fn negotiate(proxy: &Screencast, session: &Session<Screencast>) -> Result<u32> {
        let token_path = restore_token_path();
        let existing_token = std::fs::read_to_string(&token_path).ok();

        let mut select_options = SelectSourcesOptions::default()
            .set_cursor_mode(CursorMode::Embedded)
            .set_sources(SourceType::Monitor | SourceType::Window)
            .set_multiple(false)
            .set_persist_mode(PersistMode::ExplicitlyRevoked);
        if let Some(token) = existing_token.as_deref() {
            select_options = select_options.set_restore_token(token);
        }

        proxy
            .select_sources(session, select_options)
            .await
            .context("failed to send SelectSources to the portal")?
            .response()
            .context("SelectSources request was denied or cancelled")?;

        let response = proxy
            .start(session, None, Default::default())
            .await
            .context("failed to send Start to the portal")?
            .response()
            .context("screen cast was cancelled (user closed the portal picker)")?;

        // Persist whatever token came back so the *next* start() can skip
        // the picker. A stale/invalid token is not a failure mode to guard
        // against here — the portal falls back to prompting again on its
        // own if the token no longer resolves to a valid grant.
        //
        // The token is a no-prompt capability to re-open a screen capture
        // of this user's session, so it's written 0600 in a 0700 directory
        // rather than relying on umask — any other local user being able to
        // read it would let them silently re-grant themselves the same
        // capture access.
        if let Some(token) = response.restore_token() {
            if let Some(parent) = token_path.parent() {
                let _ = std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent);
            }
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(&token_path)
            {
                use std::io::Write;
                let _ = file.write_all(token.as_bytes());
            }
        }

        let stream = response
            .streams()
            .first()
            .context("portal returned zero streams")?;
        Ok(stream.pipe_wire_node_id())
    }

    /// The PipeWire node id for the selected video source — this is what
    /// gets passed to GStreamer's `pipewiresrc path=<id>`.
    pub fn video_node_id(&self) -> u32 {
        self.video_node_id
    }

    /// Explicitly closes the portal session, ending the PipeWire stream.
    pub async fn close(self) -> Result<()> {
        self.session
            .close()
            .await
            .context("failed to close the portal screencast session")
    }
}
