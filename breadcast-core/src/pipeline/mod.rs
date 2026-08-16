use std::io::{Read as _, Write as _};
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;

use crate::caststream::VideoParams;

/// Builds (but doesn't start) the capture → encode → mux → HLS pipeline for
/// a single video source. `output_dir` is created if it doesn't exist;
/// `hlssink3` writes `segment%05d.ts` files and `playlist.m3u8` there.
///
/// Idempotently calls `gst::init()` itself rather than requiring every
/// caller to remember to — `gst::parse::launch` panics
/// (`assert_initialized_main_thread!()`) if GStreamer was never
/// initialized, which every current caller happens to do first, but that's
/// a footgun for a `pub` function once something other than a smoke-test
/// example calls it (e.g. `breadcastd`).
///
/// Uses a `gst::parse::launch` string rather than the typed element-builder
/// API — this is the prototyping-first approach: get the pipeline shape
/// right and provable against real hardware before hardening it into typed
/// Rust with per-element error handling. Filesystem paths are deliberately
/// *not* interpolated into that string, though: `output_dir` is caller
/// (eventually user-facing) input, and a path containing `"`, `\`, or `!`
/// would either break `gst::parse::launch`'s own string syntax or inject
/// extra elements into the parsed graph. `hlssink3`'s `location`/
/// `playlist-location` are set as plain element properties after parsing
/// instead, which need no escaping at all.
///
/// `vah264enc` (not the deprecated `vaapih264enc`) needs `gst-plugin-va`
/// installed (`pacman -S gst-plugin-va`) — it's a separate Arch package
/// from `gst-plugins-bad` itself, not bundled in. `hlssink3` similarly
/// needs `gst-plugin-hlssink3`. `hlssink3` (not `hlscmafsink`) is
/// deliberate: the Chromecast Default Media Receiver only plays classic
/// MPEG-TS-segmented HLS, not fMP4/CMAF — and `hlssink3` does its own
/// internal MPEG-TS muxing per segment via its `video`/`audio` *request*
/// pads, so no separate `mpegtsmux` element goes in front of it (confirmed
/// via `gst-inspect-1.0 hlssink3`: its only pad templates are `video` and
/// `audio`, not a generic always-available `sink`).
pub fn build_video_pipeline(video_node_id: u32, output_dir: &Path) -> Result<gst::Pipeline> {
    gst::init().context("failed to initialize GStreamer")?;

    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create HLS output dir {}", output_dir.display()))?;

    let segment_pattern = output_dir.join("segment%05d.ts");
    let playlist_path = output_dir.join("playlist.m3u8");

    // Capped to 1280x720@30 and H.264 Main profile: this machine's native
    // 1920x1200 at an uncapped framerate (observed via ffprobe as a
    // nonsensical 120fps/240tbr — pipewiresrc doesn't cap the rate on its
    // own) was confirmed via a real Chromecast to fetch fine over HTTP
    // (200s on the playlist and first segment) but then fail to actually
    // play — consistent with exceeding what an older Chromecast's H.264
    // decoder profile/level supports, not a network/CORS/HLS-structure
    // problem. 720p30 Main is a conservative, broadly-compatible baseline;
    // revisit upward (1080p, High profile) once a specific device's real
    // ceiling is known. Note this ignores the source's 16:10 aspect ratio
    // (stretches to 16:9) — correctness/compatibility first, an
    // aspect-preserving scale (letterbox via `videoscale
    // add-borders=true`) is a follow-up, not a blocker.
    // HLS segment sizing is *the* dominant term in this path's end-to-end
    // latency, and the two knobs are coupled: a segment can only be cut on a
    // key frame, so the real segment duration is `max(target-duration,
    // GOP length)` no matter what `target-duration` says. With the previous
    // `target-duration=2` + `key-int-max=60` (60 frames / 30fps = a 2s GOP),
    // segments were 2s, and a renderer that buffers the customary three of
    // them before starting playback sits ~6s behind live -- on top of
    // however much of the playlist it decides to start from. `dlna_mirror.rs`
    // then waited for 3 segments to exist before even handing over the URL,
    // adding another ~6s of already-stale content.
    //
    // 1s segments (GOP dropped to 30 frames to make that actually
    // achievable) roughly halve that. Going below 1s is not worth it here:
    // MPEG-TS + a per-segment key frame means shorter segments cost real
    // bitrate, and classic (non-LL) HLS clients don't reliably honour
    // sub-second target durations anyway. Genuinely low latency on this path
    // needs LL-HLS, which `hlssink3` does not implement -- the Cast
    // Streaming path (`build_video_pipeline_for_streaming`) is the
    // low-latency answer, and this one is the compatibility answer.
    //
    // `playlist-length`/`max-files` shrink to match so the playlist doesn't
    // advertise a long backlog of stale segments for a client to start from.
    let pipeline_str = format!(
        "pipewiresrc path={video_node_id} do-timestamp=true ! \
         videoconvert ! videoscale ! videorate ! \
         video/x-raw,format=NV12,width=1280,height=720,framerate=30/1 ! \
         vah264enc bitrate=4000 key-int-max=30 rate-control=cbr ! \
         video/x-h264,profile=main ! \
         h264parse config-interval=1 ! \
         hlssink.video \
         hlssink3 name=hlssink target-duration=1 playlist-length=3 max-files=6"
    );

    let element = gst::parse::launch(&pipeline_str).context("failed to parse GStreamer pipeline")?;
    let Ok(pipeline) = element.downcast::<gst::Pipeline>() else {
        bail!("parsed GStreamer graph was not a top-level Pipeline");
    };

    let hlssink = pipeline
        .by_name("hlssink")
        .context("parsed pipeline has no element named 'hlssink'")?;
    hlssink.set_property(
        "location",
        segment_pattern.to_str().context("HLS segment path is not valid UTF-8")?,
    );
    hlssink.set_property(
        "playlist-location",
        playlist_path.to_str().context("HLS playlist path is not valid UTF-8")?,
    );

    Ok(pipeline)
}

/// Builds (but doesn't start) the capture → encode → `appsink` pipeline used
/// for low-latency Cast Streaming mirroring (see [`crate::caststream`]) —
/// the counterpart to [`build_video_pipeline`]'s HLS path, which the
/// Chromecast Mirroring receiver can't play (it speaks RTP, not HLS).
///
/// Differs from the HLS pipeline in exactly the ways that matter for
/// feeding openscreen's `Sender::EnqueueFrame`, which wants standalone,
/// receiver-decodable Annex-B access units, not a muxed container:
/// - `h264parse config-interval=-1` re-inserts SPS/PPS before every key
///   frame (not just once) — required since there's no container-level
///   "here's the codec config" the receiver can fall back on, unlike HLS's
///   `.ts` segments.
/// - An explicit `video/x-h264,stream-format=byte-stream,alignment=au` caps
///   filter after `h264parse` — `vah264enc`'s default output is `avc`
///   (4-byte length-prefixed NAL units, the ISO/MP4 convention), but
///   RTP/Cast Streaming payloads need Annex-B (0x00 0x00 0x00 0x01 start
///   codes), the same format `h264parse` can produce but won't unless asked.
/// - `appsink` instead of `hlssink3`: each pulled `gst::Sample` is one
///   complete access unit (`alignment=au`), ready to hand to
///   `CastStreamSender::enqueue_frame` — see `cast_stream_test.rs` for the
///   pull loop. `sync=false` since these are being forwarded over the
///   network as fast as produced, not paced against a clock for local
///   playback; `drop=true`/`max-buffers=4` bounds memory if the pull loop
///   ever falls behind rather than growing an unbounded backlog.
///
/// Returns the pipeline plus its `appsink` and the `vah264enc` element (the
/// latter so a caller can drive its `bitrate` property from
/// `CastStreamSender::estimated_bandwidth_bps()` — see
/// [`request_key_frame`]/`set_video_bitrate_kbps` for the two knobs a
/// congestion-control loop needs).
/// Which capture-side memory path [`build_video_pipeline_for_streaming`] asks
/// PipeWire (and through it, the compositor's portal implementation) to hand
/// this pipeline. The two are not interchangeable: they have *different*
/// known failure modes on different compositor versions, which is why the
/// choice is made at runtime rather than baked into one pipeline string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CaptureBackend {
    /// Zero-copy: `pipewiresrc` hands DMA-BUFs straight to `vapostproc`,
    /// which hands VA-memory straight to `vah264enc`. Nothing ever touches
    /// system memory, and the portal never falls back to `wl_shm`.
    Dmabuf,
    /// `videoconvert ! videoscale ! videorate` on plain system memory, which
    /// makes PipeWire request `wl_shm` buffers from the portal. CPU-costly
    /// and stall-prone (see [`choose_capture_backend`]), but it does not go
    /// anywhere near a compositor's DMA-BUF render-into-client-buffer path.
    Shm,
}

/// First Hyprland release containing `renderer/rbo: avoid nullptr deref`
/// (upstream commit `ae1690c2`, PR #15167, 2026-06-18), tagged in v0.56.0 on
/// 2026-07-20.
///
/// Below this, asking Hyprland for a DMA-BUF screencast can **kill the
/// user's entire compositor session**, and there is nothing this pipeline
/// can do about it from the client side. The mechanism, confirmed against a
/// real coredump on v0.55.4 plus upstream's own source:
/// `Screenshare::CScreenshareFrame::copyDmabuf()` renders the monitor into
/// the client-supplied DMA-BUF via `IHyprRenderer::beginRender` ->
/// `getOrCreateRenderbuffer`. If `CGLRenderbuffer`'s constructor fails to
/// import the buffer (`createEGLImage` returns `EGL_NO_IMAGE_KHR`) it
/// early-returns leaving `m_framebuffer` null -- and v0.55.4's destructor
/// then unconditionally does `unbind(); m_framebuffer->release();` on that
/// null pointer while the failed renderbuffer is being torn down. The
/// resulting abort takes down Hyprland, every window, and (separately,
/// same instant) `xdg-desktop-portal-hyprland`. Upstream's fix is a one-line
/// `if (m_framebuffer)` guard; it converts the abort into a dropped frame.
///
/// Reported upstream at least three times (hyprwm/Hyprland #13487, #13543,
/// #13653, all v0.54.x, all on AMD) and auto-closed unread by the
/// issues-are-disabled bot rather than triaged, so the "is it fixed?"
/// question can only be answered from the commit log, not the tracker. The
/// reported triggers (window-group tab switching, touchpad gestures, an
/// emulator in a Discord stream) have nothing in common with each other or
/// with resolution -- treat the import failure as intermittent, not as
/// something a particular capture geometry or DRM modifier provokes.
///
/// Note in particular that this is *not* avoidable by requesting a
/// "simpler" buffer layout. `vapostproc` advertises exactly one AMD DRM
/// modifier on its `video/x-raw(memory:DMABuf)` pads -- `0x0200000008401b04`
/// = GFX11, 64K_R_X tiling, `DCC=0` (verified with `gst-inspect-1.0
/// vapostproc` and `drm_fourcc.h`'s field shifts). It offers no LINEAR
/// alternative, and the one modifier it does offer is already uncompressed,
/// so there is no tiling/compression hazard left to negotiate away.
const HYPRLAND_MIN_SAFE_DMABUF: (u32, u32, u32) = (0, 56, 0);

/// Reads the running Hyprland's version over its own IPC socket (the same
/// `j/version` request `hyprctl version -j` makes) without spawning
/// `hyprctl`, which needn't be installed. `None` if this isn't a Hyprland
/// session at all, or if the version can't be determined.
fn hyprland_version() -> Option<(u32, u32, u32)> {
    let signature = std::env::var("HYPRLAND_INSTANCE_SIGNATURE").ok()?;
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR").ok()?;

    let mut socket = UnixStream::connect(format!("{runtime_dir}/hypr/{signature}/.socket.sock")).ok()?;
    // Bounded on both halves: this runs on the way into starting a mirror
    // session, and a wedged compositor must not be able to hang that.
    socket.set_write_timeout(Some(Duration::from_secs(1))).ok()?;
    socket.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    socket.write_all(b"j/version").ok()?;

    let mut response = String::new();
    socket.read_to_string(&mut response).ok()?;
    let parsed: serde_json::Value = serde_json::from_str(&response).ok()?;

    // `version` is the plain "0.55.4"; `tag` is "v0.55.4" and is what older
    // Hyprlands report, so accept either.
    let raw = parsed.get("version").or_else(|| parsed.get("tag"))?.as_str()?;
    parse_hyprland_version(raw)
}

/// Splits a Hyprland version string into comparable components. Strips a
/// leading `v` (`tag` carries one, `version` doesn't) and anything from the
/// first `-` (a git build's tag looks like `v0.55.4-123-gdeadbee`).
fn parse_hyprland_version(raw: &str) -> Option<(u32, u32, u32)> {
    let mut parts = raw.trim().trim_start_matches('v').split('-').next()?.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    // A two-component "0.56" is treated as 0.56.0 rather than rejected --
    // erring toward *parsing* here is safe, since the comparison against
    // `HYPRLAND_MIN_SAFE_DMABUF` is what decides anything.
    let patch = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor, patch))
}

/// Picks the capture path, trading two *different* real bugs off against
/// each other rather than pretending either one is hypothetical.
///
/// [`CaptureBackend::Dmabuf`] is the better path and the default: it is
/// genuinely zero-copy, and it sidesteps `xdg-desktop-portal-hyprland`'s
/// `wl_shm` stall entirely. That stall is not a hiccup -- it is terminal.
/// In xdpw's `src/portals/Screencopy.cpp`, when the PipeWire consumer is
/// holding every buffer, the portal logs "Out of buffers" and re-queues a
/// frame only while `copyRetries++ < MAX_RETRIES` (10); `copyRetries` is
/// reset to 0 *only* on a successful copy. So ten consecutive misses and
/// the portal stops requesting frames forever, without sending an error to
/// PipeWire -- which is exactly why a 45-second freeze showed up in
/// `journalctl` and nowhere on this pipeline's own GStreamer bus. (Worth
/// keeping in mind that "the consumer is holding every buffer" means the
/// stall can *originate* downstream: a brief encoder or RTP-send stall stops
/// buffers being recycled, and the portal's give-up logic then makes it
/// permanent. [`pull_encoded_frame`]'s watchdog is the backstop for both.)
///
/// But on Hyprland older than [`HYPRLAND_MIN_SAFE_DMABUF`] the DMA-BUF path
/// can abort the compositor outright, which is a categorically worse outcome
/// than a stalled cast -- so there, fall back to `wl_shm` and let the
/// watchdog bound the damage. Non-Hyprland sessions are unaffected by that
/// bug and keep DMA-BUF.
fn choose_capture_backend() -> CaptureBackend {
    let Some(version) = hyprland_version() else {
        // Either not Hyprland (so the Hyprland-specific crash can't apply),
        // or Hyprland with an unreadable version. The latter is the
        // ambiguous case; prefer the path that cannot take the desktop down.
        if std::env::var_os("HYPRLAND_INSTANCE_SIGNATURE").is_some() {
            tracing::warn!(
                "running under Hyprland but could not read its version; using the slower wl_shm \
                 capture path, since DMA-BUF screencast aborts the compositor before v{}.{}.{}",
                HYPRLAND_MIN_SAFE_DMABUF.0,
                HYPRLAND_MIN_SAFE_DMABUF.1,
                HYPRLAND_MIN_SAFE_DMABUF.2,
            );
            return CaptureBackend::Shm;
        }
        return CaptureBackend::Dmabuf;
    };

    if version < HYPRLAND_MIN_SAFE_DMABUF {
        tracing::warn!(
            hyprland = format!("{}.{}.{}", version.0, version.1, version.2),
            "this Hyprland predates the fix for the DMA-BUF screencast compositor crash \
             (upstream PR #15167, released in v0.56.0) -- falling back to the slower, \
             stall-prone wl_shm capture path. Updating Hyprland restores zero-copy capture."
        );
        return CaptureBackend::Shm;
    }

    CaptureBackend::Dmabuf
}

pub fn build_video_pipeline_for_streaming(
    video_node_id: u32,
) -> Result<(gst::Pipeline, gst_app::AppSink, gst::Element, VideoParams)> {
    gst::init().context("failed to initialize GStreamer")?;

    // 1920x1080@native-rate Main profile. Went 1080p -> 720p -> 1080p again
    // tonight: the first 1080p attempt froze near-instantly, but that had
    // nothing to do with resolution -- it was the `xdg-desktop-portal-hyprland`
    // wl_shm buffer-exhaustion bug below (real, structural, whatever the
    // resolution) compounded by openscreen's in-flight RTP budget being too
    // tight for this receiver's actual RTT (see `frame_chain_broken` in
    // `breadcast-caststream-sys/src/facade.cc`, and the playout-delay tuning
    // in `breadcast-caststream-sys/src/session.cc`). With both of those
    // fixed -- confirmed via a real, freeze-free 720p session -- the
    // packet-count increase 1080p brings back is no longer landing on an
    // already-struggling budget, so it's worth trying again on its own
    // merits.
    //
    // On the DMA-BUF path `pipewiresrc` deliberately does *not* go through
    // `videoconvert ! videoscale ! videorate ! video/x-raw,...` (plain
    // system-memory caps) -- doing so forces PipeWire to hand the
    // compositor's portal implementation a `wl_shm` (shared-memory) buffer
    // request, and on this system (`xdg-desktop-portal-hyprland`) that path
    // is real-world buggy: journalctl during a live freeze showed it
    // repeatedly logging "Asked for a wl_shm buffer which is legacy" / "Out
    // of buffers" / "Retrying screencopy" in a tight loop that never
    // actually delivered a frame -- multi-second (once 45+ second) stalls
    // with *zero* signal on breadcast's own GStreamer bus, since nothing
    // here was erroring, it was just starved waiting on a buffer the
    // portal's legacy path never produced. See `choose_capture_backend` for
    // why that stall is permanent rather than transient, and for the one
    // case where it's still the lesser evil.
    //
    // `vapostproc` (VA-API postprocessor -- confirmed present via
    // `gst-inspect-1.0 vapostproc`, ships in `gst-plugins-bad`'s `va`
    // plugin) accepts `video/x-raw(memory:DMABuf)` directly from
    // `pipewiresrc` and outputs `video/x-raw(memory:VAMemory)`, which
    // `vah264enc` also accepts natively -- a fully zero-copy DMA-BUF path
    // from portal to hardware encoder that never touches the legacy wl_shm
    // fallback at all. No `videorate` in this path: `vapostproc` is a
    // per-frame transform (scale/convert), not a temporal one, so it can't
    // do frame-rate reduction the way `videorate` does on raw memory --
    // frames flow at whatever rate PipeWire actually delivers rather than a
    // forced 30fps. This is fine for RTP: `facade.cc` derives RTP timestamps
    // from each frame's real capture time regardless of the nominal rate,
    // and openscreen's own frame pacing doesn't assume a fixed source rate
    // either. The returned `VideoParams` advertises 60 as a *ceiling*
    // (`max_frame_rate_*`), which stays truthful whether the compositor
    // actually delivers 60 or fewer; the wl_shm path's `videorate` does cap
    // hard, so it advertises the rate it really enforces.
    //
    // Both branches return the geometry they actually encode, and the caller
    // hands that straight to the Cast OFFER. That coupling is deliberate:
    // advertising a resolution other than what's really sent is a genuine
    // protocol mismatch this project has already been bitten by once, and
    // keeping two constants manually in sync across two files is how that
    // happened. Returning it makes the mismatch unrepresentable.
    let (pipeline_str, params) = match choose_capture_backend() {
        CaptureBackend::Dmabuf => (
            "pipewiresrc path=%VIDEO_NODE_ID% do-timestamp=true ! \
             video/x-raw(memory:DMABuf),format=DMA_DRM ! \
             vapostproc ! \
             video/x-raw(memory:VAMemory),format=NV12,width=1920,height=1080 ! \
             vah264enc name=venc bitrate=4000 key-int-max=60 rate-control=cbr ! \
             video/x-h264,profile=main ! \
             h264parse name=h264parse config-interval=-1 ! \
             video/x-h264,stream-format=byte-stream,alignment=au ! \
             appsink name=appsink emit-signals=false sync=false max-buffers=4 drop=true",
            VideoParams { width: 1920, height: 1080, max_frame_rate_numerator: 60, ..VideoParams::default() },
        ),
        // 720p30 rather than 1080p60 on this path on purpose: every frame is
        // a CPU convert + scale here, and CPU cost is precisely what makes
        // the portal's "Out of buffers" give-up more likely, since the
        // portal runs out exactly when the consumer is slow to recycle
        // buffers. The lighter shape is also what was last known to work on
        // real hardware before the DMA-BUF switch.
        CaptureBackend::Shm => (
            "pipewiresrc path=%VIDEO_NODE_ID% do-timestamp=true ! \
             videoconvert ! videoscale ! videorate ! \
             video/x-raw,format=NV12,width=1280,height=720,framerate=30/1 ! \
             vah264enc name=venc bitrate=4000 key-int-max=60 rate-control=cbr ! \
             video/x-h264,profile=main ! \
             h264parse name=h264parse config-interval=-1 ! \
             video/x-h264,stream-format=byte-stream,alignment=au ! \
             appsink name=appsink emit-signals=false sync=false max-buffers=4 drop=true",
            VideoParams { width: 1280, height: 720, max_frame_rate_numerator: 30, ..VideoParams::default() },
        ),
    };
    let pipeline_str = pipeline_str.replace("%VIDEO_NODE_ID%", &video_node_id.to_string());

    let element = gst::parse::launch(&pipeline_str).context("failed to parse GStreamer pipeline")?;
    let Ok(pipeline) = element.downcast::<gst::Pipeline>() else {
        bail!("parsed GStreamer graph was not a top-level Pipeline");
    };

    let appsink = pipeline
        .by_name("appsink")
        .context("parsed pipeline has no element named 'appsink'")?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow::anyhow!("'appsink' element was not a GstAppSink"))?;

    let encoder = pipeline.by_name("venc").context("parsed pipeline has no element named 'venc'")?;

    Ok((pipeline, appsink, encoder, params))
}

/// How long [`pull_encoded_frame`] waits per `try_pull_sample` call. Short
/// enough that a teardown from another thread is noticed promptly, long
/// enough not to spin.
const CAPTURE_STALL_POLL: Duration = Duration::from_millis(250);

/// How long [`pull_encoded_frame`] tolerates a *playing* pipeline producing
/// no frames at all before declaring the capture dead.
///
/// This exists because the failure it catches is otherwise completely
/// silent. `xdg-desktop-portal-hyprland` stops requesting frames after ten
/// consecutive "Out of buffers" misses and never sends an error to PipeWire
/// (see [`choose_capture_backend`]); a compositor that fails to import a
/// capture buffer likewise just drops the frame. In both cases GStreamer has
/// nothing to report -- no bus error, no EOS, no flow-return failure -- so
/// without a timeout here the frame pump blocks in `pull_sample` forever and
/// the mirror session appears frozen with nothing anywhere saying why. That
/// is precisely the 45-second freeze that took a `journalctl` dig to
/// explain.
///
/// Bailing propagates out of `breadcastd`'s frame-pump thread, which already
/// reports `DaemonCommand::SessionEnded` on exit, so the session tears down
/// and the failure surfaces as a real event instead of a hang. Generous
/// enough (10s) that a merely slow moment -- a heavy compositor frame, a
/// bitrate renegotiation -- doesn't trip it; anything longer than this is
/// not a hiccup, since neither of the known failure modes recovers.
const CAPTURE_STALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Pulls one complete Annex-B H.264 access unit from `appsink`, blocking
/// until one is available. Returns `None` once the pipeline reaches EOS or
/// the sink otherwise stops (e.g. pipeline torn down from another thread),
/// and errors if the pipeline is still playing but has gone
/// [`CAPTURE_STALL_TIMEOUT`] without producing a frame.
///
/// A buffer with no PTS is skipped (this pulls the next one instead) rather
/// than reported with a substituted timestamp, as an earlier version did
/// with `.unwrap_or(0)`. That substitution was actively dangerous rather
/// than merely imprecise: openscreen derives the frame's RTP timestamp from
/// this value and enforces strict monotonicity with a *fatal* `OSP_CHECK`
/// (`sender_impl.cc`'s `OSP_CHECK_GT(frame.rtp_timestamp, ...)`), not an
/// error return -- so a single PTS-less buffer part-way into a session would
/// abort the whole daemon. `facade.cc` independently drops non-monotonic
/// capture times as a second line of defence; neither guard makes the other
/// redundant, since `enqueue_frame` is a public FFI entry point that has to
/// hold up against any caller.
pub fn pull_encoded_frame(appsink: &gst_app::AppSink) -> Result<Option<(Vec<u8>, bool, i64)>> {
    let poll = gst::ClockTime::from_mseconds(CAPTURE_STALL_POLL.as_millis() as u64);
    let mut stalled_for = Duration::ZERO;
    loop {
        let pulled_at = std::time::Instant::now();
        let Some(sample) = appsink.try_pull_sample(Some(poll)) else {
            // EOS is the ordinary end: the user hit "Stop sharing" in the
            // portal, or the source went away.
            if appsink.is_eos() {
                return Ok(None);
            }
            // Teardown from another thread (`CastMirrorSession::stop` sets
            // the pipeline to Null) flushes the sink *before*
            // `current_state()` leaves Playing. A flushing sink returns
            // `None` immediately; counting that as stall time used to
            // raise a spurious "capture stalled" on every normal stop.
            if !matches!(appsink.current_state(), gst::State::Playing | gst::State::Paused)
                || matches!(appsink.pending_state(), gst::State::Null | gst::State::Ready)
                || pulled_at.elapsed() < Duration::from_millis(20)
            {
                return Ok(None);
            }

            stalled_for += pulled_at.elapsed();
            if stalled_for < CAPTURE_STALL_TIMEOUT {
                continue;
            }
            bail!(
                "capture stalled: no encoded frame for {}s while the pipeline was still \
                 playing (no GStreamer error, no EOS). This is the shape of a portal-side \
                 give-up -- see `choose_capture_backend` -- rather than a pipeline fault, \
                 and it will not recover on its own",
                CAPTURE_STALL_TIMEOUT.as_secs()
            );
        };
        stalled_for = Duration::ZERO;
        let buffer = sample.buffer().context("pulled sample had no buffer")?;
        let Some(capture_time_us) = buffer.pts().map(|t| t.useconds() as i64) else {
            tracing::debug!("skipped an encoded frame with no PTS");
            continue;
        };
        let map = buffer.map_readable().context("failed to map sample buffer readable")?;
        let is_key_frame = !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
        return Ok(Some((map.as_slice().to_vec(), is_key_frame, capture_time_us)));
    }
}

/// Sends an upstream "force key unit" event from `appsink`, propagating to
/// `vah264enc` and causing it to emit an IDR frame on its next output --
/// the mechanism `cast_stream_test.rs`'s pull loop uses when
/// `CastStreamSender::needs_key_frame()` reports true.
pub fn request_key_frame(appsink: &gst_app::AppSink) {
    let event = gst_video::UpstreamForceKeyUnitEvent::builder().all_headers(true).build();
    let _ = appsink.send_event(event);
}

/// Updates `encoder`'s (a `vah264enc` element, as returned by
/// [`build_video_pipeline_for_streaming`]) target bitrate in kbps. Meant to
/// be driven periodically from `CastStreamSender::estimated_bandwidth_bps()`
/// -- this vendored subset of openscreen only does flow control, not
/// congestion control (see `Sender`'s class comment in
/// `breadcast-caststream-sys/vendor/openscreen/cast/streaming/public/sender.h`),
/// so actually throttling the encoder in response is this project's own
/// responsibility.
pub fn set_video_bitrate_kbps(encoder: &gst::Element, kbps: u32) {
    encoder.set_property("bitrate", kbps);
}

/// Why [`run_until_error_or_timeout`] returned successfully — distinct from
/// each other because a caller (e.g. a UI reporting "mirroring stopped")
/// needs to tell "the user hit Stop-sharing in the portal picker, EOS is
/// expected" apart from "nothing happened for N seconds, which for a smoke
/// test just means the run duration elapsed normally." Collapsing both into
/// a bare `Ok(())`, as a previous version of this function did, is exactly
/// the kind of silent-success-that-wasn't this project has already lost a
/// lot of time chasing elsewhere (the Cast `LOAD FAILED` debugging).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOutcome {
    /// The pipeline reached end-of-stream (e.g. the portal source ended
    /// because the user stopped sharing).
    Eos,
    /// `timeout` elapsed with no error or EOS.
    Timeout,
}

/// Blocks the calling thread until the pipeline reports an error or EOS.
/// Unlike [`run_until_error_or_timeout`] this does not give up after a
/// fixed duration -- a live mirror session can last hours, and a 1-hour
/// leftover from the smoke-test helper was leaving GStreamer errors
/// unobserved for the rest of the cast. Also returns [`RunOutcome::Eos`]
/// once the pipeline has been torn down from another thread (`Null`), so
/// a daemon watcher does not sit forever after `stop()`.
pub fn run_until_eos_or_error(pipeline: &gst::Pipeline) -> Result<RunOutcome> {
    let bus = pipeline.bus().context("pipeline has no bus")?;
    loop {
        let Some(msg) = bus.timed_pop_filtered(
            gst::ClockTime::from_mseconds(500),
            &[gst::MessageType::Error, gst::MessageType::Eos, gst::MessageType::Warning],
        ) else {
            if !matches!(
                pipeline.current_state(),
                gst::State::Playing | gst::State::Paused | gst::State::Ready
            ) {
                return Ok(RunOutcome::Eos);
            }
            continue;
        };

        use gst::MessageView;
        match msg.view() {
            MessageView::Error(e) => {
                bail!(
                    "GStreamer pipeline error from {:?}: {} ({:?})",
                    e.src().map(|s| s.path_string()),
                    e.error(),
                    e.debug()
                );
            }
            MessageView::Warning(w) => {
                tracing::warn!(
                    src = ?w.src().map(|s| s.path_string()),
                    error = %w.error(),
                    "GStreamer pipeline warning"
                );
            }
            MessageView::Eos(_) => return Ok(RunOutcome::Eos),
            _ => {}
        }
    }
}

/// Blocks the calling thread until the pipeline reports an error or EOS, or
/// `timeout` elapses (whichever first). Returns which of those happened, or
/// `Err` on a real pipeline error. Meant for smoke-testing from a
/// synchronous `main`/example; the daemon uses [`run_until_eos_or_error`].
pub fn run_until_error_or_timeout(pipeline: &gst::Pipeline, timeout: gst::ClockTime) -> Result<RunOutcome> {
    let bus = pipeline.bus().context("pipeline has no bus")?;
    let deadline = std::time::Instant::now() + std::time::Duration::from(timeout);

    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return Ok(RunOutcome::Timeout);
        }
        let Some(msg) = bus.timed_pop_filtered(
            gst::ClockTime::from_mseconds(remaining.as_millis().min(500) as u64),
            &[gst::MessageType::Error, gst::MessageType::Eos, gst::MessageType::Warning],
        ) else {
            continue;
        };

        use gst::MessageView;
        match msg.view() {
            MessageView::Error(e) => {
                bail!(
                    "GStreamer pipeline error from {:?}: {} ({:?})",
                    e.src().map(|s| s.path_string()),
                    e.error(),
                    e.debug()
                );
            }
            MessageView::Warning(w) => {
                tracing::warn!(
                    src = ?w.src().map(|s| s.path_string()),
                    error = %w.error(),
                    "GStreamer pipeline warning"
                );
            }
            MessageView::Eos(_) => return Ok(RunOutcome::Eos),
            _ => {}
        }
    }
}

/// A private, per-run HLS output directory under `$XDG_RUNTIME_DIR` (0700,
/// tmpfs, cleared on logout) rather than a fixed path under `/tmp`. A fixed
/// `/tmp` path is predictable and `/tmp` is world-writable: another local
/// user could pre-create or symlink it before this runs, to either read the
/// screen-recording segments this then serves on the LAN, or plant files
/// for the HTTP server to hand out. `XDG_RUNTIME_DIR` is exclusively
/// readable/writable by this user, so predictability of the subdirectory
/// name under it doesn't matter.
///
/// `label` distinguishes concurrent sessions of different kinds (e.g.
/// `"cast-mirror"` vs `"dlna-mirror"`) from colliding on the same path if
/// ever run at once on the same machine; the process id further
/// distinguishes concurrent runs of the *same* kind.
pub fn hls_output_dir(label: &str) -> Result<PathBuf> {
    let runtime_dir = std::env::var("XDG_RUNTIME_DIR").context("XDG_RUNTIME_DIR is not set")?;
    let dir = PathBuf::from(runtime_dir)
        .join("breadcast")
        .join(format!("{label}-{}", std::process::id()));
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&dir)
        .with_context(|| format!("failed to create HLS output dir {}", dir.display()))?;
    Ok(dir)
}

/// Polls `playlist_path` until it contains at least `min_segments` `#EXTINF`
/// entries or `timeout` elapses. Casting/loading a URL before the encode
/// pipeline has actually produced any segments — which an earlier version
/// of this project's examples did unconditionally, via a fixed sleep
/// regardless of whether encoding had actually started — hands the
/// receiver a 404 playlist and produces an unexplained load failure.
pub async fn wait_for_playlist_segments(playlist_path: &Path, min_segments: usize, timeout: Duration) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(contents) = std::fs::read_to_string(playlist_path) {
            if contents.lines().filter(|l| l.starts_with("#EXTINF")).count() >= min_segments {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!(
                "HLS playlist at {} never accumulated {min_segments} segments within {timeout:?} — \
                 the encode pipeline may not be producing output (check for a GStreamer error above)",
                playlist_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_shapes_hyprland_reports() {
        // `version` (plain) and `tag` (v-prefixed) from the same running
        // compositor, plus the `-N-gSHA` suffix a git build's tag carries.
        assert_eq!(parse_hyprland_version("0.55.4"), Some((0, 55, 4)));
        assert_eq!(parse_hyprland_version("v0.55.4"), Some((0, 55, 4)));
        assert_eq!(parse_hyprland_version("v0.56.0-123-gdeadbee"), Some((0, 56, 0)));
        assert_eq!(parse_hyprland_version("0.56"), Some((0, 56, 0)));
        assert_eq!(parse_hyprland_version(" v0.56.2\n"), Some((0, 56, 2)));

        assert_eq!(parse_hyprland_version(""), None);
        assert_eq!(parse_hyprland_version("unknown"), None);
    }

    #[test]
    fn straddles_the_dmabuf_crash_fix_correctly() {
        // The whole point of the constant: v0.55.4 aborts the compositor on
        // a DMA-BUF screencast, v0.56.0 is the first release with the fix.
        assert!(parse_hyprland_version("0.55.4").unwrap() < HYPRLAND_MIN_SAFE_DMABUF);
        assert!(parse_hyprland_version("0.55.99").unwrap() < HYPRLAND_MIN_SAFE_DMABUF);
        assert!(parse_hyprland_version("0.56.0").unwrap() >= HYPRLAND_MIN_SAFE_DMABUF);
        assert!(parse_hyprland_version("0.56.1").unwrap() >= HYPRLAND_MIN_SAFE_DMABUF);
        assert!(parse_hyprland_version("1.0.0").unwrap() >= HYPRLAND_MIN_SAFE_DMABUF);
    }
}
