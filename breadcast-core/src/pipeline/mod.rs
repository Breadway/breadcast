use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;

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
    let pipeline_str = format!(
        "pipewiresrc path={video_node_id} do-timestamp=true ! \
         videoconvert ! videoscale ! videorate ! \
         video/x-raw,format=NV12,width=1280,height=720,framerate=30/1 ! \
         vah264enc bitrate=4000 key-int-max=60 rate-control=cbr ! \
         video/x-h264,profile=main ! \
         h264parse config-interval=1 ! \
         hlssink.video \
         hlssink3 name=hlssink target-duration=2 playlist-length=6 max-files=10"
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
pub fn build_video_pipeline_for_streaming(
    video_node_id: u32,
) -> Result<(gst::Pipeline, gst_app::AppSink, gst::Element)> {
    gst::init().context("failed to initialize GStreamer")?;

    // Same 1280x720@30 Main-profile baseline as build_video_pipeline, for
    // the same reason (see its doc comment) -- broad decoder compatibility
    // first, revisit upward once a specific device's real ceiling is known.
    let pipeline_str = "pipewiresrc path=%VIDEO_NODE_ID% do-timestamp=true ! \
         videoconvert ! videoscale ! videorate ! \
         video/x-raw,format=NV12,width=1280,height=720,framerate=30/1 ! \
         vah264enc name=venc bitrate=4000 key-int-max=60 rate-control=cbr ! \
         video/x-h264,profile=main ! \
         h264parse name=h264parse config-interval=-1 ! \
         video/x-h264,stream-format=byte-stream,alignment=au ! \
         appsink name=appsink emit-signals=false sync=false max-buffers=4 drop=true"
        .replace("%VIDEO_NODE_ID%", &video_node_id.to_string());

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

    Ok((pipeline, appsink, encoder))
}

/// Pulls one complete Annex-B H.264 access unit from `appsink`, blocking
/// until one is available. Returns `None` once the pipeline reaches EOS or
/// the sink otherwise stops (e.g. pipeline torn down from another thread).
pub fn pull_encoded_frame(appsink: &gst_app::AppSink) -> Result<Option<(Vec<u8>, bool, i64)>> {
    let sample = match appsink.pull_sample() {
        Ok(sample) => sample,
        Err(_) if appsink.is_eos() => return Ok(None),
        Err(e) => bail!("appsink pull_sample failed: {e}"),
    };
    let buffer = sample.buffer().context("pulled sample had no buffer")?;
    let map = buffer.map_readable().context("failed to map sample buffer readable")?;
    let is_key_frame = !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
    // `.unwrap_or(0)` rather than propagating a missing PTS as an error:
    // CastStreamSender::enqueue_frame only needs monotonically-increasing,
    // real-elapsed-time-proportional values (see its doc comment) -- an
    // occasional buffer with no PTS shouldn't abort an otherwise-live
    // stream over it.
    let capture_time_us = buffer.pts().map(|t| t.useconds() as i64).unwrap_or(0);
    Ok(Some((map.as_slice().to_vec(), is_key_frame, capture_time_us)))
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

/// Blocks the calling thread until the pipeline reports an error or EOS, or
/// `timeout` elapses (whichever first). Returns which of those happened, or
/// `Err` on a real pipeline error. Meant for smoke-testing from a
/// synchronous `main`/example; the real daemon will want an async/watch-based
/// version instead of blocking a thread.
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
