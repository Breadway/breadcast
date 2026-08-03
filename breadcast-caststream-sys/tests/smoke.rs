//! Exercises the raw FFI end-to-end from Rust: create a sender, kick off
//! negotiation, and confirm the C++ side calls back out through
//! `post_message` with a real Cast Streaming OFFER -- without needing an
//! actual receiver on the network (nothing here waits for an ANSWER).

use std::ffi::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use breadcast_caststream_sys::{
    breadcast_caststream_sender_create, breadcast_caststream_sender_destroy,
    breadcast_caststream_sender_estimated_bandwidth_bps,
    breadcast_caststream_sender_needs_key_frame, breadcast_caststream_sender_negotiate,
};

static GOT_OFFER: AtomicBool = AtomicBool::new(false);
static LAST_OFFER: Mutex<Vec<u8>> = Mutex::new(Vec::new());

extern "C" fn post_message(
    _user_data: *mut c_void,
    _destination_id: *const std::ffi::c_char,
    _destination_id_len: usize,
    _message_namespace: *const std::ffi::c_char,
    _message_namespace_len: usize,
    message: *const std::ffi::c_char,
    message_len: usize,
) {
    let bytes = unsafe { std::slice::from_raw_parts(message as *const u8, message_len) };
    *LAST_OFFER.lock().unwrap() = bytes.to_vec();
    GOT_OFFER.store(true, Ordering::Release);
}

extern "C" fn on_negotiated(_user_data: *mut c_void) {}

extern "C" fn on_error(_user_data: *mut c_void, message: *const std::ffi::c_char, message_len: usize) {
    let bytes = unsafe { std::slice::from_raw_parts(message as *const u8, message_len) };
    panic!("session error: {}", String::from_utf8_lossy(bytes));
}

extern "C" fn on_picture_lost(_user_data: *mut c_void) {}

#[test]
fn create_negotiate_sends_offer_and_tears_down_cleanly() {
    let remote_ip = "192.0.2.1";
    let local_source_id = "sender-0";
    let receiver_id = "receiver-0";

    let sender = unsafe {
        breadcast_caststream_sender_create(
            remote_ip.as_ptr() as *const _,
            remote_ip.len(),
            local_source_id.as_ptr() as *const _,
            local_source_id.len(),
            receiver_id.as_ptr() as *const _,
            receiver_id.len(),
            1920,
            1080,
            8_000_000,
            30,
            1,
            std::ptr::null_mut(),
            post_message,
            on_negotiated,
            on_error,
            on_picture_lost,
        )
    };
    assert!(!sender.is_null(), "sender_create returned null");

    unsafe { breadcast_caststream_sender_negotiate(sender) };

    let deadline = Instant::now() + Duration::from_secs(2);
    while !GOT_OFFER.load(Ordering::Acquire) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(GOT_OFFER.load(Ordering::Acquire), "never received OFFER via post_message");

    let offer = String::from_utf8(LAST_OFFER.lock().unwrap().clone()).unwrap();
    assert!(offer.contains("\"type\":\"OFFER\""), "unexpected payload: {offer}");
    assert!(offer.contains("\"codecName\":\"h264\""), "unexpected payload: {offer}");

    // Before negotiation with a real receiver completes, these should still
    // return sane defaults rather than garbage/crashing.
    let needs_key = unsafe { breadcast_caststream_sender_needs_key_frame(sender) };
    let bandwidth = unsafe { breadcast_caststream_sender_estimated_bandwidth_bps(sender) };
    assert_eq!(needs_key, 1);
    assert!(bandwidth > 0);

    unsafe { breadcast_caststream_sender_destroy(sender) };
}
