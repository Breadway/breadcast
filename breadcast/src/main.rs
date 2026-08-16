//! breadcast — GTK4 layer-shell popup: a thin IPC client of `breadcastd`
//! (see `breadcast_core::ipc` for the wire protocol). Shows discovered Cast
//! devices and start/stop controls; holds no pipeline or protocol code of
//! its own — that all lives in `breadcastd`, so closing this popup never
//! interrupts an active cast.

mod css;
mod ipc_client;

use std::cell::RefCell;
use std::rc::Rc;

use bread_theme::load_palette;
use breadcast_core::ipc::{DeviceInfo, Protocol, ServerMessage, StateInfo};
use gtk4::prelude::*;
use gtk4::{Align, Application, Box as GBox, Button, Label, ListBox, Orientation, SelectionMode, glib};
use ipc_client::IpcClient;

const PANEL_WIDTH: i32 = 360;

fn main() {
    let _singleton_guard = match bread_utils::singleton::toggle_or_kill("breadcast") {
        Ok(bread_utils::singleton::Toggle::Started(guard)) => Some(guard),
        Ok(bread_utils::singleton::Toggle::KilledExisting) => return,
        Err(e) => {
            eprintln!("breadcast: single-instance lock unavailable ({e}); continuing without it");
            None
        }
    };

    let app = Application::builder().application_id("com.breadway.breadcast").build();
    app.connect_activate(build_ui);
    app.run();
}

fn build_ui(app: &Application) {
    bread_theme::gtk::apply_shared();
    bread_theme::gtk::apply_app_css(|| css::build_css(&load_palette()));

    let window = bread_utils::gtk_popup::new_overlay_window(app, "breadcast");
    bread_theme::gtk::bind_window_auto(&window);

    let panel = GBox::new(Orientation::Vertical, 8);
    panel.add_css_class("cast-panel");
    panel.set_size_request(PANEL_WIDTH, -1);
    panel.set_halign(Align::Center);
    panel.set_valign(Align::Center);

    let header = GBox::new(Orientation::Horizontal, 8);
    let title = Label::new(Some("Cast"));
    title.add_css_class("cast-title");
    title.set_hexpand(true);
    title.set_halign(Align::Start);
    let status_pill = Label::new(Some("Idle"));
    status_pill.add_css_class("cast-status-pill");
    header.append(&title);
    header.append(&status_pill);
    panel.append(&header);

    let stop_button = Button::with_label("Stop mirroring");
    stop_button.add_css_class("cast-stop-button");
    stop_button.set_visible(false);
    panel.append(&stop_button);

    let list = ListBox::new();
    list.set_selection_mode(SelectionMode::Browse);
    panel.append(&list);

    let empty_label = Label::new(Some("Searching for devices..."));
    empty_label.add_css_class("cast-empty-label");
    panel.append(&empty_label);

    window.set_child(Some(&panel));
    bread_utils::gtk_popup::close_on_outside_click(&window, &panel, {
        let window = window.clone();
        move || window.close()
    });

    match IpcClient::connect() {
        Ok((client, rx)) => {
            let client = Rc::new(RefCell::new(client));
            let _ = client.borrow_mut().send("list_devices", serde_json::Value::Null);
            let _ = client.borrow_mut().send("get_state", serde_json::Value::Null);

            list.connect_row_activated({
                let client = client.clone();
                move |_, row| {
                    let Some(device_id) = (unsafe { row.data::<String>("device_id") }) else { return };
                    let device_id = unsafe { device_id.as_ref() }.clone();
                    let _ = client.borrow_mut().send("start_cast", serde_json::json!({ "device_id": device_id }));
                }
            });

            stop_button.connect_clicked({
                let client = client.clone();
                move |_| {
                    let _ = client.borrow_mut().send("stop_cast", serde_json::Value::Null);
                }
            });

            let list = list.clone();
            let empty_label = empty_label.clone();
            let status_pill = status_pill.clone();
            let stop_button = stop_button.clone();
            glib::timeout_add_local(std::time::Duration::from_millis(100), move || {
                while let Ok(message) = rx.try_recv() {
                    handle_server_message(message, &list, &empty_label, &status_pill, &stop_button);
                }
                glib::ControlFlow::Continue
            });
        }
        Err(e) => {
            empty_label.set_label(&format!("breadcastd isn't running ({e})"));
        }
    }

    window.present();
}

/// Both a successful `"list_devices"`/`"get_state"` response and the
/// corresponding pushed event carry the same JSON shape in `result`/`data`
/// respectively — this normalizes both into one dispatch so there's only
/// one device-list/state rendering path to keep in sync.
fn handle_server_message(
    message: ServerMessage,
    list: &ListBox,
    empty_label: &Label,
    status_pill: &Label,
    stop_button: &Button,
) {
    let payload = match message {
        ServerMessage::Event { event, data } => Some((event, data)),
        ServerMessage::Response { result: Some(result), .. } if result.is_array() => {
            Some(("device_list_changed".to_string(), result))
        }
        ServerMessage::Response { result: Some(result), .. } if result.get("state").is_some() => {
            Some(("state_changed".to_string(), result))
        }
        ServerMessage::Response { error: Some(error), .. } => {
            eprintln!("breadcast: request failed: {error}");
            None
        }
        _ => None,
    };
    let Some((event, data)) = payload else { return };
    match event.as_str() {
        "device_list_changed" => update_device_list(list, empty_label, data),
        "state_changed" => update_state(status_pill, stop_button, data),
        _ => {}
    }
}

fn update_device_list(list: &ListBox, empty_label: &Label, data: serde_json::Value) {
    let Ok(devices) = serde_json::from_value::<Vec<DeviceInfo>>(data) else { return };

    while let Some(row) = list.row_at_index(0) {
        list.remove(&row);
    }

    empty_label.set_visible(devices.is_empty());
    list.set_visible(!devices.is_empty());

    for device in &devices {
        let row = gtk4::ListBoxRow::new();
        unsafe { row.set_data("device_id", device.id.clone()) };

        let row_box = GBox::new(Orientation::Vertical, 2);
        row_box.add_css_class("cast-device-row");
        let name = Label::new(Some(&device.name));
        name.set_halign(Align::Start);
        let model = Label::new(Some(&format!("{} · {}", device.model, protocol_label(device.protocol))));
        model.add_css_class("cast-device-model");
        model.set_halign(Align::Start);
        row_box.append(&name);
        row_box.append(&model);

        row.set_child(Some(&row_box));
        list.append(&row);
    }
}

fn protocol_label(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Cast => "Cast",
        Protocol::Dlna => "DLNA",
    }
}

fn update_state(status_pill: &Label, stop_button: &Button, data: serde_json::Value) {
    let Ok(state) = serde_json::from_value::<StateInfo>(data) else { return };
    match state {
        StateInfo::Idle => {
            status_pill.set_label("Idle");
            status_pill.remove_css_class("casting");
            stop_button.set_visible(false);
        }
        StateInfo::Casting { device_name, .. } => {
            status_pill.set_label(&format!("Casting to {device_name}"));
            status_pill.add_css_class("casting");
            stop_button.set_visible(true);
        }
    }
}
