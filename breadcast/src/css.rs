use bread_theme::Palette;

/// Plain GTK4 CSS (not libadwaita) — matches `bos-settings`'/`breadclip`'s
/// precedent: libadwaita rows can't be width-constrained from outside a
/// fixed-width popup panel the way a plain `ListBoxRow` can.
pub fn build_css(palette: &Palette) -> String {
    format!(
        r#"
        .cast-panel {{
            background-color: {bg};
            border-radius: 12px;
            border: 1px solid alpha({fg}, 0.08);
            padding: 12px;
        }}
        .cast-title {{
            font-weight: 700;
            font-size: 1.1em;
            color: {fg};
        }}
        .cast-status-pill {{
            border-radius: 999px;
            padding: 3px 10px;
            font-size: 0.85em;
            background-color: {surface};
            color: {overlay};
        }}
        .cast-status-pill.casting {{
            background-color: alpha({accent}, 0.25);
            color: {accent};
        }}
        .cast-device-row {{
            border-radius: 8px;
            padding: 8px 10px;
            color: {fg};
        }}
        .cast-device-row:hover {{
            background-color: alpha({fg}, 0.06);
        }}
        .cast-device-row:selected {{
            background-color: alpha({accent}, 0.18);
        }}
        .cast-device-model {{
            font-size: 0.85em;
            color: {overlay};
        }}
        .cast-empty-label {{
            color: {overlay};
            padding: 24px 8px;
        }}
        .cast-stop-button {{
            background-color: alpha(#e35b5b, 0.15);
            color: #e35b5b;
            border-radius: 8px;
        }}
        "#,
        bg = palette.background,
        fg = palette.foreground,
        surface = palette.color0,
        overlay = palette.color7,
        accent = palette.color4,
    )
}
