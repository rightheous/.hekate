use std::collections::VecDeque;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::time::Duration;

use atspi::proxy::accessible::AccessibleProxy;
use atspi::proxy::component::ComponentProxy;
use atspi::{AccessibilityConnection, CoordType, ObjectRef, State};
use enigo::{Axis, Button, Coordinate, Direction, Enigo, Key, Keyboard, Mouse, Settings};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use x11rb::connection::Connection;
use x11rb::protocol::xproto::{
    Atom, AtomEnum, ConfigureWindowAux, ConnectionExt, ImageFormat, ImageOrder, InputFocus,
    StackMode, Window,
};
use x11rb::rust_connection::RustConnection;
use x11rb::{connect, CURRENT_TIME};

#[derive(Debug, Error)]
pub enum ComputerUseError {
    #[error("X11 is unavailable: {0}")]
    X11(String),
    #[error("AT-SPI is unavailable: {0}")]
    Accessibility(String),
    #[error("input simulation failed: {0}")]
    Input(String),
    #[error("screenshot failed: {0}")]
    Screenshot(String),
    #[error("invalid computer-use request: {0}")]
    Invalid(String),
}

#[derive(Clone, Debug, Serialize)]
pub struct Bounds {
    pub x: i32,
    pub y: i32,
    pub width: u32,
    pub height: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct WindowInfo {
    pub id: u32,
    pub title: String,
    pub application: String,
    pub focused: bool,
    pub bounds: Bounds,
}

#[derive(Clone, Debug, Serialize)]
pub struct Screenshot {
    pub path: String,
    pub width: u16,
    pub height: u16,
    pub size: u64,
    pub content_hash: String,
    pub media_type: &'static str,
}

#[derive(Clone, Debug, Serialize)]
pub struct AccessibilityNode {
    pub id: String,
    pub depth: usize,
    pub role: String,
    pub name: String,
    pub enabled: bool,
    pub focused: bool,
    pub bounds: Option<Bounds>,
}

#[derive(Clone, Debug, Serialize)]
pub struct InteractionObservation {
    pub screen_hash: String,
    pub focused_window_id: Option<u32>,
    pub windows: Vec<WindowInfo>,
}

#[derive(Clone, Debug, Serialize)]
pub struct InteractionResult {
    pub before: InteractionObservation,
    pub after: InteractionObservation,
    pub state_changed: bool,
    pub verified: bool,
}

#[derive(Clone)]
pub struct ComputerUseAdapter {
    artifact_root: PathBuf,
}

impl ComputerUseAdapter {
    pub fn new(workspace_root: impl AsRef<Path>) -> Self {
        Self {
            artifact_root: workspace_root.as_ref().join("var/artifacts/computer"),
        }
    }

    pub fn list_windows(&self) -> Result<Vec<WindowInfo>, ComputerUseError> {
        let (connection, screen) = x11()?;
        windows(&connection, screen)
    }

    pub fn focus_window(&self, id: u32) -> Result<InteractionResult, ComputerUseError> {
        self.interact(|_| {
            let (connection, _) = x11()?;
            connection
                .configure_window(id, &ConfigureWindowAux::new().stack_mode(StackMode::ABOVE))
                .map_err(x11_error)?
                .check()
                .map_err(x11_error)?;
            connection
                .set_input_focus(InputFocus::PARENT, id, CURRENT_TIME)
                .map_err(x11_error)?
                .check()
                .map_err(x11_error)?;
            Ok(())
        })
    }

    pub fn screenshot(&self) -> Result<Screenshot, ComputerUseError> {
        let (connection, screen_number) = x11()?;
        let screen = &connection.setup().roots[screen_number];
        let (rgb, hash) = screen_pixels(&connection, screen_number)?;
        std::fs::create_dir_all(&self.artifact_root)
            .map_err(|error| ComputerUseError::Screenshot(error.to_string()))?;
        let path = self
            .artifact_root
            .join(format!("{}.png", uuid::Uuid::new_v4()));
        write_png(&path, screen.width_in_pixels, screen.height_in_pixels, &rgb)?;
        let size = std::fs::metadata(&path)
            .map_err(|error| ComputerUseError::Screenshot(error.to_string()))?
            .len();
        Ok(Screenshot {
            path: path.to_string_lossy().into_owned(),
            width: screen.width_in_pixels,
            height: screen.height_in_pixels,
            size,
            content_hash: hash,
            media_type: "image/png",
        })
    }

    pub async fn inspect_accessibility(
        &self,
        max_nodes: usize,
    ) -> Result<Vec<AccessibilityNode>, ComputerUseError> {
        if !(1..=1000).contains(&max_nodes) {
            return Err(ComputerUseError::Invalid(
                "max_nodes must be between 1 and 1000".to_owned(),
            ));
        }
        let connection = AccessibilityConnection::new()
            .await
            .map_err(|error| ComputerUseError::Accessibility(error.to_string()))?;
        let root = AccessibleProxy::builder(connection.connection())
            .destination("org.a11y.atspi.Registry")
            .map_err(accessibility_error)?
            .path("/org/a11y/atspi/accessible/root")
            .map_err(accessibility_error)?
            .build()
            .await
            .map_err(accessibility_error)?;
        let children = root.get_children().await.map_err(accessibility_error)?;
        let mut pending: VecDeque<_> = children.into_iter().map(|item| (item, 0)).collect();
        let mut nodes = Vec::new();
        while let Some((object, depth)) = pending.pop_front() {
            if nodes.len() == max_nodes {
                break;
            }
            let proxy = accessible_proxy(connection.connection(), &object).await?;
            let name = proxy.name().await.unwrap_or_default();
            let role = proxy
                .get_role_name()
                .await
                .unwrap_or_else(|_| "unknown".to_owned());
            let state = proxy.get_state().await.unwrap_or_default();
            let bounds = component_bounds(connection.connection(), &object).await;
            nodes.push(AccessibilityNode {
                id: format!("{}{}", object.name, object.path),
                depth,
                role,
                name,
                enabled: state.contains(State::Enabled),
                focused: state.contains(State::Focused),
                bounds,
            });
            if let Ok(children) = proxy.get_children().await {
                pending.extend(children.into_iter().map(|item| (item, depth + 1)));
            }
        }
        Ok(nodes)
    }

    pub fn click(
        &self,
        x: i32,
        y: i32,
        double: bool,
    ) -> Result<InteractionResult, ComputerUseError> {
        validate_point(x, y)?;
        self.interact(|enigo| {
            enigo
                .move_mouse(x, y, Coordinate::Abs)
                .and_then(|_| enigo.button(Button::Left, Direction::Click))
                .and_then(|_| {
                    if double {
                        enigo.button(Button::Left, Direction::Click)
                    } else {
                        Ok(())
                    }
                })
                .map_err(input_error)
        })
    }

    pub fn type_text(&self, text: &str) -> Result<InteractionResult, ComputerUseError> {
        if text.is_empty() {
            return Err(ComputerUseError::Invalid("text cannot be empty".to_owned()));
        }
        if text.len() > 10_000 {
            return Err(ComputerUseError::Invalid(
                "text cannot exceed 10000 bytes".to_owned(),
            ));
        }
        self.interact(|enigo| enigo.text(text).map_err(input_error))
    }

    pub fn key(&self, key: &str) -> Result<InteractionResult, ComputerUseError> {
        let key = parse_key(key)?;
        self.interact(|enigo| enigo.key(key, Direction::Click).map_err(input_error))
    }

    pub fn hotkey(&self, keys: &[String]) -> Result<InteractionResult, ComputerUseError> {
        if !(2..=5).contains(&keys.len()) {
            return Err(ComputerUseError::Invalid(
                "hotkey requires between 2 and 5 keys".to_owned(),
            ));
        }
        let keys = keys
            .iter()
            .map(|key| parse_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        self.interact(|enigo| {
            let mut pressed = Vec::new();
            let mut failure = None;
            for key in &keys {
                if let Err(error) = enigo.key(*key, Direction::Press) {
                    failure = Some(error);
                    break;
                }
                pressed.push(*key);
            }
            for key in pressed.into_iter().rev() {
                if let Err(error) = enigo.key(key, Direction::Release) {
                    failure.get_or_insert(error);
                }
            }
            failure.map_or(Ok(()), |error| Err(input_error(error)))
        })
    }

    pub fn scroll(
        &self,
        amount: i32,
        horizontal: bool,
    ) -> Result<InteractionResult, ComputerUseError> {
        if amount == 0 || !(-100..=100).contains(&amount) {
            return Err(ComputerUseError::Invalid(
                "scroll amount must be between -100 and 100 and cannot be zero".to_owned(),
            ));
        }
        self.interact(|enigo| {
            enigo
                .scroll(
                    amount,
                    if horizontal {
                        Axis::Horizontal
                    } else {
                        Axis::Vertical
                    },
                )
                .map_err(input_error)
        })
    }

    pub fn move_pointer(&self, x: i32, y: i32) -> Result<InteractionResult, ComputerUseError> {
        validate_point(x, y)?;
        self.interact(|enigo| enigo.move_mouse(x, y, Coordinate::Abs).map_err(input_error))
    }

    fn interact(
        &self,
        action: impl FnOnce(&mut Enigo) -> Result<(), ComputerUseError>,
    ) -> Result<InteractionResult, ComputerUseError> {
        let before = observe()?;
        let mut enigo = Enigo::new(&Settings::default()).map_err(input_error)?;
        action(&mut enigo)?;
        std::thread::sleep(Duration::from_millis(75));
        let after = observe()?;
        let state_changed = before.screen_hash != after.screen_hash
            || before.focused_window_id != after.focused_window_id;
        Ok(InteractionResult {
            before,
            after,
            state_changed,
            // A visual/focus delta is evidence, not proof of the requested semantic effect.
            verified: false,
        })
    }
}

async fn accessible_proxy<'a>(
    connection: &'a atspi::proxy::zbus::Connection,
    object: &'a ObjectRef,
) -> Result<AccessibleProxy<'a>, ComputerUseError> {
    AccessibleProxy::builder(connection)
        .destination(object.name.as_str())
        .map_err(accessibility_error)?
        .path(object.path.as_str())
        .map_err(accessibility_error)?
        .build()
        .await
        .map_err(accessibility_error)
}

async fn component_bounds(
    connection: &atspi::proxy::zbus::Connection,
    object: &ObjectRef,
) -> Option<Bounds> {
    let proxy = ComponentProxy::builder(connection)
        .destination(object.name.as_str())
        .ok()?
        .path(object.path.as_str())
        .ok()?
        .build()
        .await
        .ok()?;
    let (x, y, width, height) = proxy.get_extents(CoordType::Screen).await.ok()?;
    Some(Bounds {
        x,
        y,
        width: width.max(0) as u32,
        height: height.max(0) as u32,
    })
}

fn observe() -> Result<InteractionObservation, ComputerUseError> {
    let (connection, screen) = x11()?;
    let focused_window_id = connection
        .get_input_focus()
        .map_err(x11_error)?
        .reply()
        .map_err(x11_error)?
        .focus;
    let (_, screen_hash) = screen_pixels(&connection, screen)?;
    Ok(InteractionObservation {
        screen_hash,
        focused_window_id: (focused_window_id != 0).then_some(focused_window_id),
        windows: windows(&connection, screen)?,
    })
}

fn x11() -> Result<(RustConnection, usize), ComputerUseError> {
    connect(None).map_err(x11_error)
}

fn validate_point(x: i32, y: i32) -> Result<(), ComputerUseError> {
    let (connection, screen_number) = x11()?;
    let screen = &connection.setup().roots[screen_number];
    if x < 0
        || y < 0
        || x >= i32::from(screen.width_in_pixels)
        || y >= i32::from(screen.height_in_pixels)
    {
        return Err(ComputerUseError::Invalid(format!(
            "point ({x}, {y}) is outside the {}x{} desktop",
            screen.width_in_pixels, screen.height_in_pixels
        )));
    }
    Ok(())
}

fn windows(
    connection: &RustConnection,
    screen_number: usize,
) -> Result<Vec<WindowInfo>, ComputerUseError> {
    let root = connection.setup().roots[screen_number].root;
    let focused = connection
        .get_input_focus()
        .map_err(x11_error)?
        .reply()
        .map_err(x11_error)?
        .focus;
    let clients_atom = atom(connection, "_NET_CLIENT_LIST")?;
    let clients = connection
        .get_property(false, root, clients_atom, AtomEnum::WINDOW, 0, u32::MAX)
        .map_err(x11_error)?
        .reply()
        .map_err(x11_error)?
        .value32()
        .map(|values| values.collect::<Vec<_>>())
        .unwrap_or_else(|| {
            connection
                .query_tree(root)
                .ok()
                .and_then(|cookie| cookie.reply().ok())
                .map(|reply| reply.children)
                .unwrap_or_default()
        });
    let utf8 = atom(connection, "UTF8_STRING")?;
    let name_atom = atom(connection, "_NET_WM_NAME")?;
    let class_atom = atom(connection, "WM_CLASS")?;
    let mut result = Vec::new();
    for id in clients {
        let Ok(cookie) = connection.get_geometry(id) else {
            continue;
        };
        let Ok(geometry) = cookie.reply() else {
            continue;
        };
        let translated = connection
            .translate_coordinates(id, root, 0, 0)
            .map_err(x11_error)?
            .reply()
            .map_err(x11_error)?;
        result.push(WindowInfo {
            id,
            title: property_text(connection, id, name_atom, utf8),
            application: property_text(connection, id, class_atom, AtomEnum::STRING.into())
                .split('\0')
                .filter(|part| !part.is_empty())
                .last()
                .unwrap_or_default()
                .to_owned(),
            focused: id == focused,
            bounds: Bounds {
                x: i32::from(translated.dst_x),
                y: i32::from(translated.dst_y),
                width: u32::from(geometry.width),
                height: u32::from(geometry.height),
            },
        });
    }
    Ok(result)
}

fn screen_pixels(
    connection: &RustConnection,
    screen_number: usize,
) -> Result<(Vec<u8>, String), ComputerUseError> {
    let setup = connection.setup();
    let screen = &setup.roots[screen_number];
    let reply = connection
        .get_image(
            ImageFormat::Z_PIXMAP,
            screen.root,
            0,
            0,
            screen.width_in_pixels,
            screen.height_in_pixels,
            u32::MAX,
        )
        .map_err(x11_error)?
        .reply()
        .map_err(x11_error)?;
    let format = setup
        .pixmap_formats
        .iter()
        .find(|format| format.depth == reply.depth)
        .ok_or_else(|| ComputerUseError::Screenshot("unknown X11 pixel format".to_owned()))?;
    let bytes_per_pixel = usize::from(format.bits_per_pixel / 8);
    if !(2..=4).contains(&bytes_per_pixel) {
        return Err(ComputerUseError::Screenshot(format!(
            "unsupported X11 pixel width: {}",
            format.bits_per_pixel
        )));
    }
    let visual = screen
        .allowed_depths
        .iter()
        .flat_map(|depth| depth.visuals.iter())
        .find(|visual| visual.visual_id == screen.root_visual)
        .ok_or_else(|| ComputerUseError::Screenshot("root visual is missing".to_owned()))?;
    let stride = reply.data.len() / usize::from(screen.height_in_pixels);
    let mut rgb = Vec::with_capacity(
        usize::from(screen.width_in_pixels) * usize::from(screen.height_in_pixels) * 3,
    );
    for y in 0..usize::from(screen.height_in_pixels) {
        for x in 0..usize::from(screen.width_in_pixels) {
            let start = y * stride + x * bytes_per_pixel;
            let mut bytes = [0_u8; 4];
            bytes[..bytes_per_pixel].copy_from_slice(&reply.data[start..start + bytes_per_pixel]);
            let pixel = if setup.image_byte_order == ImageOrder::LSB_FIRST {
                u32::from_le_bytes(bytes)
            } else {
                bytes[..bytes_per_pixel].reverse();
                u32::from_le_bytes(bytes)
            };
            rgb.extend([
                channel(pixel, visual.red_mask),
                channel(pixel, visual.green_mask),
                channel(pixel, visual.blue_mask),
            ]);
        }
    }
    let hash = hex_hash(&rgb);
    Ok((rgb, hash))
}

fn channel(pixel: u32, mask: u32) -> u8 {
    if mask == 0 {
        return 0;
    }
    let value = (pixel & mask) >> mask.trailing_zeros();
    let maximum = mask >> mask.trailing_zeros();
    ((u64::from(value) * 255) / u64::from(maximum)) as u8
}

fn write_png(path: &Path, width: u16, height: u16, rgb: &[u8]) -> Result<(), ComputerUseError> {
    let file =
        File::create(path).map_err(|error| ComputerUseError::Screenshot(error.to_string()))?;
    let mut encoder = png::Encoder::new(BufWriter::new(file), u32::from(width), u32::from(height));
    encoder.set_color(png::ColorType::Rgb);
    encoder.set_depth(png::BitDepth::Eight);
    encoder
        .write_header()
        .and_then(|mut writer| writer.write_image_data(rgb))
        .map_err(|error| ComputerUseError::Screenshot(error.to_string()))
}

fn atom(connection: &RustConnection, name: &str) -> Result<Atom, ComputerUseError> {
    connection
        .intern_atom(false, name.as_bytes())
        .map_err(x11_error)?
        .reply()
        .map(|reply| reply.atom)
        .map_err(x11_error)
}

fn property_text(
    connection: &RustConnection,
    window: Window,
    property: Atom,
    type_: Atom,
) -> String {
    connection
        .get_property(false, window, property, type_, 0, 4096)
        .ok()
        .and_then(|cookie| cookie.reply().ok())
        .map(|reply| String::from_utf8_lossy(&reply.value).into_owned())
        .unwrap_or_default()
}

fn parse_key(value: &str) -> Result<Key, ComputerUseError> {
    let key = match value.to_ascii_lowercase().as_str() {
        "alt" => Key::Alt,
        "backspace" => Key::Backspace,
        "control" | "ctrl" => Key::Control,
        "delete" => Key::Delete,
        "down" => Key::DownArrow,
        "end" => Key::End,
        "enter" | "return" => Key::Return,
        "escape" | "esc" => Key::Escape,
        "home" => Key::Home,
        "left" => Key::LeftArrow,
        "meta" | "super" => Key::Meta,
        "pagedown" => Key::PageDown,
        "pageup" => Key::PageUp,
        "right" => Key::RightArrow,
        "shift" => Key::Shift,
        "space" => Key::Space,
        "tab" => Key::Tab,
        "up" => Key::UpArrow,
        "f1" => Key::F1,
        "f2" => Key::F2,
        "f3" => Key::F3,
        "f4" => Key::F4,
        "f5" => Key::F5,
        "f6" => Key::F6,
        "f7" => Key::F7,
        "f8" => Key::F8,
        "f9" => Key::F9,
        "f10" => Key::F10,
        "f11" => Key::F11,
        "f12" => Key::F12,
        _ => {
            let mut chars = value.chars();
            match (chars.next(), chars.next()) {
                (Some(character), None) => Key::Unicode(character),
                _ => {
                    return Err(ComputerUseError::Invalid(format!(
                        "unsupported key: {value}"
                    )))
                }
            }
        }
    };
    Ok(key)
}

fn hex_hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn x11_error(error: impl std::fmt::Display) -> ComputerUseError {
    ComputerUseError::X11(error.to_string())
}

fn accessibility_error(error: impl std::fmt::Display) -> ComputerUseError {
    ComputerUseError::Accessibility(error.to_string())
}

fn input_error(error: impl std::fmt::Display) -> ComputerUseError {
    ComputerUseError::Input(error.to_string())
}
