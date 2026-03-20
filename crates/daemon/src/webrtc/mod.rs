pub mod handlers;
pub mod manager;
pub mod signaling;

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    Connect { host: String, port: u16 },
    Disconnect,
    Call,
    Hangup,
    SetMute(bool),
    SetDeafen(bool),
    SetInputVolume(f32),
    SetOutputVolume(f32),
    Shutdown,
}
