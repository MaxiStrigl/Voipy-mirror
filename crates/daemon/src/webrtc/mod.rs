pub mod handlers;
pub mod manager;
pub mod signaling;

pub enum Command {
    Connect { host: String, port: u16 },
    Disconnect,
    Call,
    Hangup,
    SetMute(bool),
    SetDeafen(bool),
    SetInputVolume(f32),
    SetOutputVolume(f32),
}
