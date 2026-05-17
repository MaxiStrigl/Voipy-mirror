pub mod handlers;
pub mod manager;
pub mod signaling;

#[derive(Debug)]
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
    Configure(daemon_protocol::voicechat::ConfigureRequest),
    GetConfig(tokio::sync::oneshot::Sender<daemon_protocol::voicechat::GetConfigResponse>),
}
