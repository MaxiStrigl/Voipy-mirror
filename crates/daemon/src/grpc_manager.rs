use std::{net::Shutdown, pin::Pin};

use daemon_protocol::voicechat::{
    AudioLevelsUpdate, CallRequest, CallResponse, ConfigureRequest, ConfigureResponse,
    ConnectRequest, ConnectResponse, DeafenRequest, DeafenResponse, DisconnectRequest,
    DisconnectResponse, GetConfigRequest, GetConfigResponse, GetStatusRequest, GetStatusResponse,
    HangupRequest, HangupResponse, ListDevicesRequest, ListDevicesResponse, MuteRequest,
    MuteResponse, SetInputVolumeRequest, SetInputVolumeResponse, SetOutputVolumeRequest,
    SetOutputVolumeResponse, ShutdownRequest, ShutdownResponse, StatusUpdate,
    SubscribeAudioLevelsRequest, SubscribeStatusRequest, voicechat_daemon_server::VoicechatDaemon,
};
use futures_util::{Stream, future::ok};
use tokio::sync::{mpsc, watch};
use tonic::{Request as TonicRequest, Response as TonicResponse, Status as TonicStatus};

use crate::webrtc::Command;

type GrpcResult<T> = Result<T, tonic::Status>;
type ResponseStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send>>;

pub struct VoiceChatService {
    command_sender: tokio::sync::mpsc::Sender<Command>,
    status_receiver: watch::Receiver<GetStatusResponse>,
    shutdown_sender: watch::Sender<()>,
}

impl VoiceChatService {
    pub fn new(
        command_sender: mpsc::Sender<Command>,
        status_receiver: watch::Receiver<GetStatusResponse>,
        shutdown_sender: watch::Sender<()>,
    ) -> Self {
        Self {
            command_sender,
            status_receiver,
            shutdown_sender,
        }
    }
}

#[tonic::async_trait]
impl VoicechatDaemon for VoiceChatService {
    /// Establish a connection to the signaling server
    async fn connect_to_server(
        &self,
        request: TonicRequest<ConnectRequest>,
    ) -> Result<TonicResponse<ConnectResponse>, TonicStatus> {
        let req = request.into_inner();

        let port = u16::try_from(req.port)
            .map_err(|_| TonicStatus::invalid_argument("Invalid port number (must be 0-65535)"))?;

        self.command_sender
            .send(Command::Connect {
                host: req.address,
                port,
            })
            .await
            .map_err(|_| TonicStatus::unavailable("Signaling manager task is not responding"))?;

        Ok(TonicResponse::new(ConnectResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    /// Disconnects from the signaling server
    async fn disconnect_from_server(
        &self,
        _request: tonic::Request<DisconnectRequest>,
    ) -> Result<TonicResponse<DisconnectResponse>, TonicStatus> {
        self.command_sender
            .send(Command::Disconnect)
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(DisconnectResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    /// Initiates a voice call with the connected peer"]
    async fn call(
        &self,
        _request: tonic::Request<CallRequest>,
    ) -> Result<TonicResponse<CallResponse>, TonicStatus> {
        self.command_sender
            .send(Command::Call)
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(CallResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    /// Ends the current call
    async fn hangup(
        &self,
        _request: tonic::Request<HangupRequest>,
    ) -> Result<TonicResponse<HangupResponse>, TonicStatus> {
        self.command_sender
            .send(Command::Hangup)
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(HangupResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    /// Mutes or unmutes the microphone
    async fn set_mute(
        &self,
        request: tonic::Request<MuteRequest>,
    ) -> Result<TonicResponse<MuteResponse>, TonicStatus> {
        let active = request.into_inner().active;
        self.command_sender
            .send(Command::SetMute(active))
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(MuteResponse {}))
    }

    /// Deafens (mutes output) or undeafens the daemon
    async fn set_deafen(
        &self,
        request: tonic::Request<DeafenRequest>,
    ) -> Result<TonicResponse<DeafenResponse>, TonicStatus> {
        let active = request.into_inner().active;
        self.command_sender
            .send(Command::SetDeafen(active))
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(DeafenResponse {}))
    }

    /// Sets the input (microphone) volume gain.
    async fn set_input_volume(
        &self,
        request: tonic::Request<SetInputVolumeRequest>,
    ) -> Result<TonicResponse<SetInputVolumeResponse>, TonicStatus> {
        let volume = request.into_inner().volume;
        self.command_sender
            .send(Command::SetInputVolume(volume))
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(SetInputVolumeResponse {}))
    }

    /// Sets the output (speaker) volume gain.
    async fn set_output_volume(
        &self,
        request: tonic::Request<SetOutputVolumeRequest>,
    ) -> Result<TonicResponse<SetOutputVolumeResponse>, TonicStatus> {
        let volume = request.into_inner().volume;
        self.command_sender
            .send(Command::SetOutputVolume(volume))
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(SetOutputVolumeResponse {}))
    }

    /// Applies the full configuration upgrade to the daemon
    async fn configure(
        &self,
        request: tonic::Request<ConfigureRequest>,
    ) -> Result<tonic::Response<ConfigureResponse>, tonic::Status> {
        let req = request.into_inner();
        self.command_sender
            .send(Command::Configure(req))
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        Ok(TonicResponse::new(ConfigureResponse {
            success: true,
            error_message: String::new(),
        }))
    }

    /// Retrieves the current configuration"]
    async fn get_config(
        &self,
        _request: tonic::Request<GetConfigRequest>,
    ) -> Result<tonic::Response<GetConfigResponse>, tonic::Status> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.command_sender
            .send(Command::GetConfig(tx))
            .await
            .map_err(|_| TonicStatus::unavailable("WebRTC manager is not responding"))?;

        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), rx)
            .await
            .map_err(|_| TonicStatus::deadline_exceeded("Timed out waiting for config"))?
            .map_err(|_| TonicStatus::unavailable("WebRTC manager dropped the config reply"))?;

        Ok(TonicResponse::new(resp))
    }

    /// Lists available audio input devices
    async fn list_input_devices(
        &self,
        _request: tonic::Request<ListDevicesRequest>,
    ) -> Result<tonic::Response<ListDevicesResponse>, tonic::Status> {
        // as of cpal 0.17 has very bad printing/information gathering. I wait until they have proper support for pipewire/pulse
        todo!()
    }

    /// Lists available audio output devices
    async fn list_output_devices(
        &self,
        _request: tonic::Request<ListDevicesRequest>,
    ) -> Result<tonic::Response<ListDevicesResponse>, tonic::Status> {
        // as of cpal 0.17 has very bad printing/information gathering. I wait until they have proper support for pipewire/pulse
        todo!()
    }

    /// Gets  the current status of the daemon (connection, call state)
    async fn get_status(
        &self,
        _request: tonic::Request<GetStatusRequest>,
    ) -> Result<TonicResponse<GetStatusResponse>, TonicStatus> {
        let current_status = self.status_receiver.borrow().clone();

        Ok(TonicResponse::new(current_status))
    }

    /// Server streaming response type for the SubscribeStatus method.
    type SubscribeStatusStream = ResponseStream<StatusUpdate>;

    /// Subscribe to server updates (server events, errors, changes
    async fn subscribe_status(
        &self,
        _request: tonic::Request<SubscribeStatusRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStatusStream>, tonic::Status> {
        let status_rx = self.status_receiver.clone();

        let output_stream =
            futures_util::stream::unfold((status_rx, true), |(mut rx, is_first)| async move {
                if !is_first {
                    if rx.changed().await.is_err() {
                        // Channel closed
                        return None;
                    }
                }

                let status = rx.borrow_and_update().clone();

                let update = StatusUpdate {
                    state: status.status,
                    error_message: status.error_message,
                    connected_peer: status.connected_peer,
                    packaet_loss_percent: status.packaet_loss_percent,
                    ping_ms: status.ping_ms,
                };

                Some((Ok(update), (rx, false)))
            });

        Ok(TonicResponse::new(
            Box::pin(output_stream) as Self::SubscribeStatusStream
        ))
    }

    type SubscribeAudioLevelsStream = ResponseStream<AudioLevelsUpdate>;

    /// Subscribe to real-time audio level updates (for UI-visualization)
    async fn subscribe_audio_levels(
        &self,
        _request: tonic::Request<SubscribeAudioLevelsRequest>,
    ) -> std::result::Result<tonic::Response<Self::SubscribeAudioLevelsStream>, tonic::Status> {
        todo!()
    }

    /// Gracefully shuts down the daemon
    async fn shutdown(
        &self,
        _request: tonic::Request<ShutdownRequest>,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        let _ = self.command_sender.send(Command::Shutdown).await;

        self.shutdown_sender.send(()).ok();

        Ok(TonicResponse::new(ShutdownResponse {}))
    }
}
