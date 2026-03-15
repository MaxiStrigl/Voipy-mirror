use std::pin::Pin;

use daemon_protocol::voicechat::{
    AudioLevelsUpdate, CallRequest, CallResponse, ConfigureRequest, ConfigureResponse,
    ConnectRequest, ConnectResponse, DeafenRequest, DeafenResponse, DisconnectRequest,
    DisconnectResponse, GetConfigRequest, GetConfigResponse, GetStatusRequest, GetStatusResponse,
    HangupRequest, HangupResponse, ListDevicesRequest, ListDevicesResponse, MuteRequest,
    MuteResponse, SetInputVolumeRequest, SetInputVolumeResponse, SetOutputVolumeRequest,
    SetOutputVolumeResponse, ShutdownRequest, ShutdownResponse, StatusUpdate,
    SubscribeAudioLevelsRequest, SubscribeStatusRequest, voicechat_daemon_server::VoicechatDaemon,
};
use futures_util::Stream;
use tokio::sync::{mpsc, watch};
use tonic::{Request as TonicRequest, Response as TonicResponse, Status as TonicStatus};

use crate::webrtc::Command;

type GrpcResult<T> = Result<T, tonic::Status>;
type ResponseStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send>>;

pub struct VoiceChatService {
    command_sender: tokio::sync::mpsc::Sender<Command>,
    shutdown_sender: watch::Sender<()>,
}

impl VoiceChatService {
    pub fn new(command_sender: mpsc::Sender<Command>, shutdown_sender: watch::Sender<()>) -> Self {
        Self {
            command_sender,
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
        request: tonic::Request<CallRequest>,
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

    #[doc = " Ends the current call"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn hangup<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<HangupRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<HangupResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Mutes or unmutes the microphone"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn set_mute<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<MuteRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<MuteResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Deafens (mutes output) or undeafens the daemon"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn set_deafen<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<DeafenRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<DeafenResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Sets the input (microphone) volume gain."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn set_input_volume<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<SetInputVolumeRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<SetInputVolumeResponse>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Sets the output (speaker) volume gain."]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn set_output_volume<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<SetOutputVolumeRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<SetOutputVolumeResponse>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Applies the full configuration upgrade to the daemon"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn configure<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<ConfigureRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<ConfigureResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Retrieves the current configuration"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_config<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetConfigRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<GetConfigResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Lists available audio input devices"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn list_input_devices<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<ListDevicesRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<ListDevicesResponse>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Lists available audio output devices"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn list_output_devices<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<ListDevicesRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<ListDevicesResponse>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Gets  the current status of the daemon (connection, call state)"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn get_status<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<GetStatusRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<GetStatusResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Server streaming response type for the SubscribeStatus method."]
    type SubscribeStatusStream = ResponseStream<StatusUpdate>;

    #[doc = " Subscribe to server updates (server events, errors, changes)"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn subscribe_status<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<SubscribeStatusRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<Self::SubscribeStatusStream>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    type SubscribeAudioLevelsStream = ResponseStream<AudioLevelsUpdate>;

    #[doc = " Subscribe to real-time audio level updates (for UI-visualization)"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn subscribe_audio_levels<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<SubscribeAudioLevelsRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<
                        tonic::Response<Self::SubscribeAudioLevelsStream>,
                        tonic::Status,
                    >,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }

    #[doc = " Gracefully shuts down the daemon"]
    #[must_use]
    #[allow(
        elided_named_lifetimes,
        clippy::type_complexity,
        clippy::type_repetition_in_bounds
    )]
    fn shutdown<'life0, 'async_trait>(
        &'life0 self,
        request: tonic::Request<ShutdownRequest>,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<
                    Output = std::result::Result<tonic::Response<ShutdownResponse>, tonic::Status>,
                > + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}
