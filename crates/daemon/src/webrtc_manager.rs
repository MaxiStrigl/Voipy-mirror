use std::{sync::Arc, time::Duration};

use anyhow::Result;
use audio_capture::{Capture, CaptureConfig};
use audio_playback::{Playback, PlaybackConfig};
use futures_util::{
    SinkExt, StreamExt,
    lock::Mutex,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{self, Receiver, UnboundedSender},
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use webrtc::{
    api::{
        API, APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{MIME_TYPE_OPUS, MediaEngine},
        setting_engine::SettingEngine,
    },
    ice_transport::{
        ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
        ice_server::RTCIceServer,
    },
    interceptor::registry::Registry,
    peer_connection::{
        RTCPeerConnection, configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::{
        RTCRtpTransceiver,
        rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType},
        rtp_receiver::RTCRtpReceiver,
    },
    track::{
        track_local::track_local_static_sample::TrackLocalStaticSample, track_remote::TrackRemote,
    },
};

#[derive(Debug)]
pub enum Command {
    StartCall,
    StopCall,
    Shutdown,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum SignalMessage {
    Offer(String),
    Answer(String),
    Candidate(RTCIceCandidateInit),
}

enum LoopControl {
    Continue,
    Break,
}
/// Sets up the WebRTC API with a MediaEngine and default interceptors,
/// configured for OPUS audio.
fn setup_api() -> Result<API> {
    let mut m = MediaEngine::default();

    m.register_codec(
        RTCRtpCodecParameters {
            capability: RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_string(),
                clock_rate: 48000,
                channels: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        RTPCodecType::Audio,
    )?;

    let mut registry = Registry::new();

    registry = register_default_interceptors(registry, &mut m)?;

    let mut settings_engine = SettingEngine::default();
    settings_engine.set_ice_timeouts(Some(Duration::from_secs(1)), None, None);

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_setting_engine(settings_engine)
        .with_interceptor_registry(registry)
        .build();

    Ok(api)
}

pub struct WebRtcManager {
    command_receiver: Receiver<Command>,
    signaling_server_ip: String,
    api: API,
    peer_connection: Option<Arc<RTCPeerConnection>>,
    audio_track: Option<Arc<TrackLocalStaticSample>>,
    capture: Option<Capture>,
}

impl WebRtcManager {
    pub fn new(command_receiver: Receiver<Command>, signaling_server_ip: String) -> Result<Self> {
        let api = setup_api()?;
        Ok(Self {
            command_receiver,
            signaling_server_ip,
            api,
            peer_connection: None,
            audio_track: None,
            capture: None,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let address = format!("ws://{}:40000/signaling_route", self.signaling_server_ip);

        loop {
            println!("Connecting to Signaling Server");
            match connect_async(&address).await {
                Ok((ws_stream, _)) => {
                    println!("Connected to signaling server.");
                    let (ws_sender, ws_receiver) = ws_stream.split();

                    match self.handle_connection(ws_sender, ws_receiver).await {
                        Ok(true) => break,
                        Ok(false) => {}
                        Err(_) => {}
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Failed to connect to signaling server: {}. Retrying in 5s...",
                        e
                    );
                    dbg!(&address);
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            }
        }

        dbg!("Run terminated");
        Ok(())
    }

    async fn handle_connection(
        &mut self,
        mut ws_sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
        mut ws_receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    ) -> Result<bool> {
        //TODO: Maybe move this up to parent method
        let (candidate_sender, mut candidate_receiver) =
            mpsc::unbounded_channel::<RTCIceCandidateInit>();

        loop {
            tokio::select! {
                Some(command) = self.command_receiver.recv() => {
                    if let Err(e) = self.handle_command(
                        command,
                        &candidate_sender,
                        &mut ws_sender,
                    ).await {
                        eprintln!("Handle command returned: {}", e);
                        return Ok(true);
                    }
                }

                Some (candidate) = candidate_receiver.recv() => {
                    if let Err(e) = Self::handle_outgoing_candidate(
                        candidate,
                        &mut ws_sender).await {
                        eprintln!("Error sending outgoing ICE candidate: {}", e);
                        return Err(e);
                    }
                }

                msg = ws_receiver.next() => {
                    match msg {
                        Some(Ok(Message::Close(_))) | None => {
                            return Ok(true)
                        },
                        Some(Ok(msg)) => {
                            if let Err(e) = self.handle_websocket_message(
                                msg,
                                &candidate_sender,
                                &mut ws_sender
                            ).await {
                                eprintln!("Error in websocket message: {}", e);
                                return Err(e);
                            }
                        }
                        Some(Err(e)) => return Err(e.into()),
                    }
                }
            }
        }
    }

    async fn create_webrtc_components(
        &mut self,
        candidate_sender: UnboundedSender<RTCIceCandidateInit>,
    ) -> Result<()> {
        let config = RTCConfiguration {
            ice_servers: vec![
                RTCIceServer {
                    urls: vec!["stun:stun.l.google.com:19302".to_owned()],
                    ..Default::default()
                },
                RTCIceServer {
                    urls: vec!["stun:stun1.l.google.com:19302".to_owned()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let peer_connection = Arc::new(self.api.new_peer_connection(config).await?);

        let audio_track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 1,
                ..Default::default()
            },
            "audio".to_owned(), //TODO: Make this audio-<USR-ID>. But first implement some usr-id
            "default".to_owned(),
        ));

        let capture_config = CaptureConfig {
            track: audio_track.clone(),
            tokio_handle: tokio::runtime::Handle::current(),
            sample_rate: 48000,
            snippet_duration: 20,
            channels: 1,
        };
        let capture = Capture::new(capture_config)?;

        peer_connection.add_track(audio_track.clone()).await?;

        setup_peer_connection_handlers(&peer_connection, candidate_sender);

        self.peer_connection = Some(peer_connection);
        self.audio_track = Some(audio_track);
        self.capture = Some(capture);

        Ok(())
    }

    async fn handle_websocket_message(
        &mut self,
        msg: Message,
        candidate_sender: &UnboundedSender<RTCIceCandidateInit>,
        ws_sender: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<()> {
        match msg {
            Message::Text(text) => match serde_json::from_str::<SignalMessage>(&text) {
                Ok(SignalMessage::Offer(sdp)) => {
                    if !self.ensure_ready_for_call(candidate_sender).await? {
                        return Ok(());
                    }

                    println!("<- Received offer");

                    let offer = RTCSessionDescription::offer(sdp)?;
                    let pc = self.peer_connection.clone().unwrap();
                    pc.set_remote_description(offer).await?;

                    let answer = pc.create_answer(None).await?;
                    let answer_sdp = answer.sdp.clone();
                    pc.set_local_description(answer).await?;

                    let answer_msg = SignalMessage::Answer(answer_sdp);
                    let answer_msg_json = serde_json::to_string(&answer_msg)?;
                    println!("-> Sending Answer");
                    ws_sender
                        .send(Message::Text(answer_msg_json.into()))
                        .await?;
                }
                Ok(SignalMessage::Answer(sdp)) => {
                    println!("<- Received Answer");
                    let answer = RTCSessionDescription::answer(sdp)?;
                    self.peer_connection
                        .clone()
                        .unwrap()
                        .set_remote_description(answer)
                        .await?;
                }
                Ok(SignalMessage::Candidate(candidate_init)) => {
                    println!("<- Recieved Ice candidate");
                    self.peer_connection
                        .clone()
                        .unwrap()
                        .add_ice_candidate(candidate_init)
                        .await?;
                }
                Err(e) => eprintln!("Failed to parse signal message: {}", e),
            },
            _ => {}
        }

        Ok(())
    }

    async fn handle_outgoing_candidate(
        candidate: RTCIceCandidateInit,
        ws_sender: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<()> {
        let msg = SignalMessage::Candidate(candidate);
        let json_msg = serde_json::to_string(&msg)?;
        ws_sender.send(Message::Text(json_msg.into())).await?;

        Ok(())
    }

    async fn ensure_ready_for_call(
        &mut self,
        candidate_sender: &UnboundedSender<RTCIceCandidateInit>,
    ) -> Result<bool> {
        let state = self
            .peer_connection
            .as_ref()
            .map_or(RTCPeerConnectionState::Closed, |pc| pc.connection_state());

        match state {
            RTCPeerConnectionState::Closed
            | RTCPeerConnectionState::Unspecified
            | RTCPeerConnectionState::Disconnected
            | RTCPeerConnectionState::Failed => {
                println!("Connection is Failed/Closed creating a new one");
                self.create_webrtc_components(candidate_sender.clone())
                    .await?;
                Ok(true)
            }
            RTCPeerConnectionState::Connecting | RTCPeerConnectionState::Connected => {
                println!("Warning: Received StartCall but state is: {}", state);
                Ok(false)
            }
            RTCPeerConnectionState::New => Ok(true),
        }
    }

    async fn handle_command(
        &mut self,
        command: Command,
        candidate_sender: &UnboundedSender<RTCIceCandidateInit>,
        ws_sender: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ) -> Result<LoopControl> {
        match command {
            Command::StartCall => {
                if !self.ensure_ready_for_call(candidate_sender).await? {
                    return Ok(LoopControl::Continue);
                }

                println!("Initiating call...");
                let pc = self.peer_connection.clone().unwrap();
                let offer = pc.create_offer(None).await?;
                let offer_sdp = offer.sdp.clone();
                pc.set_local_description(offer).await?;

                let offer_msg = SignalMessage::Offer(offer_sdp);
                let json_msg = serde_json::to_string(&offer_msg)?;

                println!("-> Sending Offer");
                ws_sender.send(Message::Text(json_msg.into())).await?;
            }
            Command::StopCall => {
                println!("Stopping call...");
                if let Some(pc) = self.peer_connection.take() {
                    pc.close().await?;
                }
                self.audio_track = None;
                self.capture = None;

                println!("Call stopped. Ready for next");
            }

            Command::Shutdown => {
                println!("Shutting down daemon");
                if let Some(pc) = self.peer_connection.take() {
                    pc.close().await?;
                }
                ws_sender.send(Message::Close(None)).await?;
                return Ok(LoopControl::Break);
            }
        }

        Ok(LoopControl::Continue)
    }
}

fn setup_peer_connection_handlers(
    peer_connection: &Arc<RTCPeerConnection>,
    candidate_sender: mpsc::UnboundedSender<RTCIceCandidateInit>,
) {
    let tokio_handle = tokio::runtime::Handle::current();
    let active_playbacks = Arc::new(Mutex::new(Vec::<Playback>::new()));

    peer_connection.on_ice_candidate(Box::new(move |c: Option<RTCIceCandidate>| {
        let sender = candidate_sender.clone();
        Box::pin(async move {
            if let Some(candidate) = c {
                match candidate.to_json() {
                    Ok(init) => {
                        if let Err(e) = sender.send(init) {
                            eprintln!("Failed to send ICE candidate: {}", e);
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to serialzie ICE candidate: {}", e)
                    }
                }
            }
        })
    }));

    peer_connection.on_track(Box::new(
        move |track: Arc<TrackRemote>, _: Arc<RTCRtpReceiver>, _: Arc<RTCRtpTransceiver>| {
            let handle_clone = tokio_handle.clone();
            let playbacks = active_playbacks.clone();
            Box::pin(async move {
                if track.kind() == RTPCodecType::Audio {
                    let config = PlaybackConfig {
                        track: track.clone(),
                        sample_rate: 48000,
                        channels: 1,
                        tokio_handle: handle_clone,
                    };
                    let playback = Playback::new(config);

                    match playback {
                        Ok(p) => {
                            println!("Got audio track: {}", track.stream_id());
                            playbacks.lock().await.push(p);
                        }
                        Err(_) => todo!(),
                    }
                } else {
                }
            })
        },
    ));

    peer_connection.on_peer_connection_state_change(Box::new(
        move |state: RTCPeerConnectionState| {
            println!("Connection State changed: {}", state);
            Box::pin(async move {
                if state == RTCPeerConnectionState::Failed
                    || state == RTCPeerConnectionState::Disconnected
                    || state == RTCPeerConnectionState::Closed
                {
                    println!("Peer connection: {}", state);
                }
            })
        },
    ));
}
