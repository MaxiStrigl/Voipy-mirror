use anyhow::Result;
use std::{sync::Arc, time::Duration};

use audio_capture::{Capture, CaptureConfig};
use tokio::sync::mpsc::Receiver;
use webrtc::{
    api::{
        API, APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{MIME_TYPE_OPUS, MediaEngine},
        setting_engine::SettingEngine,
    },
    interceptor::registry::Registry,
    peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription},
    rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType},
    track::track_local::track_local_static_sample::TrackLocalStaticSample,
};

use crate::webrtc::{
    Command,
    signaling::{SignalMessage, SignalingConnection},
};

enum DaemonState {
    Disconnected,
    Connecting {
        host: String,
        port: u16,
        retry_count: u32,
    },
    Connected {
        host: String,
        port: u16,
        // signaling Handle
    },
    InCall {
        host: String,
        port: u16,
        // signaling Handle
        // pc:
    },
}

pub struct WebRtcManager {
    command_receiver: Receiver<Command>,
    state: DaemonState,
    signaling: Option<SignalingConnection>,

    api: API,
    peer_connection: Option<Arc<RTCPeerConnection>>,
    capture: Option<Capture>,
}

impl WebRtcManager {
    pub fn new(command_receiver: Receiver<Command>) -> Result<Self> {
        Ok(Self {
            command_receiver,
            state: DaemonState::Disconnected,
            signaling: None,
            api: Self::setup_api()?,
            peer_connection: None,
            capture: None,
        })
    }

    pub async fn run(&mut self) {
        loop {
            tokio::select! {
              // 1. Process commands coming from the gRPC frontend
              Some(command) = self.command_receiver.recv() => {
                self.handle_command(command).await;
              }

              // 2. Process incoming messages from the signaling server
              recv_result = async {
                if let Some(sig) = self.signaling.as_mut() {
                  sig.recv().await
                } else {
                  std::future::pending().await
                }
              } => {
                match recv_result {
                  Ok(Some(msg)) => { self.handle_signaling_message(msg).await; },
                  Ok(None) | Err(_) => {
                    self.disconnect();
                  },
                }
              }
            }
        }
    }

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
        settings_engine.set_ice_timeouts(Some(Duration::from_secs(5)), None, None);

        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_setting_engine(settings_engine)
            .with_interceptor_registry(registry)
            .build();

        Ok(api)
    }

    pub async fn handle_command(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Connect { host, port } => {
                self.state = DaemonState::Connecting {
                    host: host.clone(),
                    port,
                    retry_count: 0,
                };

                match SignalingConnection::connect(&host, port).await {
                    Ok(connection) => {
                        self.signaling = Some(connection);
                        self.state = DaemonState::Connected {
                            host: host,
                            port: port,
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to signaling Server: {}", e);
                        self.disconnect();
                    }
                }
            }
            Command::Disconnect => {
                self.disconnect();
            }
            Command::Call => {
                let pc = self.prepare_connection().await?;

                // Create offer
                let offer = pc.create_offer(None).await?;
                let sdp = offer.sdp.clone();
                pc.set_local_description(offer).await?;

                // Send offer
                if let Some(sig) = &mut self.signaling {
                    sig.send(SignalMessage::Offer(sdp)).await?;
                }
            }
            _ => todo!(),
        }

        Ok(())
    }

    async fn handle_signaling_message(&mut self, msg: SignalMessage) -> Result<()> {
        match msg {
            SignalMessage::Offer(sdp) => {
                let pc = self.prepare_connection().await?;

                let offer = RTCSessionDescription::offer(sdp)?;
                pc.set_remote_description(offer).await?;

                let answer = pc.create_answer(None).await?;
                let sdp_answer = answer.sdp.clone();
                pc.set_local_description(answer.clone()).await?;

                if let Some(sig) = &mut self.signaling {
                    sig.send(SignalMessage::Answer(sdp_answer)).await?;
                }
            }
            SignalMessage::Answer(sdp) => {
                println!("<- Received offer from peer");

                if let Some(pc) = &self.peer_connection {
                    // Parse the string to an WebRTC answer
                    let answer = match RTCSessionDescription::offer(sdp) {
                        Ok(desc) => desc,
                        Err(e) => {
                            return Err(anyhow::anyhow!("Failed to parse Answer SDP: {}", e));
                        }
                    };

                    // Set the Remote description
                    if let Err(e) = pc.set_remote_description(answer).await {
                        return Err(anyhow::anyhow!("Failed to set remote description: {}", e));
                    }

                    // Update state
                    if let DaemonState::Connected { host, port } = &self.state {
                        self.state = DaemonState::InCall {
                            host: host.clone(),
                            port: *port,
                        }
                    }
                    println!("WebRTC Handshake complete. Audio should be flowing");
                } else {
                    eprintln!("Received Answer but no active PeerConnection exists");
                }
            }
            SignalMessage::Candidate(_rtcice_candidate_init) => todo!(),
        }

        Ok(())
    }

    fn disconnect(&mut self) {
        println!("Cleaning up connections and media");
        self.signaling = None;

        self.capture = None;
        if let Some(pc) = self.peer_connection.take() {
            tokio::spawn(async move {
                let _ = pc.close().await;
                println!("WebRTC PeerConnection closed");
            });
        }

        self.state = DaemonState::Disconnected;
        println!("Manager is now Disconnected")
    }

    async fn prepare_connection(&mut self) -> Result<Arc<RTCPeerConnection>> {
        // Create peer Connection
        let config = webrtc::peer_connection::configuration::RTCConfiguration::default();
        let pc = Arc::new(self.api.new_peer_connection(config).await?);

        // Setup Audio Track & Capture
        let audio_track = Arc::new(TrackLocalStaticSample::new(
            RTCRtpCodecCapability {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48000,
                channels: 1,
                ..Default::default()
            },
            "audio".to_owned(),
            "default".to_owned(),
        ));

        pc.add_track(audio_track.clone()).await?;

        let capture = audio_capture::Capture::new(CaptureConfig {
            track: audio_track,
            tokio_handle: tokio::runtime::Handle::current(),
            sample_rate: 48000,
            snippet_duration: 20,
            channels: 1,
        })?;

        self.peer_connection = Some(pc.clone());
        self.capture = Some(capture);
        Ok(pc)
    }
}
