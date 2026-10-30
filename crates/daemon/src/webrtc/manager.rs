use anyhow::Result;
use audio_capture::{Capture, CaptureConfig};
use audio_playback::{Playback, PlaybackConfig};
use daemon_protocol::voicechat::{GetStatusResponse, get_status_response::Status};
use std::{sync::Arc, time::Duration};
use tokio::sync::{
    mpsc::{self, Receiver},
    watch,
};
use webrtc::{
    api::{
        API, APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{MIME_TYPE_OPUS, MediaEngine},
        setting_engine::SettingEngine,
    },
    ice_transport::{ice_candidate::RTCIceCandidateInit, ice_server::RTCIceServer},
    interceptor::registry::Registry,
    peer_connection::{RTCPeerConnection, sdp::session_description::RTCSessionDescription},
    rtp_transceiver::rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType},
    stats::StatsReportType,
    track::{
        self, track_local::track_local_static_sample::TrackLocalStaticSample,
        track_remote::TrackRemote,
    },
};

use crate::{
    config::DaemonConfig,
    webrtc::{
        Command,
        signaling::{SignalMessage, SignalingConnection},
    },
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
    status_sender: watch::Sender<GetStatusResponse>,
    state: DaemonState,
    signaling: Option<SignalingConnection>,

    api: API,
    peer_connection: Option<Arc<RTCPeerConnection>>,
    capture: Option<Capture>,
    audio_track: Option<Arc<TrackLocalStaticSample>>,

    playbacks: Vec<Playback>,
    remote_tracks: Vec<Arc<TrackRemote>>,

    config: DaemonConfig,
    muted: bool,
    deafened: bool,
}

pub enum ManagerInternalEvent {
    RemoteTrackReceived(Arc<TrackRemote>),
    IceCandidateGenerated(RTCIceCandidateInit),
}

impl WebRtcManager {
    pub fn new(
        command_receiver: Receiver<Command>,
        status_sender: watch::Sender<GetStatusResponse>,
    ) -> Result<Self> {
        Ok(Self {
            command_receiver,
            state: DaemonState::Disconnected,
            signaling: None,
            status_sender,
            api: Self::setup_api(Duration::from_secs(5))?,
            peer_connection: None,
            capture: None,
            audio_track: None,
            playbacks: Vec::new(),
            remote_tracks: Vec::new(),
            config: DaemonConfig::default(),
            muted: false,
            deafened: false,
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        let (internal_tx, mut internal_rx) = mpsc::unbounded_channel::<ManagerInternalEvent>();

        let mut stats_interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
              // 1. Process commands coming from the gRPC frontend
              Some(command) = self.command_receiver.recv() => {
                self.handle_command(command, &internal_tx).await?;
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
                  Ok(Some(msg)) => {
                    if let Err(e) = self.handle_signaling_message(msg, &internal_tx).await {
                      eprintln!("Failed to handle signaling message: {}", e);
                    };
                  },
                  Ok(None) | Err(_) => {
                    self.disconnect().await;
                  },
                }
              }

              // 3. Process WebRTC messages
              Some(event) = internal_rx.recv() => {
                match event {
                    ManagerInternalEvent::RemoteTrackReceived(track) => { if let Err(e) = self.start_playback(track).await {
                      eprintln!("Error starting Playback: {}", e);
                    }; },
                    ManagerInternalEvent::IceCandidateGenerated(rtcice_candidate_init) => {
                      if let Some(sig) = &mut self.signaling {
                        if let Err(e) =
                        sig.send(SignalMessage::Candidate(rtcice_candidate_init)).await {
                          eprintln!("Error sending ice candidate: {}", e);
                        };
                      }
                    },
                }
              }

              // 4. Periodically poll WebRTC stats
              _ = stats_interval.tick() => {
                if self.peer_connection.is_some() {
                  if let Err(e) = self.update_network_stats().await {
                    eprintln!("Failed to poll stats: {}", e);
                  }
                }
              }
            }
        }
    }

    fn setup_api(ice_timeout: Duration) -> Result<API> {
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
        settings_engine.set_ice_timeouts(Some(ice_timeout), None, None);

        let api = APIBuilder::new()
            .with_media_engine(m)
            .with_setting_engine(settings_engine)
            .with_interceptor_registry(registry)
            .build();

        Ok(api)
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        internal_tx: &mpsc::UnboundedSender<ManagerInternalEvent>,
    ) -> Result<()> {
        match command {
            Command::Connect { host, port } => {
                self.state = DaemonState::Connecting {
                    host: host.clone(),
                    port,
                    retry_count: 0,
                };
                self.sync_status().await;

                match SignalingConnection::connect(&host, port).await {
                    Ok(connection) => {
                        self.signaling = Some(connection);
                        self.state = DaemonState::Connected {
                            host: host,
                            port: port,
                        };
                        self.sync_status().await;
                    }
                    Err(e) => {
                        eprintln!("Failed to connect to signaling Server: {}", e);
                        self.disconnect().await;
                    }
                }
            }
            Command::Disconnect => {
                self.disconnect().await;
            }
            Command::Call => {
                println!("Starting call");
                let pc = self.prepare_connection(internal_tx.clone()).await?;

                let offer = pc.create_offer(None).await?;
                let sdp = offer.sdp.clone();
                pc.set_local_description(offer).await?;

                if let Some(sig) = &mut self.signaling {
                    sig.send(SignalMessage::Offer(sdp)).await?;
                }
            }
            Command::Hangup => {
                self.hangup().await;
            }

            Command::SetMute(active) => {
                self.muted = active;
                if let Some(capture) = &mut self.capture {
                    capture.set_mute(active);
                }
            }

            Command::SetDeafen(active) => {
                self.deafened = active;
                for playback in &mut self.playbacks {
                    playback.set_deafen(active);
                }
            }

            Command::SetInputVolume(volume) => {
                self.config.audio.input_volume = volume;
                if let Some(capture) = &mut self.capture {
                    capture.set_volume(volume);
                }
            }

            Command::SetOutputVolume(volume) => {
                self.config.audio.output_volume = volume;
                for playback in &mut self.playbacks {
                    playback.set_volume(volume);
                }
            }
            Command::Configure(req) => {
                self.apply_config(DaemonConfig::from_config_request(&req));
            }
            Command::GetConfig(tx) => {
                let _ = tx.send(self.config.to_config_response());
            }

            Command::Shutdown => {
                self.disconnect().await;
                self.playbacks.clear();
                return Ok(());
            }

            _ => {
                eprintln!("unhandled command")
            }
        }

        Ok(())
    }

    fn apply_config(&mut self, new_config: DaemonConfig) {
        let capture_chagned = new_config.audio.input_device != self.config.audio.input_device
            || new_config.audio.input_latency_ms != self.config.audio.input_latency_ms
            || new_config.audio.processing != self.config.audio.processing;

        let playback_changed = new_config.audio.output_device != self.config.audio.output_device;

        //Hot apply -- volumes
        if (new_config.audio.input_volume != self.config.audio.input_volume) {
            if let Some(capture) = &mut self.capture {
                capture.set_volume(new_config.audio.input_volume);
            }
        }

        if (new_config.audio.output_volume != self.config.audio.output_volume) {
            for playback in &mut self.playbacks {
                playback.set_volume(new_config.audio.output_volume);
            }
        }

        // hot apply -- jitter buffer
        if new_config.audio.output_jitter_buffer_ms != self.config.audio.output_jitter_buffer_ms {
            for playback in &mut self.playbacks {
                playback.set_jitter_buffer_ms(new_config.audio.output_jitter_buffer_ms);
            }
        }

        self.config = new_config;

        // rebild capture/playback if big info changed
        if capture_chagned && self.capture.is_some() {
            if let Err(e) = self.rebuild_capture() {
                panic!("failed to rebuild capture")
            }
        }

        if playback_changed && !self.playbacks.is_empty() {
            if let Err(e) = self.rebuild_playback() {
                panic!("failed to rebuild payback")
            }
        }
    }

    fn capture_config(&self, track: Arc<TrackLocalStaticSample>) -> audio_capture::CaptureConfig {
        let processing = audio_capture::ProcessingConfig {
            vad_threshold: self.config.audio.processing.vad_threshold,
            noise_suppression: self.config.audio.processing.noise_suppression,
            agc_enabled: self.config.audio.processing.automatic_gain_control,
            agc_target_level: self.config.audio.processing.agc_target_level,
            agc_max_gain: self.config.audio.processing.agc_max_gain,
            high_pass_filter_hz: self.config.audio.processing.high_pass_filter_hz,
            low_pass_filter_hz: self.config.audio.processing.low_pass_filter_hz,
        };

        CaptureConfig {
            track,
            tokio_handle: tokio::runtime::Handle::current(),
            sample_rate: 48000,
            snippet_duration: 20,
            channels: 1,
            input_device: self.config.audio.input_device.clone(),
            input_latency_ms: self.config.audio.input_latency_ms,
            processing,
        }
    }

    fn rebuild_capture(&mut self) -> Result<()> {
        let audio_track = match &mut self.audio_track {
            Some(t) => t.clone(),
            None => return Ok(()),
        };

        let mut capture = audio_capture::Capture::new(self.capture_config(audio_track))?;
        capture.set_volume(self.config.audio.input_volume);
        capture.set_mute(self.muted);
        self.capture = Some(capture);
        Ok(())
    }

    fn build_playback(&self, track: Arc<TrackRemote>) -> Result<Playback> {
        let mut playback = Playback::new(PlaybackConfig {
            track,
            sample_rate: 48000,
            tokio_handle: tokio::runtime::Handle::current(),
            channels: 1,
            output_device: self.config.audio.output_device.clone(),
            jitter_buffer_ms: self.config.audio.output_jitter_buffer_ms,
        })?;

        playback.set_volume(self.config.audio.output_volume);
        playback.set_deafen(self.deafened);
        Ok(playback)
    }

    fn rebuild_playback(&mut self) -> Result<()> {
        self.playbacks.clear();
        let tracks = self.remote_tracks.clone();
        for track in tracks {
            self.playbacks.push(self.build_playback(track)?);
        }
        Ok(())
    }

    async fn handle_signaling_message(
        &mut self,
        msg: SignalMessage,
        internal_tx: &mpsc::UnboundedSender<ManagerInternalEvent>,
    ) -> Result<()> {
        match msg {
            SignalMessage::Offer(sdp) => {
                println!("<- Received offer from peer");
                let pc = self.prepare_connection(internal_tx.clone()).await?;

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
                println!("<- Received answer from peer");

                if let Some(pc) = &self.peer_connection {
                    // Parse the string to an WebRTC answer
                    let answer = match RTCSessionDescription::answer(sdp) {
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
                        };
                        self.sync_status().await;
                    }
                    println!("WebRTC Handshake complete. Audio should be flowing");
                } else {
                    eprintln!("Received Answer but no active PeerConnection exists");
                }
            }
            SignalMessage::Candidate(rtcice_candidate_init) => {
                if let Some(pc) = &mut self.peer_connection {
                    pc.add_ice_candidate(rtcice_candidate_init).await?;
                }
            }
        }

        Ok(())
    }

    async fn disconnect(&mut self) {
        println!("Cleaning up connections and media");
        self.signaling = None;
        self.capture = None;
        self.audio_track = None;
        self.remote_tracks.clear();
        self.playbacks.clear();

        if let Some(pc) = self.peer_connection.take() {
            tokio::spawn(async move {
                let _ = pc.close().await;
                println!("WebRTC PeerConnection closed");
            });
        }

        self.state = DaemonState::Disconnected;
        self.sync_status().await;
        println!("Manager is now Disconnected")
    }

    async fn hangup(&mut self) {
        if let Some(pc) = &mut self.peer_connection {
            println!("Clearing up peek connection and media ");
            let _ = pc.close().await;

            self.capture = None;
            self.audio_track = None;

            self.remote_tracks.clear();
            self.playbacks.clear();

            if let DaemonState::InCall { host, port } = &self.state {
                self.state = DaemonState::Connected {
                    host: host.clone(),
                    port: *port,
                };
                self.sync_status().await;
            }
        }
    }

    async fn update_network_stats(&self) -> Result<()> {
        let pc = match &self.peer_connection {
            Some(pc) => pc,
            None => return Ok(()),
        };

        let stats = pc.get_stats().await;

        let mut ping = 0.0;
        let mut _loss = 0.0;

        for (_, report_type) in stats.reports {
            // Get Ping (Round trip time)
            if let StatsReportType::CandidatePair(pair_stats) = report_type {
                ping = pair_stats.current_round_trip_time * 1000.0;
            }

            // Get packet loss (how many of my packets did not make it to the callee)
            // TODO: This is not implemented as of version 0.17 of webrtc.
        }

        let mut current_status = self.status_sender.borrow().clone();

        current_status.ping_ms = ping as f32;
        current_status.packaet_loss_percent = -1.0;

        let _ = self.status_sender.send(current_status);

        Ok(())
    }

    async fn sync_status(&mut self) {
        let mut response = self.status_sender.borrow().clone();

        match &self.state {
            DaemonState::Disconnected => response.status = Status::Idle as i32,
            DaemonState::Connecting { host, .. } => {
                response.status = Status::Connecting as i32;
                response.connected_peer = host.clone();
            }
            DaemonState::Connected { host, .. } => {
                response.status = Status::Connected as i32;
                response.connected_peer = host.clone()
            }
            DaemonState::InCall { host, .. } => {
                response.status = Status::InCall as i32;
                response.connected_peer = host.clone()
            }
        }

        let _ = self.status_sender.send(response);
    }

    async fn start_playback(&mut self, track: Arc<TrackRemote>) -> Result<()> {
        println!("Starting audio playback for remote track: {}", track.id());

        let playback = self.build_playback(track.clone())?;
        self.remote_tracks.push(track);
        self.playbacks.push(playback);
        Ok(())
    }

    async fn prepare_connection(
        &mut self,
        internal_tx: mpsc::UnboundedSender<ManagerInternalEvent>,
    ) -> Result<Arc<RTCPeerConnection>> {
        // Create peer Connection
        self.api = Self::setup_api(Duration::from_secs(
            self.config.connection.ice_timeout_seconds,
        ))?;

        let ice_servers = self
            .config
            .connection
            .stun_servers
            .iter()
            .map(|s| RTCIceServer {
                urls: vec![s.to_owned()],
                ..Default::default()
            })
            .collect();

        let config = webrtc::peer_connection::configuration::RTCConfiguration {
            ice_servers: ice_servers,
            ..Default::default()
        };
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

        let capture = audio_capture::Capture::new(self.capture_config(audio_track.clone()))?;

        crate::webrtc::handlers::setup_pc_handler(&pc, internal_tx);

        self.audio_track = Some(audio_track);
        self.peer_connection = Some(pc.clone());
        self.capture = Some(capture);
        Ok(pc)
    }
}
