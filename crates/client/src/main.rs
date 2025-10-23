use anyhow::Result;
use std::sync::Arc;

use audio_capture::{Capture, CaptureConfig};
use audio_playback::{Playback, PlaybackConfig};
use futures_util::{SinkExt, StreamExt, lock::Mutex};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use webrtc::{
    api::{
        API, APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{MIME_TYPE_OPUS, MediaEngine},
    },
    ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
    interceptor::registry::Registry,
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
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

/// Enum representing the signalling message for the ICE connection setup
#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum SignalMessage {
    Offer(String),
    Answer(String),
    Candidate(RTCIceCandidateInit),
}

#[tokio::main]
async fn main() -> Result<()> {
    let ip = std::env::args().nth(1);
    let ip = ip.unwrap_or("0.0.0.0".into());
    let address = format!("ws://{}:40000/signaling_route", ip);

    // Prepare variables to determine callee/caller (Very Temporary approach)
    let is_caller = std::env::args().nth(2).is_some();
    let mut offer_sent = false;

    let (ws_stream, _) = connect_async(address).await?;
    println!("Connected to the server");

    let (mut ws_sender, mut ws_reciever) = ws_stream.split();

    let api = setup_api()?;

    let config = RTCConfiguration::default();
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);
    let (candidate_sender, mut candidate_receiver) =
        mpsc::unbounded_channel::<RTCIceCandidateInit>();

    let tokio_handle = tokio::runtime::Handle::current();
    let active_playbacks = Arc::new(Mutex::new(Vec::<Playback>::new()));

    // Forward newly gathered ICE candidates to the signaling server.
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

    // When a new remote audio track is received, create a new audio playback stream.
    let tokio_handle_clone = tokio_handle.clone();
    peer_connection.on_track(Box::new(
        move |track: Arc<TrackRemote>, _: Arc<RTCRtpReceiver>, _: Arc<RTCRtpTransceiver>| {
            let handle_clone = tokio_handle_clone.clone();
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

    // When the connection state changes print some basic info
    peer_connection.on_peer_connection_state_change(Box::new(|state: RTCPeerConnectionState| {
        println!("Connection State changed: {}", state);

        if state == RTCPeerConnectionState::Failed {
            println!("Peer Connection failed");
        }

        Box::pin(async {})
    }));

    // Create a audio track for input and hook it up with a audio capture
    let audio_track = Arc::new(TrackLocalStaticSample::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48000,
            channels: 1,
            ..Default::default()
        },
        "audio".to_owned(),
        "test".to_owned(),
    ));

    let capture_config = CaptureConfig {
        track: audio_track.clone(),
        tokio_handle: tokio_handle,
        sample_rate: 48000,
        snippet_duration: 20,
        channels: 1,
    };

    let _capture = Capture::new(capture_config);

    // Add the track to the connection (stream it to others)
    peer_connection.add_track(audio_track.clone()).await?;
    println!("Added audio connection to track");

    println!("Waiting for signals");

    loop {
        tokio::select! {
            // Sends the ICE offer if this client is the caller. Runs just  once at the start
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)), if is_caller && !offer_sent => {
                offer_sent = true;
                println!("Creating Offer...");
                let offer = peer_connection.create_offer(None).await?;
                let offer_sdp = offer.sdp.clone();

                // Set local description *before* sending
                peer_connection.set_local_description(offer).await?;

                let offer_msg = SignalMessage::Offer(offer_sdp);
                let json_msg = serde_json::to_string(&offer_msg)?;
                println!("-> Sending Offer");
                ws_sender.send(Message::Text(json_msg.into())).await?;
                }


                Some (candidate) = candidate_receiver.recv() => {
                    let msg = SignalMessage::Candidate(candidate);
                    let json_msg = serde_json::to_string(&msg)?;
                    ws_sender.send(Message::Text(json_msg.into())).await?;
            }

                        Some(Ok(msg)) = ws_reciever.next() => {
                            match msg {
                            Message::Text(text) => {
                                match serde_json::from_str::<SignalMessage>(&text) {
                                    Ok(SignalMessage::Offer(sdp)) => {
                                        println!("<- Received offer");

                                        let offer = RTCSessionDescription::offer(sdp)?;
                                        peer_connection.set_remote_description(offer).await?;

                                        let answer = peer_connection.create_answer(None).await?;
                                        let answer_sdp = answer.sdp.clone();
                                        peer_connection.set_local_description(answer).await?;

                                        let answer_msg = SignalMessage::Answer(answer_sdp);
                                        let answer_msg_json = serde_json::to_string(&answer_msg)?;
                                        println!("-> Sending Answer");
                                        ws_sender.send(Message::Text(answer_msg_json.into())).await?;

                                    }
                                    Ok(SignalMessage::Answer(sdp)) => {
                                        println!("<- Received Answer");
                                        let answer = RTCSessionDescription::answer(sdp)?;
                                        peer_connection.set_remote_description(answer).await?;
                                    }
                                    Ok(SignalMessage::Candidate(candidate_init)) => {
                                        println!("<- Recieved Ice candidate");
                                        peer_connection.add_ice_candidate(candidate_init).await?;
                                    }
                                    Err(e) => eprintln!("Failed to parse signal message: {}", e),
                                }
                            }
                            Message::Close(_) => {
                                println!("Closing connection");
                                break;}
                            _ => {}
                        }
                    }
                }
    }
    Ok(())
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
        webrtc::rtp_transceiver::rtp_codec::RTPCodecType::Audio,
    )?;

    let mut registry = Registry::new();

    registry = register_default_interceptors(registry, &mut m)?;

    let api = APIBuilder::new()
        .with_media_engine(m)
        .with_interceptor_registry(registry)
        .build();

    Ok(api)
}
