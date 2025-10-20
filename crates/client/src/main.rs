use std::{io::Read, net::UdpSocket, sync::Arc, time::Duration};

use anyhow::Result;

use bincode::config;
use futures_util::{SinkExt, StreamExt};
use protocol::Packet;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use webrtc::{
    api::{
        API, APIBuilder,
        interceptor_registry::register_default_interceptors,
        media_engine::{MIME_TYPE_OPUS, MediaEngine},
    },
    ice::candidate,
    ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit},
    interceptor::{Interceptor, registry::Registry},
    peer_connection::{
        OnTrackHdlrFn, configuration::RTCConfiguration,
        peer_connection_state::RTCPeerConnectionState,
        sdp::session_description::RTCSessionDescription,
    },
    rtp_transceiver::{
        RTCRtpTransceiver,
        rtp_codec::{RTCRtpCodecCapability, RTCRtpCodecParameters},
        rtp_receiver::RTCRtpReceiver,
    },
    track::{
        track_local::track_local_static_sample::TrackLocalStaticSample, track_remote::TrackRemote,
    },
};

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum SignalMessage {
    Offer(String),
    Answer(String),
    Candidate(RTCIceCandidateInit),
}

#[tokio::main]
async fn main() -> Result<()> {
    // let ip = std::env::args().nth(1);
    // let password = std::env::args().nth(2);

    // if ip.is_none() || password.is_none() {
    // println!("Usage: client <server_ip> <password>");
    // }

    // let ip = ip.unwrap();
    // let password = password.unwrap();

    // let socket = setup_udp(&ip, &password);

    let api = setup_api()?;

    let is_caller = std::env::args().nth(1).is_some();
    let mut offer_sent = false;

    let (ws_stream, _) = connect_async("ws://127.0.0.1:40000/signaling_route").await?;
    println!("Connected to the server");

    let (mut ws_sender, mut ws_reciever) = ws_stream.split();

    let config = RTCConfiguration::default();
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    let (candidate_sender, mut candidate_receiver) =
        mpsc::unbounded_channel::<RTCIceCandidateInit>();

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

    //TODO: Handle audio setup here
    peer_connection.on_track(Box::new(
        move |track: Arc<TrackRemote>, _: Arc<RTCRtpReceiver>, _: Arc<RTCRtpTransceiver>| {
            Box::pin(async move {
                println!("Got remote track: {}", track.tid());
            })
        },
    ));

    peer_connection.on_peer_connection_state_change(Box::new(|state: RTCPeerConnectionState| {
        println!("Connection State changed: {}", state);

        if state == RTCPeerConnectionState::Failed {
            println!("Peer Connection failed");
        }

        Box::pin(async {})
    }));

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

    peer_connection.add_track(audio_track.clone()).await?;
    println!("Added audio connection to track");

    tokio::spawn(async move {
        let silent_opus_frame: [u8; 3] = [0xf8, 0xff, 0xfe];
        let sample = webrtc::media::Sample {
            data: silent_opus_frame.to_vec().into(),
            duration: Duration::from_millis(20),
            ..Default::default()
        };

        let mut ticker = tokio::time::interval(Duration::from_millis(20));

        println!("sending audio samples");

        loop {
            ticker.tick().await;

            if let Err(e) = audio_track.write_sample(&sample).await {
                eprintln!("Failed to send audio");
                break;
            }
        }
    });

    println!("Waiting for signals");

    loop {
        tokio::select! {
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

                                        // Create answer
                                        let answer = peer_connection.create_answer(None).await?;
                                        let answer_sdp = answer.sdp.clone();
                                        peer_connection.set_local_description(answer).await?;

                                        // Send Answer
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

//
// let config = CaptureConfig {
//     socket: socket.clone(),
//     remote_addr: ip,
//     sample_rate: 48000,
//     snippet_duration: 20,
// };
// let capture = Capture::new(config).ok().unwrap();
//
// let config = PlaybackConfig {
//     socket: socket,
//     sample_rate: 48000,
//     snippet_duration: 20,
// };
//
// let playback = Playback::new(config).ok().unwrap();
//
// std::thread::park();
//
// Ok(())

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

fn setup_udp(ip: &str, password: &str) -> Arc<UdpSocket> {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    let socket = Arc::new(socket);

    let handshake_packet = Packet::Handshake {
        password: password.into(),
    };

    let config = config::standard();

    match bincode::serde::encode_to_vec(handshake_packet, config) {
        Ok(encoded_packet) => {
            socket.send_to(&encoded_packet, ip).ok();
        }
        Err(_) => panic!("Failed to encode handshake packet"),
    };
    socket
}
