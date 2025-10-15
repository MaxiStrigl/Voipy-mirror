use core::panic::{self, PanicMessage};
use std::{net::{IpAddr, UdpSocket}, sync::Arc};

use audio_capture::{Capture, CaptureConfig};
use audio_playback::{Playback, PlaybackConfig};
use bincode::config;
use protocol::Packet;

fn main() {

    let ip = std::env::args().nth(1);
    let password = std::env::args().nth(2);

    if ip.is_none() || password.is_none() {
        println!("Usage: client <server_ip> <password>");
    }

    let ip = ip.unwrap();
    let password = password.unwrap();

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    let socket = Arc::new(socket);

    let handshake_packet = Packet::Handshake { password: password }; 

    let config = config::standard();

    match bincode::serde::encode_to_vec(handshake_packet, config) {
        Ok(encoded_packet) => {
            socket.send_to(&encoded_packet, &ip).ok();
        },
        Err(_) => panic!("Failed to encode handshake packet"),
    };

    let config = CaptureConfig {
        socket: socket.clone(),
        remote_addr: ip,
        sample_rate: 48000,
        snippet_duration: 20,
    };
    let capture = Capture::new(config).ok().unwrap();

    let config = PlaybackConfig {
        socket: socket,
        sample_rate: 48000,
        snippet_duration: 20,
    };

    let playback = Playback::new(config).ok().unwrap();

    std::thread::park();
}
