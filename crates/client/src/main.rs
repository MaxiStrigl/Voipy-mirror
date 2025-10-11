use core::panic;
use std::{net::UdpSocket, sync::Arc};

use audio_capture::{Capture, CaptureConfig};
use audio_playback::{Playback, PlaybackConfig};

fn main() {
    let mode = std::env::args().nth(1);
    let ip = std::env::args().nth(2);

    let mode = match mode {
        Some(s) => s,
        None => panic!("U messed up!"),
    };

    if mode == "s".to_string() {
        let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
        let socket = Arc::new(socket);

        let config = CaptureConfig {
            socket: socket,
            remote_addr: ip.unwrap(),
            sample_rate: 48000,
            snippet_duration: 20,
        };
        let capture = Capture::new(config).ok().unwrap();

        std::thread::park();
    } else if mode == "r".to_string() {
        let socket = UdpSocket::bind(ip.clone().unwrap()).unwrap();
        let socket = Arc::new(socket);


        let config = PlaybackConfig {
            socket: socket,
            sample_rate: 48000,
            snippet_duration: 20,
        };

        let playback = Playback::new(config).ok().unwrap();

        std::thread::park();
    } else {
        panic!("U messed up here");
    }
}
