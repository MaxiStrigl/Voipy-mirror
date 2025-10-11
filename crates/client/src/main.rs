use core::panic;
use std::{net::UdpSocket, sync::Arc};

use audio_capture::{Capture, CaptureConfig};
use audio_playback::{Playback, PlaybackConfig};

fn main() {

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    let socket = Arc::new(socket);

    let config = CaptureConfig {
        socket: socket.clone(),
        remote_addr: "192.168.2.164:40000".to_string(),
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
