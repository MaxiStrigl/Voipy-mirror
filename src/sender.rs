use std::{net::UdpSocket, os::unix::thread, sync::Arc};

use anyhow::Result;
use cpal::traits::{DeviceTrait, HostTrait};

const SNIPPET_DURATION_MS: u32 = 20;

fn main() -> Result<()> {
    // Read command line arguemts
    let remote_addr = std::env::args()
        .nth(1)
        .expect("Usage: sender <REMOTE_IP:PORT>");
    println!("Sending audio to: {}", remote_addr);

    // Setup socket
    let socket = Arc::new(UdpSocket::bind("0.0.0.0:0")?);

    // Setup cpal for audio input
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .expect("No default input device available!");
    let config = device.default_input_config()?.config();

    println!("Using input device: {}", device.name()?);
    println!("Using input config: {:?}", config);

    let channels = config.channels as u32;
    let sample_rate = config.sample_rate.0;

    let samples_per_snippet = (channels * sample_rate * SNIPPET_DURATION_MS / 1000) as usize; 

    println!("Sending audio data in snippets of {} ms ({} samples)", SNIPPET_DURATION_MS, samples_per_snippet);

    let mut audio_bufffer: Vec<f32> = Vec::new();

    let input_callback = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        audio_bufffer.extend_from_slice(data);

        while audio_bufffer.len() >= samples_per_snippet {
            let chunk = &audio_bufffer[..samples_per_snippet];
            let bytes: &[u8] = bytemuck::cast_slice(chunk);

            socket.send_to(bytes, &remote_addr).ok();

            audio_bufffer.drain(..samples_per_snippet);
        }
    };

    let error_callback = |err| {
        eprintln!("An error occured in the input stream: {}", err);
    };

    let stream = device.build_input_stream(&config, input_callback, error_callback, None);

    std::thread::park();    

    Ok(())
}
