use std::{net::UdpSocket, os::unix::thread, sync::Arc};

use anyhow::Result;
use cpal::{traits::{DeviceTrait, HostTrait, StreamTrait}, SampleRate};
use opus::Encoder;

const SNIPPET_DURATION_MS: u32 = 20;

const DESIRED_SAMPLE_RATE: SampleRate = SampleRate(48000);

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
    // let config = device.default_input_config()?.config();

    let mut configs = device.supported_input_configs()?;

    let supported = configs.find(|c| c.min_sample_rate().0 <= DESIRED_SAMPLE_RATE.0 && c.max_sample_rate().0 >= DESIRED_SAMPLE_RATE.0).expect("No fitting config found");
    let config = supported.with_sample_rate(DESIRED_SAMPLE_RATE).config();

    println!("Using input device: {}", device.name()?);
    println!("Using input config: {:?}", config);

    let sample_rate = config.sample_rate.0;
    let channel_count = config.channels;

    let opus_channels = match channel_count {
        1 => opus::Channels::Mono,
        2 => opus::Channels::Stereo,
        _ => panic!("Invalid amount of channels")
    };


    let samples_per_snippet = (channel_count as u32 * sample_rate * SNIPPET_DURATION_MS / 1000) as usize; 

    let mut encoder = Encoder::new(DESIRED_SAMPLE_RATE.0, opus_channels, opus::Application::Voip)?;

    println!("Sending audio data in snippets of {} ms ({} samples)", SNIPPET_DURATION_MS, samples_per_snippet);

    let mut opus_buffer = vec![0u8; 4096];
    let mut audio_bufffer: Vec<f32> = Vec::new();

    let input_callback = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        audio_bufffer.extend_from_slice(data);

        while audio_bufffer.len() >= samples_per_snippet {
            let chunk = &audio_bufffer[..samples_per_snippet];

            match encoder.encode_float(chunk, &mut opus_buffer) {
                Ok(compressed_size) => {
                    let compressed_data = &opus_buffer[..compressed_size];
                    socket.send_to(compressed_data, &remote_addr).ok();
                },
                Err(err) => eprintln!("Opus error: {}", err),
            }

            audio_bufffer.drain(..samples_per_snippet);
        }
    };

    let error_callback = |err| {
        eprintln!("An error occured in the input stream: {}", err);
    };

    let stream = device.build_input_stream(&config, input_callback, error_callback, None)?;
    stream.play().ok();

    std::thread::park();    

    Ok(())
}
