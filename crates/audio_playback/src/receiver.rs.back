use std::{net::UdpSocket, sync::Arc};

use anyhow::Result;
use cpal::{
    SampleRate,
    traits::{DeviceTrait, HostTrait, StreamTrait},
};
use opus::{Channels, Decoder, Encoder};
use ringbuf::{HeapRb, traits::*};

const DESIRED_SAMPLE_RATE: SampleRate = SampleRate(48000);

fn main() -> Result<()> {
    // Read command line arguemts
    let local_addr = std::env::args()
        .nth(1)
        .expect("Usage: sender <REMOTE_IP:PORT>");
    println!("Listening for audio on: {}", local_addr);

    // Setup socket
    let socket = UdpSocket::bind(&local_addr)?;

    // Setup cpal for audio input
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .expect("No default output device available!");
    // let config = device.default_output_config()?.config();

    let mut configs = device.supported_output_configs()?;

    let supported = configs
        .find(|c| {
            c.min_sample_rate().0 <= DESIRED_SAMPLE_RATE.0
                && c.max_sample_rate().0 >= DESIRED_SAMPLE_RATE.0
        })
        .expect("No fitting config found");
    let config = supported.with_sample_rate(DESIRED_SAMPLE_RATE).config();

    let sample_rate = config.sample_rate.0;
    let channel_count = config.channels;

    let opus_channels = match channel_count {
        1 => opus::Channels::Mono,
        2 => opus::Channels::Stereo,
        _ => panic!("Invalid amount of channels"),
    };

    println!("Using output device: {}", device.name()?);
    println!("Using output config: {:?}", config);

    let mut decoder = Decoder::new(DESIRED_SAMPLE_RATE.0, opus_channels)?;

    // Setup ring buffer
    let ringbuffer = HeapRb::<f32>::new(48000);
    let (mut producer, mut consumer) = ringbuffer.split();

    std::thread::spawn(move || {
        let mut receive_buffer = [0u8; 4096]; //4 KiB // just a guess
        let mut opus_buffer = [0f32; 11520]; // 48000 Sample rate * 2 Channels * 0.120 ms (opus max)

        loop {
            if let Ok((size, _)) = socket.recv_from(&mut receive_buffer) {
                let compressed = &receive_buffer[..size];

                match decoder.decode_float(compressed, &mut opus_buffer, false) {
                    Ok(num_decoded_samples_per_channel) => {
                        let total_samples =
                            num_decoded_samples_per_channel * channel_count as usize;

                        producer.push_slice(&opus_buffer[..total_samples]);
                    }
                    Err(e) => eprintln!("Decode error: {}", e),
                }
            }
        }
    });

    let output_callback = move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
        for sample in data.iter_mut() {
            *sample = match consumer.try_pop() {
                Some(s) => s,
                None => 0.0,
            }
        }
    };

    let error_callback = |err| {
        eprintln!("An error occured in the input stream: {}", err);
    };

    let stream = device.build_output_stream(&config, output_callback, error_callback, None)?;

    stream.play()?;
    std::thread::park();

    Ok(())
}
