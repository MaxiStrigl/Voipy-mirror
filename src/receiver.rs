use std::{net::UdpSocket, sync::Arc};

use anyhow::Result;
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::{HeapCons, HeapProd, HeapRb, Prod, traits::*};
fn main() -> Result<()> {
    // Read command line arguemts
    let local_addr = std::env::args()
        .nth(1)
        .expect("Usage: sender <REMOTE_IP:PORT>");
    println!("Listening for audio on: {}", local_addr);

    // Setup socket
    let socket = UdpSocket::bind(&local_addr)?;
    println!("res0");

    // Setup cpal for audio input
    let host = cpal::default_host();
    let device = host
        .default_output_device()
        .expect("No default output device available!");
    let config = device.default_output_config()?.config();

    println!("Using output device: {}", device.name()?);
    println!("Using output config: {:?}", config);

    // Setup ring buffer
    let ringbuffer = HeapRb::<f32>::new(48000);

    let (mut producer, mut consumer) = ringbuffer.split();

    println!("res0");
    std::thread::spawn(move || {
        let mut receive_buffer = [0u8; 4096]; //4 KiB // just a guess

        loop {
            if let Ok((size, _)) = socket.recv_from(&mut receive_buffer) {
                let sample_count = size / std::mem::size_of::<f32>();

                if sample_count > 0 {
                    let samples: &[f32] = bytemuck::cast_slice(&receive_buffer[..size]);
                    producer.push_slice(samples);
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
