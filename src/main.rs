use std::sync::Arc;

use cpal::{
    traits::{DeviceTrait, HostTrait, StreamTrait}, BuildStreamError, Device, Stream, StreamConfig
};
use ringbuf::{HeapCons, HeapProd, HeapRb, Prod, traits::*};

fn main() {
    let host = cpal::default_host();

    let output_device = host
        .default_output_device()
        .expect("No default output device available!");

    let input_device = host
        .default_input_device()
        .expect("No default input device available!");

    println!(
        "Using Input device {}",
        input_device.name().expect("Couldn't get input name")
    );
    println!(
        "Using Output device {}",
        output_device.name().expect("Couldn't get output name")
    );

    let input_config = input_device
        .default_input_config()
        .expect("Failed to get input config");
    let output_config = input_device
        .default_input_config()
        .expect("Failed to get output config");

    let config = input_config.config();

    let latency_samples = (config.sample_rate.0 as f32 * 0.100) as usize;
    let mut ringbuffer = HeapRb::<f32>::new(latency_samples * config.channels as usize * 2);

    let (mut producer, mut consumer) = ringbuffer.split();

    let input_stream = build_input_stream(input_device, &config, producer).expect("Failed to get input stream");
    let output_stream = build_output_stream(output_device, &config, consumer).expect("Failed to get output stream");

    input_stream.play();
    output_stream.play();

    println!("Looping audio");

    std::thread::park();
}

fn build_input_stream(
    device: Device,
    config: &StreamConfig,
    mut producer: HeapProd<f32>,
) -> Result<Stream, BuildStreamError> {
    let input_callback = move |data: &[f32], _: &cpal::InputCallbackInfo| {
        producer.push_slice(data);
    };

    let error_callback = |err| {
        eprintln!("An error occured in the input stream: {}", err);
    };

    device.build_input_stream(config, input_callback, error_callback, None)
}

fn build_output_stream(device: Device, config: &StreamConfig, mut consumer: HeapCons<f32>) -> Result<Stream, BuildStreamError> {
    let output_callback = move |data:  &mut [f32], _: &cpal::OutputCallbackInfo| {
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


    device.build_output_stream(config, output_callback, error_callback, None)
}
