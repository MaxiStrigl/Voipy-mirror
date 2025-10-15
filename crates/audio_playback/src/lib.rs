use anyhow::Result;
use bincode::config;
use cpal::traits::{HostTrait, StreamTrait};
use cpal::traits::{DeviceTrait};
use opus::{Decoder};
use protocol::Packet;
use ringbuf::HeapRb;
use ringbuf::traits::*;
use std::{net::UdpSocket, sync::Arc};
use thiserror::Error;

/// Custom error type for the capture library.
#[derive(Debug, Error)]
pub enum PlaybackError {
    #[error("No default output device available")]
    NoOutputDevice,
    #[error("No supported output config found for desired sample rate")]
    NoSupportedConfig,
    #[error("A cpal name error occured: {0}")]
    DeviceNameError(#[from] cpal::DeviceNameError),
    #[error("A cpal supported configs error occured: {0}")]
    SupportedStreamConfigsError(#[from] cpal::SupportedStreamConfigsError),
    #[error("An invalid number of channels was provided {0}")]
    InvlaidChannelCount(u32),
    #[error("An Opus error occured: {0}")]
    OpusError(#[from] opus::Error),
    #[error("An cpal build stream error occured: {0}")]
    BuildStreamError(#[from] cpal::BuildStreamError),
}

/// Configurations for the audio playback stream
pub struct PlaybackConfig {
    /// Shared UDP sockert for receiving the audio data
    pub socket: Arc<UdpSocket>,
    /// Desired sample rate for the transmitioon
    pub sample_rate: u32,
    /// Audio-Duration of a single packet
    pub snippet_duration: u32,
}

pub struct Playback {
    _stream: cpal::Stream,
}

impl Playback {
    /// Creates a new Playback instance and starts the audio stream.
    pub fn new(config: PlaybackConfig) -> Result<Self, PlaybackError> {
        // --- Setup cpal audio output ---
        let host = cpal::default_host();
        let device = host
            .default_output_device()
            .ok_or(PlaybackError::NoOutputDevice)?;

        println!("Using output device: {}", device.name()?);

        let desired_sample_rate = cpal::SampleRate(config.sample_rate);
        let supported_configs_range = device
            .supported_output_configs()?
            .find(|c| {
                c.min_sample_rate().0 <= config.sample_rate
                    && c.max_sample_rate().0 >= config.sample_rate
            })
            .ok_or(PlaybackError::NoSupportedConfig)?;

        let stream_config = supported_configs_range
            .with_sample_rate(desired_sample_rate)
            .config();

        println!("Using input config {:?}", stream_config);

        // --- Setup Opus Encoder ---
        let opus_channels = match stream_config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("Invalid amount of channels"),
        };

        let mut decoder = Decoder::new(config.sample_rate, opus_channels)?;

        // --- Setup Ring Buffer ----
        let ringbuffer = HeapRb::<f32>::new(48000);
        let (mut producer, mut consumer) = ringbuffer.split();

        // --- Spawn receiving thread ---
        std::thread::spawn(move || {
            let mut receive_buffer = [0u8; 4096]; //4 KiB // just a guess
            let mut opus_buffer = [0f32; 11520]; // 48000 Sample rate * 2 Channels * 0.120 ms (opus max)

            loop {
                if let Ok((size, _)) = config.socket.recv_from(&mut receive_buffer) {
                    let compressed = &receive_buffer[..size];

                    let compressed: Packet = bincode::serde::decode_from_slice(&compressed, config::standard()).unwrap().0;

                    let compressed = match compressed {
                        Packet::Handshake { .. } => continue,
                        Packet::Audio { data } => data,
                    };

                    match decoder.decode_float(&compressed, &mut opus_buffer, false) {
                        Ok(num_decoded_samples_per_channel) => {
                            let total_samples =
                                num_decoded_samples_per_channel * stream_config.channels as usize;

                            producer.push_slice(&opus_buffer[..total_samples]);
                        }
                        Err(e) => eprintln!("Opus decoding error: {}", e),
                    }
                }
            }
        });

        // --- Audio processing callbacks ---
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

        let stream = device.build_output_stream(&stream_config, output_callback, error_callback, None)?;
        stream.play().unwrap();

        Ok(Self {_stream: stream})
    }
}
