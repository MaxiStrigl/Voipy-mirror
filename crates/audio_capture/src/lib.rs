#![recursion_limit = "123"]

use anyhow::Result;
use cpal::traits::HostTrait;
use cpal::traits::{DeviceTrait, StreamTrait};
use opus::Encoder;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::runtime::Handle;
use webrtc::{
    media::Sample, track::track_local::track_local_static_sample::TrackLocalStaticSample,
};


/// Custom error type for the capture library.
#[derive(Debug, Error)]
pub enum CaptureError {
    #[error("No default input device available")]
    NoInputDevice,
    #[error("No supported input config found for desired sample rate")]
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

/// Configurations for the audio capture stream
pub struct CaptureConfig {
    /// The WebRTC track to write captured audio samples to.
    pub track: Arc<TrackLocalStaticSample>,
    /// Tokio handle
    pub tokio_handle: Handle,
    /// Desired sample rate for the transmission
    pub sample_rate: u32,
    /// Audio-Duration of a single packet
    pub snippet_duration: u32,
    /// Desired amount of channels
    pub channels: u16,
}

pub struct Capture {
    _stream: cpal::Stream,
}

impl Capture {
    ///Creates a new Capture instance and starts the audio stream.
    pub fn new(config: CaptureConfig) -> Result<Self, CaptureError> {
        let host = cpal::default_host();
        let device = host
            .default_input_device()
            .ok_or(CaptureError::NoInputDevice)?;
        println!("Using input device: {}", device.name()?);

        let desired_sample_rate = cpal::SampleRate(config.sample_rate);
        let supported_configs_range = device
            .supported_input_configs()?
            .find(|c| {
                c.min_sample_rate().0 <= config.sample_rate
                    && c.max_sample_rate().0 >= config.sample_rate
                    && c.channels() == config.channels
            })
            .ok_or(CaptureError::NoSupportedConfig)?;

        let stream_config = supported_configs_range
            .with_sample_rate(desired_sample_rate)
            .config();
        println!("Using input config {:?}", stream_config);

        let opus_channels = match stream_config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("Invalid amount of channels"),
        };

        let mut encoder = Encoder::new(config.sample_rate, opus_channels, opus::Application::Voip)?;

        let samples_per_snippet =
            (stream_config.channels as u32 * config.sample_rate * config.snippet_duration / 1000)
                as usize;

        println!(
            "Sending audio data in snippets of {} ms ({} samples)",
            config.snippet_duration, samples_per_snippet
        );

        let mut audio_buffer: Vec<f32> = Vec::new();
        let mut opus_buffer = vec![0u8; 4096];

        let track = config.track;
        let handle = config.tokio_handle;
        let snippet_duration = Duration::from_millis(20);

        let input_callback = move |data: &[f32], _: &cpal::InputCallbackInfo| {
            audio_buffer.extend_from_slice(data);

            while audio_buffer.len() >= samples_per_snippet {
                let chunk = &audio_buffer[..samples_per_snippet];

                match encoder.encode_float(chunk, &mut opus_buffer) {
                    Ok(compressed_size) => {
                        let compressed_data = &opus_buffer[..compressed_size];

                        let track_clone = track.clone();

                        let data_copy = compressed_data.to_vec();

                        handle.spawn(async move {
                            let sample = Sample {
                                data: data_copy.into(),
                                duration: snippet_duration,
                                ..Default::default()
                            };

                            if let Err(e) = track_clone.write_sample(&sample).await {
                                eprintln!("Failed to write sample on audio-track: {}", e);
                            }
                        });
                    }
                    Err(err) => eprintln!("Opus encoding error: {}", err),
                }

                audio_buffer.drain(..samples_per_snippet);
            }
        };

        let error_callback = |err| {
            eprintln!("An error occured in the input stream: {}", err);
        };

        let stream =
            device.build_input_stream(&stream_config, input_callback, error_callback, None)?;
        stream.play().unwrap();

        Ok(Self { _stream: stream })
    }
}
