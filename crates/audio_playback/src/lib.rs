use anyhow::Result;
use cpal::traits::DeviceTrait;
use cpal::traits::{HostTrait, StreamTrait};
use opus::Decoder;
use ringbuf::HeapRb;
use ringbuf::traits::*;
use std::sync::Arc;
use thiserror::Error;
use tokio::runtime::Handle;
use webrtc::track::track_remote::TrackRemote;

/// Custom error type for the playback library.
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
    /// The WebRTC track from which to read audio samples.
    pub track: Arc<TrackRemote>,
    /// Desired sample rate for the output audio.
    pub sample_rate: u32,
    /// Tokio handle for spawning the receiving task.
    pub tokio_handle: Handle,
    /// Number of Channels
    pub channels: u32,
}

pub struct Playback {
    _stream: cpal::Stream,
}

impl Playback {
    /// Creates a new Playback instance and starts the audio stream.
    pub fn new(config: PlaybackConfig) -> Result<Self, PlaybackError> {
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

        let opus_channels = match stream_config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("Invalid amount of channels"),
        };

        let mut decoder = Decoder::new(config.sample_rate, opus_channels)?;

        let ringbuffer = HeapRb::<f32>::new(9600);
        let (mut producer, mut consumer) = ringbuffer.split();

        let track = config.track;

        config.tokio_handle.spawn(async move {
            let mut opus_buffer = [0f32; 11520]; // 48000 Sample rate * 2 Channels * 0.120 ms (opus max)

            loop {
                match track.read_rtp().await {
                    Ok((packet, _)) => {
                        let compressed = packet.payload;

                        match decoder.decode_float(&compressed, &mut opus_buffer, false) {
                            Ok(num_decoded_samples_per_channel) => {
                                let total_samples =
                                    num_decoded_samples_per_channel * config.channels as usize;
                                producer.push_slice(&opus_buffer[..total_samples]);
                            }
                            Err(e) => {
                                eprintln!("Failed to decode opus packet: {}", e);
                            }
                        }
                    }
                    Err(e) => {
                        println!("RTP track read finished: {}", e);
                        break;
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
            eprintln!("An error occured in the output stream: {}", err);
        };

        let stream =
            device.build_output_stream(&stream_config, output_callback, error_callback, None)?;
        stream.play().unwrap();

        Ok(Self { _stream: stream })
    }
}
