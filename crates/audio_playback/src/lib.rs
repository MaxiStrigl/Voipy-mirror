use anyhow::Result;
use cpal::traits::DeviceTrait;
use cpal::traits::{HostTrait, StreamTrait};
use opus::Decoder;
use ringbuf::HeapRb;
use ringbuf::traits::*;
use rubato::{Resampler, ResamplerConstructionError};
use std::borrow::Cow;
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
    #[error("An rubato error occured: {0}")]
    RubatoError(#[from] ResamplerConstructionError),
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
    pub channels: u16,
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
        let mut supported_configs: Vec<_> = device.supported_output_configs()?.collect();

        supported_configs.sort_by(|a, b| b.max_sample_rate().cmp(&a.max_sample_rate()));

        let supported_config_range = supported_configs
            .iter()
            .find(|c| {
                c.channels() == config.channels
                    && c.min_sample_rate() <= desired_sample_rate
                    && c.max_sample_rate() >= desired_sample_rate
            })
            .or_else(|| {
                supported_configs.iter().find(|c| {
                    c.channels() == 2
                        && c.min_sample_rate() <= desired_sample_rate
                        && c.max_sample_rate() >= desired_sample_rate
                })
            })
            .or_else(|| {
                supported_configs.iter().find(|c| {
                    c.channels() == 1
                        && c.min_sample_rate() <= desired_sample_rate
                        && c.max_sample_rate() >= desired_sample_rate
                })
            })
            // No config supports our rate take best we can get
            .or_else(|| {
                supported_configs
                    .iter()
                    .find(|c| c.channels() == config.channels)
                    .or_else(|| supported_configs.iter().find(|c| c.channels() == 2))
                    .or_else(|| supported_configs.iter().find(|c| c.channels() == 1))
            })
            // Just to be max robust: If we still did not find anything take whatever we can get
            .or_else(|| supported_configs.iter().next())
            .ok_or(PlaybackError::NoSupportedConfig)?;

        let sample_rate_to_use = if supported_config_range.min_sample_rate() <= desired_sample_rate
            && supported_config_range.max_sample_rate() >= desired_sample_rate
        {
            desired_sample_rate
        } else {
            supported_config_range.max_sample_rate()
        };

        let stream_config = supported_config_range
            .with_sample_rate(sample_rate_to_use)
            .config();

        println!("Using input config {:?}", stream_config);

        let opus_channels = match config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("Invalid amount of channels"),
        };

        let mut resampler = if stream_config.sample_rate.0 != config.sample_rate {
            println!(
                "Input sample rate ({}) does not match desired one {}. Using Resampler.",
                stream_config.sample_rate.0, config.sample_rate
            );

            let resampler = rubato::FftFixedInOut::<f32>::new(
                config.sample_rate as usize,
                stream_config.sample_rate.0 as usize,
                1024,
                config.channels as usize,
            )?;
            Some(Box::new(resampler))
        } else {
            None
        };

        let mut decoder = Decoder::new(config.sample_rate, opus_channels)?;

        let ringbuffer = HeapRb::<f32>::new(9600);
        let (mut producer, mut consumer) = ringbuffer.split();

        let track = config.track;
        let input_channels = config.channels;
        let output_channels = stream_config.channels;

        config.tokio_handle.spawn(async move {
            let mut opus_buffer = [0f32; 11520]; // 48000 Sample rate * 2 Channels * 0.120 ms (opus max)
            let mut last_seq:Option<u16> = None;
            loop {
                match track.read_rtp().await {
                    Ok((packet, _)) => {
                        let seq = packet.header.sequence_number;

                        if let Some(prev_seq) = last_seq {
                            let expected_seq = prev_seq.wrapping_add(1);
                            if seq != expected_seq {
                                let lost = seq.wrapping_sub(prev_seq).saturating_sub(1);
                                println!("Playback: Packet loss detected. From seq {} to {}. Lost ~{} packets.", prev_seq, seq, lost);
                            }
                        }
                        last_seq = Some(seq);

                        let compressed = packet.payload;

                        match decoder.decode_float(&compressed, &mut opus_buffer, false) {
                            Ok(num_decoded_samples_per_channel) => {
                                // let total_samples =
                                //     num_decoded_samples_per_channel * config.channels as usize;
                                // producer.push_slice(&opus_buffer[..total_samples]);

                                let decoded_samples = &opus_buffer
                                    [..num_decoded_samples_per_channel * input_channels as usize];

                                let mut processed_data = Cow::from(decoded_samples);

                                if let Some(resampler) = &mut resampler {
                                    let channels = input_channels as usize;
                                    let num_frames = decoded_samples.len() / channels;
                                    let mut deinterleaved = vec![vec![0.0; num_frames]; channels];

                                    for i in 0..num_frames {
                                        for j in 0..channels {
                                            deinterleaved[j][i] = decoded_samples[i * channels + j];
                                        }
                                    }

                                    match resampler.process(&deinterleaved, None) {
                                        Ok(resampled_deinterleaved) => {
                                            let resampled_channels = resampled_deinterleaved.len();
                                            let resampled_frames = if resampled_channels > 0 {
                                                resampled_deinterleaved[0].len()
                                            } else {
                                                0
                                            };

                                            let mut interleaved =
                                                vec![0.0; resampled_frames * resampled_channels];

                                            for i in 0..resampled_frames {
                                                for j in 0..resampled_channels {
                                                    interleaved[i * resampled_channels + j] =
                                                        resampled_deinterleaved[j][i];
                                                }
                                            }
                                            processed_data = std::borrow::Cow::from(interleaved);
                                        }

                                        Err(e) => {
                                            eprintln!("Resampling error: {}", e);
                                            return;
                                        }
                                    }
                                }

                                if input_channels != output_channels {
                                    let mut converted = Vec::new();
                                    if input_channels == 2 && output_channels == 1 {
                                        for frame in processed_data.chunks_exact(2) {
                                            converted.push((frame[0] + frame[1]) / 2.0);
                                        }
                                        processed_data = std::borrow::Cow::from(converted);
                                    } else if input_channels == 1 && output_channels == 2 {
                                        for sample in processed_data.iter() {
                                            converted.push(*sample);
                                            converted.push(*sample);
                                        }
                                        processed_data = std::borrow::Cow::from(converted);
                                    }
                                }

                                producer.push_slice(&processed_data);
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
            let mut underruns = 0;

            for sample in data.iter_mut() {
                *sample = match consumer.try_pop() {
                    Some(s) => s,
                    None => {
                        underruns += 1;
                        0.0
                    }
                }
            }

            if underruns > 0 {
                println!(
                    "Playback Audio Buffer underrun. Played {} slices of silence",
                    underruns
                );
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
