mod agc;

use agc::Agc;
use anyhow::Result;
use core::num;
use cpal::traits::HostTrait;
use cpal::traits::{DeviceTrait, StreamTrait};
use fundsp::hacker::*;
use opus::Encoder;
use ringbuf::HeapRb;
use ringbuf::traits::{Consumer, Observer, Producer, Split};
use rubato::{FftFixedInOut, Resampler, ResamplerConstructionError};
use std::sync::Mutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::{Acquire, Relaxed};
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use webrtc::media::Sample;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

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
    #[error("An rubato error occured: {0}")]
    RubatoError(#[from] ResamplerConstructionError),
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

struct SharedVolume(AtomicU32);

impl SharedVolume {
    fn new(val: f32) -> Self {
        Self(AtomicU32::new(val.to_bits()))
    }
    fn get(&self) -> f32 {
        f32::from_bits(self.0.load(Relaxed))
    }
    fn set(&self, val: f32) {
        self.0.store(val.to_bits(), Relaxed);
    }
}

pub struct Capture {
    _stream: cpal::Stream,
    volume_handle: Arc<SharedVolume>,
}

struct AudioProcessor {
    resampler: Option<Box<FftFixedInOut<f32>>>,
    input_channels: u16,
    output_channels: u16,
    pre_fx: Box<dyn AudioUnit>,
    post_fx: Box<dyn AudioUnit>,
    volume: f32,
    agc: Agc,
    input_scratch: Vec<Vec<f32>>,
    output_scratch: Vec<Vec<f32>>,
    output_buffer: Vec<f32>,
}

impl AudioProcessor {
    fn new(
        input_sample_rate: u32,
        input_channels: u16,
        ouptput_sample_rate: u32,
        output_channels: u16,
    ) -> Result<Self, CaptureError> {
        let hpf = highpass_hz(80.0, 1.0);

        let threshold = 0.00001;
        let release = 0.3;

        let gate_logic = shape_fn(move |env: f32| if env > threshold { 1.0 } else { 0.0 });

        let noise_gate = (pass() ^ (follow(release) >> gate_logic)) >> (pass() * pass());

        // let mut pre_chain = Box::new(hpf >> noise_gate);
        let mut pre_chain = Box::new(hpf >> noise_gate);
        pre_chain.set_sample_rate(input_sample_rate as f64);
        pre_chain.reset();

        let limiter = limiter(0.002, 0.1);
        // let mut post_chain = Box::new(limiter);
        let mut post_chain = Box::new(pass());
        post_chain.set_sample_rate(input_sample_rate as f64);
        post_chain.reset();

        let agc = Agc::new(input_sample_rate, input_channels.into());

        let max_input_frames = 2048;
        let ratio = input_sample_rate as f32 / ouptput_sample_rate as f32;
        let max_output_frames = (max_input_frames as f32 * ratio).ceil() as usize + 10;

        let input_scratch = vec![vec![0.0; max_input_frames]; input_channels as usize];
        let output_scratch = vec![vec![0.0; max_output_frames]; output_channels as usize];

        let output_buffer = vec![0.0; max_output_frames * output_channels as usize];

        let resampler = if input_sample_rate != ouptput_sample_rate {
            println!(
                "Input sample rate ({}) does not match desired one {}. Using Resampler.",
                input_sample_rate, ouptput_sample_rate
            );

            let resampler = rubato::FftFixedInOut::<f32>::new(
                input_sample_rate as usize,
                ouptput_sample_rate as usize,
                1024,
                input_channels as usize,
            )?;
            Some(Box::new(resampler))
        } else {
            None
        };

        let samples_per_10_ms_per_channel = input_sample_rate as usize / 100;
        let chunk_size = samples_per_10_ms_per_channel * input_channels as usize;

        Ok(Self {
            resampler,
            input_channels,
            output_channels,
            pre_fx: pre_chain,
            post_fx: post_chain,
            volume: 1.0,
            agc,
            input_scratch,
            output_scratch,
            output_buffer,
        })
    }

    fn set_volume(&mut self, volume: f32) {
        self.volume = volume;
    }

    fn process(&mut self, data: &[f32]) -> &[f32] {
        let num_frames = data.len() / self.input_channels as usize;

        for i in 0..num_frames {
            for ch in 0..self.input_channels as usize {
                let sample_idx = i * self.input_channels as usize + ch;
                let mut sample = data[sample_idx];

                sample = self.pre_fx.filter_mono(sample);

                self.input_scratch[ch][i] = sample;
            }
        }

        // Calculate peak amplitude after pre_fx
        let mut peak_after_pre_fx = 0.0f32;
        for ch in 0..self.input_channels as usize {
            for i in 0..num_frames {
                peak_after_pre_fx = peak_after_pre_fx.max(self.input_scratch[ch][i].abs());
            }
        }
        let db_value = if peak_after_pre_fx > 0.0 {
            20.0 * peak_after_pre_fx.log10()
        } else {
            -120.0 // A very low dB value for silence
        };
        println!("Peak after pre_fx: {:.2} dB", db_value);

        let frames_after_resample: usize;

        if let Some(resampler) = &mut self.resampler {
            let inputs: Vec<&[f32]> = self
                .input_scratch
                .iter()
                .map(|v| &v[0..num_frames])
                .collect();

            let mut outputs: Vec<&mut [f32]> = self
                .output_scratch
                .iter_mut()
                .map(|v| v.as_mut_slice())
                .collect();

            match resampler.process_into_buffer(&inputs, &mut outputs, None) {
                Ok((_, len)) => frames_after_resample = len,
                Err(_) => return &[],
            }

            // let channels = self.input_channels as usize;
            // let num_frames = processed_data.len() / channels;
            // let mut deinterleaved = vec![vec![0.0; num_frames]; channels];
            //
            // for i in 0..num_frames {
            //     for j in 0..channels {
            //         deinterleaved[j][i] = processed_data[i * channels + j];
            //     }
            // }
            //
            // match resampler.process(&deinterleaved, None) {
            //     Ok(resampled_deinterleaved) => {
            //         let resampled_channels = resampled_deinterleaved.len();
            //         let resampled_frames = if resampled_channels > 0 {
            //             resampled_deinterleaved[0].len()
            //         } else {
            //             0
            //         };
            //
            //         let mut interleaved = vec![0.0; resampled_frames * resampled_channels];
            //
            //         for i in 0..resampled_frames {
            //             for j in 0..resampled_channels {
            //                 interleaved[i * resampled_channels + j] = resampled_deinterleaved[j][i];
            //             }
            //         }
            //         processed_data = std::borrow::Cow::from(interleaved);
            //     }
            //     Err(e) => {
            //         eprintln!("Resampling error: {}", e);
            //         return Vec::new();
            //     }
            // }
        } else {
            frames_after_resample = num_frames;
            for ch in 0..self.input_channels as usize {
                self.output_scratch[ch][0..num_frames]
                    .copy_from_slice(&self.input_scratch[ch][0..num_frames]);
            }
        }

        // Channel conversion
        // if self.input_channels != self.output_channels {
        //     let mut converted = Vec::new();
        //     if self.input_channels == 2 && self.output_channels == 1 {
        //         for frame in processed_data.chunks_exact(2) {
        //             converted.push((frame[0] + frame[1]) / 2.0);
        //         }
        //         processed_data = std::borrow::Cow::from(converted);
        //     } else if self.input_channels == 1 && self.output_channels == 2 {
        //         for sample in processed_data.iter() {
        //             converted.push(*sample);
        //             converted.push(*sample);
        //         }
        //         processed_data = std::borrow::Cow::from(converted);
        //     }
        // }
        // processed_data.to_vec()

        for i in 0..frames_after_resample {
            if self.input_channels == 2 && self.output_channels == 1 {
                // Stereo to Mono
                let left = self.output_scratch[0][i];
                let right = self.output_scratch[1][i];
                self.output_buffer[i] = (left + right) / 2.0;
            } else if self.input_channels == 1 && self.output_channels == 2 {
                let sample = self.output_scratch[0][i];
                self.output_buffer[i * 2] = sample;
                self.output_buffer[i * 2 + 1] = sample;
            } else {
                for ch in 0..self.output_channels as usize {
                    self.output_buffer[i * self.input_channels as usize + ch] =
                        self.output_scratch[ch][i];
                }
            }
        }

        let total_frames = frames_after_resample * self.output_channels as usize;
        &self.output_buffer[0..total_frames]
    }
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
        let mut supported_configs: Vec<_> = device.supported_input_configs()?.collect();

        supported_configs.sort_by(|a, b| b.max_sample_rate().cmp(&a.max_sample_rate()));

        // Fund the best config
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
            .ok_or(CaptureError::NoSupportedConfig)?;

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

        let mut processor = AudioProcessor::new(
            stream_config.sample_rate.0,
            stream_config.channels,
            config.sample_rate,
            config.channels,
        )?;

        let samples_per_snippet =
            (config.channels as u32 * config.sample_rate * config.snippet_duration / 1000) as usize;

        let opus_channels = match config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("Invalid amount of channels"),
        };

        let mut encoder = Encoder::new(config.sample_rate, opus_channels, opus::Application::Voip)?;

        // let mut audio_buffer: Vec<f32> = Vec::new();
        // let mut opus_buffer = vec![0u8; 4096];

        let (audio_sender, mut audio_receiver) = mpsc::unbounded_channel::<Vec<u8>>();
        let track = config.track;
        let snippet_duration_ms = config.snippet_duration;

        let ringbuffer_size = config.sample_rate as usize * config.channels as usize;
        let rb = HeapRb::<f32>::new(ringbuffer_size);
        let (mut prod, mut cons) = rb.split();

        let shared_volume = Arc::new(SharedVolume::new(1.0));
        let shared_volume_clone = shared_volume.clone();

        config.tokio_handle.spawn_blocking(move || {
            let mut hardware_buffer = vec![0.0; 1024];
            let mut opus_buffer = vec![0u8; 4096];
            let mut final_delivery_buffer: Vec<f32> = Vec::new();

            loop {
                let available = cons.occupied_len();
                if available > 0 {
                    let read = cons.pop_slice(&mut hardware_buffer);
                    let frames = read / stream_config.channels as usize;

                    let vol = shared_volume_clone.get();

                    for f in 0..frames {
                        let idx = f * stream_config.channels as usize;
                        let sample = hardware_buffer[idx] * vol;

                        let filtered_sample = processor.pre_fx.filter_mono(sample);

                        for ch in 0..processor.input_channels as usize {
                            processor.input_scratch[ch][f] = filtered_sample;
                        }
                    }

                    // resample

                    let frames_out = if let Some(resampler) = &mut processor.resampler {
                        let mut inputs = Vec::new();

                        for ch in 0..processor.input_channels as usize {
                            inputs.push(&processor.input_scratch[ch][..frames]);
                        }

                        let mut outputs: Vec<&mut [f32]> = processor
                            .output_scratch
                            .iter_mut()
                            .map(|v| v.as_mut_slice())
                            .collect();

                        match resampler.process_into_buffer(&inputs, &mut outputs, None) {
                            Ok((_, len)) => len,
                            Err(_) => 0,
                        }
                    } else {
                        for ch in 0..processor.input_channels as usize {
                            processor.output_scratch[ch][..frames]
                                .copy_from_slice(&processor.input_scratch[ch][..frames]);
                        }

                        frames
                    };

                    // post processing

                    for i in 0..frames_out {
                        if stream_config.channels == 2 && processor.output_channels == 1 {
                            // Stereo to Mono
                            let left = processor.output_scratch[0][i];
                            let right = processor.output_scratch[1][i];
                            processor.output_buffer[i] = (left + right) / 2.0;
                        } else if stream_config.channels == 1 && processor.output_channels == 2 {
                            let sample = processor.output_scratch[0][i];
                            processor.output_buffer[i * 2] = sample;
                            processor.output_buffer[i * 2 + 1] = sample;
                        } else {
                            for ch in 0..processor.output_channels as usize {
                                processor.output_buffer
                                    [i * processor.input_channels as usize + ch] =
                                    processor.output_scratch[ch][i];
                            }
                        }
                    }

                    let interleaved =
                        &processor.output_buffer[..frames_out * processor.output_channels as usize];

                    final_delivery_buffer.extend_from_slice(interleaved);

                    while final_delivery_buffer.len() > samples_per_snippet {
                        let chunk = &final_delivery_buffer[..samples_per_snippet];

                        match encoder.encode_float(chunk, &mut opus_buffer) {
                            Ok(size) => {
                                let compressed_data = &opus_buffer[..size];
                                if let Err(e) = audio_sender.send(compressed_data.to_vec()) {
                                    eprintln!("Failed to send audio packet to channel: {}", e);
                                }
                            }
                            Err(err) => eprintln!("Opus encoding error: {}", err),
                        }

                        final_delivery_buffer.drain(..samples_per_snippet);
                    }
                } else {
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        });

        config.tokio_handle.spawn(async move {
            let snippet_duration = Duration::from_millis(snippet_duration_ms as u64);

            while let Some(data) = audio_receiver.recv().await {
                let sample = Sample {
                    data: data.into(),
                    duration: snippet_duration,
                    ..Default::default()
                };

                if let Err(e) = track.write_sample(&sample).await {
                    eprintln!("Failed to write sample on audio-track: {}", e);
                }
            }
        });

        let input_callback = move |data: &[f32], _: &cpal::InputCallbackInfo| {
            prod.push_slice(data);
        };

        let error_callback = |err| {
            eprintln!("An error occured in the input stream: {}", err);
        };

        let stream =
            device.build_input_stream(&stream_config, input_callback, error_callback, None)?;
        stream.play().unwrap();

        Ok(Self {
            _stream: stream,
            volume_handle: shared_volume,
        })
    }

    pub fn set_volume(&mut self, volume: f32) {
        self.volume_handle.set(volume);
    }
}
