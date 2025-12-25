/// Audio Capture
///
mod agc;

use anyhow::Result;
use cpal::traits::DeviceTrait;
use cpal::traits::HostTrait;
use cpal::traits::StreamTrait;
use cpal::{BuildStreamError, Device, SampleFormat, Stream, StreamConfig};
use fundsp::hacker::*;
use nnnoiseless::DenoiseState;
use opus::Encoder;
use ringbuf::traits::Consumer;
use ringbuf::traits::{Observer, Producer, Split};
use ringbuf::wrap::caching::Caching;
use ringbuf::{HeapRb, SharedRb};
use rubato::ResamplerConstructionError;
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::time::Duration;
use thiserror::Error;
use tokio::runtime::Handle;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;

type ProdType = Caching<Arc<SharedRb<ringbuf::storage::Heap<f32>>>, true, false>;

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
    #[error("An invalid format was provided {0}")]
    UnsupportedFormat(SampleFormat),
    #[error("An Opus error occured: {0}")]
    OpusError(#[from] opus::Error),
    #[error("An cpal build stream error occured: {0}")]
    BuildStreamError(#[from] cpal::BuildStreamError),
    #[error("An rubato error occured: {0}")]
    RubatoError(#[from] ResamplerConstructionError),
}

/// Configurations for the audio capture stream
#[derive(Clone)]
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
    worker_thread: Option<std::thread::JoinHandle<()>>,
    shutdown_signal: Arc<AtomicBool>,
}

impl Capture {
    ///Creates a new Capture instance and starts the audio stream.
    pub fn new(config: CaptureConfig) -> Result<Self, CaptureError> {
        let (device, stream_config, sample_format) = match setup_device(&config) {
            Ok((d, c, f)) => (d, c, f),
            Err(_) => return Err(CaptureError::NoSupportedConfig),
        };

        let (prod, mut cons) = setup_ringbuffer(&stream_config);
        let volume_handle = Arc::new(SharedVolume::new(1.0));
        let stream = setup_input_stream(sample_format, device, &stream_config, prod)?;

        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown_signal.clone();
        let track = config.track.clone();
        let tokio_handle = config.tokio_handle.clone();

        let frame_size = (stream_config.sample_rate.0 as f32
            * (config.snippet_duration as f32 / 1000.0)) as usize
            * stream_config.channels as usize;

        let worker_thread = std::thread::spawn(move || {
            let core_ids = core_affinity::get_core_ids().unwrap();
            core_affinity::set_for_current(core_ids[0]);

            let thread_priority_handle =
                match audio_thread_priority::promote_current_thread_to_real_time(512, 48000) {
                    Ok(h) => Some(h),
                    Err(e) => {
                        eprintln!("Failed to promote worker thread to realtime: {}", e);
                        None
                    }
                };

            let mut processor =
                AudioProcessor::new(stream_config.sample_rate.0, stream_config.channels)
                    .expect("Failed to create processor");

            let mut read_buffer = vec![0.0f32; frame_size];

            while !shutdown_clone.load(Relaxed) {
                if cons.occupied_len() >= frame_size {
                    cons.pop_slice(&mut read_buffer);

                    let processed_data = match processor.process(&read_buffer) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("Encoding Error: {:?}", e);
                            continue;
                        }
                    };

                    let track_clone = track.clone();
                    let out_bytes = processed_data.to_vec();

                    tokio_handle.spawn(async move {
                        let _ = track_clone
                            .write_sample(&webrtc::media::Sample {
                                data: out_bytes.into(),
                                duration: Duration::from_millis(config.snippet_duration as u64),
                                ..Default::default()
                            })
                            .await;
                    });
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }

            if let Some(handle) = thread_priority_handle {
                let _ = audio_thread_priority::demote_current_thread_from_real_time(handle);
            }
        });

        stream.play().map_err(|_| {
            CaptureError::BuildStreamError(cpal::BuildStreamError::DeviceNotAvailable)
        })?;

        Ok(Self {
            _stream: stream,
            volume_handle,
            worker_thread: Some(worker_thread),
            shutdown_signal,
        })
    }
}

fn setup_input_stream(
    sample_format: SampleFormat,
    device: Device,
    stream_config: &StreamConfig,
    mut prod: ProdType,
) -> Result<Stream, BuildStreamError> {
    let stream = match sample_format {
        SampleFormat::F32 => device.build_input_stream(
            &stream_config,
            move |data: &[f32], _| {
                for &sample in data {
                    let _ = prod.try_push(sample);
                }
            },
            move |err| eprintln!("Stream error: {}", err),
            None,
        ),
        SampleFormat::I16 => device.build_input_stream(
            &stream_config,
            move |data: &[i16], _| {
                for &sample in data {
                    let _ = prod.try_push(sample as f32 / i16::MAX as f32);
                }
            },
            move |err| eprintln!("Stream error: {}", err),
            None,
        ),
        SampleFormat::U8 => device.build_input_stream(
            &stream_config,
            move |data: &[u8], _| {
                for &sample in data {
                    let _ = prod.try_push((sample as f32 - 128.0) / 128.0);
                }
            },
            move |err| eprintln!("Stream error: {}", err),
            None,
        ),
        _ => panic!("Not yet implemented: Format: {:?}", sample_format),
    };

    stream
}

fn setup_ringbuffer(
    stream_config: &StreamConfig,
) -> (
    Caching<Arc<SharedRb<ringbuf::storage::Heap<f32>>>, true, false>,
    Caching<Arc<SharedRb<ringbuf::storage::Heap<f32>>>, false, true>,
) {
    let latency_ms = 200;
    let buffer_capacity =
        (stream_config.sample_rate.0 as usize * stream_config.channels as usize * latency_ms)
            / 1000;

    let rb = HeapRb::<f32>::new(buffer_capacity);
    rb.split()
}

fn setup_device(
    config: &CaptureConfig,
) -> Result<(Device, StreamConfig, SampleFormat), Box<dyn std::error::Error>> {
    let host = cpal::default_host();
    let device = host
        .default_input_device()
        .ok_or(CaptureError::NoInputDevice)?;

    println!("Using input device: {}", device.name()?);

    let desired_sample_rate = cpal::SampleRate(config.sample_rate);
    let mut supported_configs: Vec<_> = device.supported_input_configs()?.collect();

    supported_configs.sort_by(|a, b| b.max_sample_rate().cmp(&a.max_sample_rate()));

    // Find the best config
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

    let sample_format = supported_config_range.sample_format();

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

    Ok((device, stream_config, sample_format))
}

struct AudioProcessor {
    input_channels: u16,
    encoder: Encoder,
    fx_chain: Box<dyn AudioUnit>,
    post_chain: Box<dyn AudioUnit>,
    denoiser: DenoiseState<'static>,
    mono_buffer: Vec<f32>,
    denoise_buffer: Vec<f32>,
    opus_out: Vec<u8>,
    gate_level: f32,
    hold_frames: usize,
}

impl AudioProcessor {
    fn new(input_sample_rate: u32, input_channels: u16) -> Result<Self, CaptureError> {
        let mut encoder = Encoder::new(
            input_sample_rate,
            opus::Channels::Mono,
            opus::Application::Voip,
        )?;

        encoder.set_bitrate(opus::Bitrate::Bits(32000))?;
        encoder.set_vbr(true)?;

        let chain = dcblock() >> highpass_hz(80.0, 0.707);
        let post_chain = lowpass_hz(10000.0, 0.707);

        let mut fx_chain = Box::new(chain);
        fx_chain.set_sample_rate(input_sample_rate as f64);

        let mut fx_post_chain = Box::new(post_chain);
        fx_post_chain.set_sample_rate(input_sample_rate as f64);

        let denoiser = DenoiseState::new();

        Ok(Self {
            input_channels,
            encoder,
            fx_chain,
            post_chain: fx_post_chain,
            denoiser: *denoiser,
            mono_buffer: Vec::new(),
            denoise_buffer: vec![0.0; 480],
            opus_out: vec![0u8; 4000],
            gate_level: 0.0,
            hold_frames: 0,
        })
    }

    fn process(&mut self, data: &[f32]) -> Result<&[u8], CaptureError> {
        self.mono_buffer.clear();

        const SCALE_UP: f32 = 32767.0;
        const SCALE_DOWN: f32 = 1.0 / 32767.0;

        for frame in data.chunks_exact(self.input_channels as usize) {
            let sum: f32 = frame.iter().sum();
            let mono = sum / self.input_channels as f32;

            let filtered = self.fx_chain.filter_mono(mono);

            self.mono_buffer.push(filtered * SCALE_UP);
        }

        let mut offset = 0;
        while offset + 480 <= self.mono_buffer.len() {
            let chunk = &self.mono_buffer[offset..offset + 480];
            self.denoiser.process_frame(&mut self.denoise_buffer, chunk);

            self.mono_buffer[offset..offset + 480].copy_from_slice(&self.denoise_buffer);

            offset += 480;
        }

        let open_threshold = 0.02;
        let close_threshold = 0.008;
        let release_speed = 0.005;
        let hold_time_samples = (48.000 as f32 * 0.1) as usize;

        let mut max_amp = 0.0;
        for sample in self.mono_buffer.iter_mut() {
            *sample *= SCALE_DOWN;

            let abs = sample.abs();
            max_amp = max_amp.max(abs);
        }

        if max_amp > open_threshold {
            self.gate_level = 1.0;
            self.hold_frames = hold_time_samples;
        } else if max_amp < close_threshold {
            if self.hold_frames > 0 {
                self.hold_frames -= Num::min(self.mono_buffer.len(), self.hold_frames);
            } else {
                self.gate_level = (self.gate_level - release_speed).max(0.0);
            }
        }

        for sample in self.mono_buffer.iter_mut() {
            *sample *= self.gate_level;
            *sample = self.post_chain.filter_mono(*sample);
        }

        let output_size = self
            .encoder
            .encode_float(&self.mono_buffer, &mut self.opus_out)?;

        Ok(&self.opus_out[..output_size])
    }
}
