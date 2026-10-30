/// Audio Capture
mod agc;

use anyhow::Result;
use cpal::SupportedStreamConfigRange;
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
use rubato::ResampleError;
use rubato::Resampler;
use rubato::ResamplerConstructionError;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::time::Duration;
use thiserror::Error;
use tokio::io::ReadBuf;
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
    #[error("A cpal device enumeration error occured: {0}")]
    DevicesError(#[from] cpal::DevicesError),
    #[error("An invalid number of channels was provided {0}")]
    InvlaidChannelCount(u32),
    #[error("An invalid format was provided {0}")]
    UnsupportedFormat(SampleFormat),
    #[error("An Opus error occured: {0}")]
    OpusError(#[from] opus::Error),
    #[error("An cpal build stream error occured: {0}")]
    BuildStreamError(#[from] cpal::BuildStreamError),
    #[error("an rubato error occured: {0}")]
    Rubatoerror(#[from] ResamplerConstructionError),
    #[error("an rubato resample error occured: {0}")]
    RubatoResampleError(#[from] ResampleError),
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
    /// Prefred input device by name (None = default)
    pub input_device: Option<String>,
    /// Ring-buffer latency ms
    pub input_latency_ms: u32,
    /// Processing Settings
    pub processing: ProcessingConfig,
}

#[derive(Debug, Clone)]
pub struct ProcessingConfig {
    /// Vad open threshold
    pub vad_threshold: f32,
    /// Enable/disable nnnoise denoiser + speach-probability
    pub noise_suppression: bool,
    /// Enable AGC
    pub agc_enabled: bool,
    /// AGC target level (0..1)
    pub agc_target_level: f32,
    /// AGC max gain
    pub agc_max_gain: f32,
    /// High pass filter cutof in Hz
    pub high_pass_filter_hz: f32,
    /// Low pass filter cutof in Hz
    pub low_pass_filter_hz: f32,
}

pub struct Capture {
    _stream: cpal::Stream,
    volume: Arc<AtomicU32>,
    mute: Arc<AtomicBool>,
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

        let (prod, mut cons) = setup_ringbuffer(&stream_config, config.input_latency_ms);
        let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));
        let volume_clone = volume.clone();
        let mute = Arc::new(AtomicBool::new(false));
        let mute_clone = mute.clone();
        let stream = setup_input_stream(sample_format, device, &stream_config, prod)?;

        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown_signal.clone();
        let track = config.track.clone();

        let packet_duration = Duration::from_millis(config.snippet_duration as u64);

        let tokio_handle = config.tokio_handle.clone();
        let frame_size = (stream_config.sample_rate as f32
            * (config.snippet_duration as f32 / 1000.0)) as usize
            * stream_config.channels as usize;

        let (packet_tx, mut packet_rx) = tokio::sync::mpsc::channel::<(Vec<u8>, Duration)>(64);

        config.tokio_handle.spawn(async move {
            while let Some((payload, duration)) = packet_rx.recv().await {
                let sample = webrtc::media::Sample {
                    data: payload.into(),
                    duration,
                    ..Default::default()
                };
                if track.write_sample(&sample).await.is_err() {
                    break;
                }
            }
        });

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

            let mut processor = AudioProcessor::new(
                stream_config.sample_rate,
                stream_config.channels,
                volume_clone,
                mute_clone,
                &config.processing,
            )
            .expect("Failed to create processor");

            let mut read_buffer = vec![0.0f32; frame_size];
            let mut dropped_packets: u64 = 0;

            while !shutdown_clone.load(Relaxed) {
                if cons.occupied_len() >= frame_size {
                    cons.pop_slice(&mut read_buffer);

                    match processor.process(&read_buffer) {
                        Ok(data) if !data.is_empty() => {
                            if packet_tx
                                .try_send((data.to_vec(), packet_duration))
                                .is_err()
                            {
                                dropped_packets += 1;
                            }
                        }
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Encoding Error: {:?}", e);
                        }
                    }
                } else {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
            }

            if dropped_packets > 0 {
                eprintln!("Capture dropped {} packets", dropped_packets);
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
            volume,
            mute,
            worker_thread: Some(worker_thread),
            shutdown_signal,
        })
    }

    pub fn get_volume(&self) -> f32 {
        f32::from_bits(self.volume.load(Ordering::Relaxed))
    }

    pub fn set_volume(&mut self, value: f32) {
        self.volume.store(value.to_bits(), Ordering::Release);
    }

    pub fn get_mute(&self) -> bool {
        self.mute.load(Relaxed)
    }

    pub fn set_mute(&mut self, active: bool) {
        self.mute.store(active, Ordering::Release);
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        self.shutdown_signal.store(true, Ordering::Release);
        if let Some(handle) = self.worker_thread.take() {
            let _ = handle.join();
        }
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
    latency_ms: u32,
) -> (
    Caching<Arc<SharedRb<ringbuf::storage::Heap<f32>>>, true, false>,
    Caching<Arc<SharedRb<ringbuf::storage::Heap<f32>>>, false, true>,
) {
    let buffer_capacity = (stream_config.sample_rate as usize
        * stream_config.channels as usize
        * latency_ms as usize)
        / 1000;

    let rb = HeapRb::<f32>::new(buffer_capacity);
    rb.split()
}

fn setup_device(
    config: &CaptureConfig,
) -> Result<(Device, StreamConfig, SampleFormat), Box<dyn std::error::Error>> {
    let host = cpal::default_host();
    let device = match &config.input_device {
        Some(name) => host
            .input_devices()?
            .find(|device| {
                device
                    .description()
                    .map(|description| description.name() == name)
                    .unwrap_or(false)
            })
            .ok_or(CaptureError::NoInputDevice)?,
        None => host
            .default_input_device()
            .ok_or(CaptureError::NoInputDevice)?,
    };
    println!("Using input device: {}", device.description()?.name());

    let configs: Vec<_> = device.supported_input_configs()?.collect();
    let target_rate = config.sample_rate;
    let target_fmt = cpal::SampleFormat::F32;

    let supports_48k = |c: &SupportedStreamConfigRange| {
        (c.min_sample_rate()..=c.max_sample_rate()).contains(&target_rate)
    };

    let best_config = configs
        .iter()
        // `F32, Mono, 48k
        .find(|c| c.sample_format() == target_fmt && c.channels() == 1 && supports_48k(c))
        // F32, Stereo, 48k
        .or_else(|| {
            configs
                .iter()
                .find(|c| c.sample_format() == target_fmt && c.channels() == 2 && supports_48k(c))
        })
        // F32, any, 48k
        .or_else(|| {
            configs
                .iter()
                .find(|c| c.sample_format() == target_fmt && supports_48k(c))
        })
        // F32, Mono, any
        .or_else(|| {
            configs
                .iter()
                .find(|c| c.sample_format() == target_fmt && c.channels() == 1)
        })
        // F32, any, any
        .or_else(|| configs.iter().find(|c| c.sample_format() == target_fmt))
        .or_else(|| configs.first())
        .ok_or(CaptureError::NoSupportedConfig)?;

    let final_rate =
        target_rate.clamp(best_config.min_sample_rate(), best_config.max_sample_rate());
    let final_config = best_config.with_sample_rate(final_rate);

    Ok((device, final_config.config(), final_config.sample_format()))
}

struct AudioProcessor {
    input_channels: u16,
    encoder: Encoder,
    resampler: Option<rubato::FftFixedInOut<f32>>,
    resample_in_buffer: Vec<Vec<f32>>,
    resample_out_buffer: Vec<Vec<f32>>,
    fx_chain: Box<dyn AudioUnit>,
    post_chain: Box<dyn AudioUnit>,
    denoiser: DenoiseState<'static>,
    mono_buffer: Vec<f32>,
    denoise_buffer: Vec<f32>,
    opus_out: Vec<u8>,
    gate_level: f32,
    hold_frames: usize,
    output_staging_buffer: Vec<f32>,
    agc: agc::Agc,
    volume: Arc<AtomicU32>,
    mute: Arc<AtomicBool>,
    vad_theshold: f32,
    vad_close_theshold: f32,
    noise_supression: bool,
    agc_enabled: bool,
}

impl AudioProcessor {
    fn new(
        input_sample_rate: u32,
        input_channels: u16,
        volume: Arc<AtomicU32>,
        mute: Arc<AtomicBool>,
        processing: &ProcessingConfig,
    ) -> Result<Self, CaptureError> {
        let mut encoder = Encoder::new(48000, opus::Channels::Mono, opus::Application::Voip)?;
        encoder.set_bitrate(opus::Bitrate::Bits(32000))?;
        encoder.set_vbr(true)?;

        let chain = dcblock() >> highpass_hz(processing.high_pass_filter_hz, 0.707);
        let post_chain = lowpass_hz(processing.low_pass_filter_hz, 0.707);

        let mut fx_chain = Box::new(chain);
        fx_chain.set_sample_rate(input_sample_rate as f64);

        let mut fx_post_chain = Box::new(post_chain);
        fx_post_chain.set_sample_rate(48000 as f64);

        let denoiser = DenoiseState::new();

        let agc = agc::Agc::new(48000, 1);

        let (resampler, resample_in_buffer, resample_out_buffer) = if input_sample_rate != 48000 {
            let resampler =
                rubato::FftFixedInOut::<f32>::new(input_sample_rate as usize, 48000, 1024, 1)?;
            let in_buf = resampler.input_buffer_allocate(true);
            let out_buf = resampler.output_buffer_allocate(true);
            (Some(resampler), in_buf, out_buf)
        } else {
            (None, Vec::new(), Vec::new())
        };

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
            resampler,
            resample_in_buffer,
            resample_out_buffer,
            output_staging_buffer: Vec::with_capacity(4800),
            agc,
            volume,
            mute,
            vad_theshold: processing.vad_threshold,
            vad_close_theshold: processing.vad_threshold / 3.0,
            noise_supression: processing.noise_suppression,
            agc_enabled: processing.agc_enabled,
        })
    }

    fn process(&mut self, data: &[f32]) -> Result<&[u8], CaptureError> {
        self.mono_buffer.clear();

        const SCALE_UP: f32 = 32767.0;
        const SCALE_DOWN: f32 = 1.0 / 32767.0;

        // to Mono
        for frame in data.chunks_exact(self.input_channels as usize) {
            let sum: f32 = frame.iter().sum();
            let mono = sum / self.input_channels as f32;

            let filtered = self.fx_chain.filter_mono(mono);

            self.mono_buffer.push(filtered);
        }

        // resample
        if let Some(ref mut resampler) = self.resampler {
            while self.mono_buffer.len() >= resampler.input_frames_next() {
                let frames_needed = resampler.input_frames_next();

                self.resample_in_buffer[0].copy_from_slice(&self.mono_buffer[..frames_needed]);
                self.mono_buffer.drain(..frames_needed);

                let (_, len) = resampler
                    .process_into_buffer(
                        &self.resample_in_buffer,
                        &mut self.resample_out_buffer,
                        None,
                    )
                    .map_err(|e| CaptureError::RubatoResampleError(e))?;

                self.output_staging_buffer
                    .extend_from_slice(&self.resample_out_buffer[0][..len]);
            }
        } else {
            self.output_staging_buffer
                .extend(self.mono_buffer.drain(..));
        }

        let mut max_speech_prob = 0.0;

        // 20 ms
        if self.output_staging_buffer.len() >= 960 {
            let mut offset = 0;
            let chunk = &mut self.output_staging_buffer[..960];

            if self.noise_supression {
                while offset + 480 <= chunk.len() {
                    let sub_chunk = &mut chunk[offset..offset + 480];
                    sub_chunk.iter_mut().for_each(|f| *f *= SCALE_UP);
                    max_speech_prob = max_speech_prob.max(
                        self.denoiser
                            .process_frame(&mut self.denoise_buffer, sub_chunk),
                    );

                    chunk[offset..offset + 480].copy_from_slice(&self.denoise_buffer);
                    offset += 480;
                }
            } else {
                max_speech_prob = 1.0;
            }

            let open_threshold = self.vad_theshold;
            let close_threshold = self.vad_close_theshold;
            let release_speed = 0.02;
            let hold_time_samples = (48000.0 as f32 * 0.1) as usize;
            let volume: f32 = f32::from_bits(self.volume.load(Relaxed));
            let mute: bool = self.mute.load(Relaxed);

            let effective_volume = if mute { 0.0 } else { volume };

            let mut max_amp = 0.0;
            for sample in chunk.iter_mut() {
                *sample *= SCALE_DOWN;
                let abs = sample.abs();
                max_amp = max_amp.max(abs);
            }

            if max_amp > open_threshold && max_speech_prob > 0.6 {
                self.gate_level = 1.0;
                self.hold_frames = hold_time_samples;
            } else if max_amp < close_threshold || max_speech_prob <= 0.6 {
                if self.hold_frames > 0 {
                    self.hold_frames = self.hold_frames.saturating_sub(chunk.len())
                } else {
                    self.gate_level = (self.gate_level - release_speed).max(0.0);
                }
            }

            for sample in chunk.iter_mut() {
                *sample *= self.gate_level;
            }

            if self.agc_enabled && max_speech_prob > 0.6 {
                self.agc.process(chunk);
            }

            for sample in chunk.iter_mut() {
                *sample = self.post_chain.filter_mono(*sample) * effective_volume;
            }

            let output_size = self.encoder.encode_float(chunk, &mut self.opus_out)?;

            self.output_staging_buffer.drain(0..960);

            return Ok(&self.opus_out[..output_size]);
        }

        Ok(&[])
    }
}
