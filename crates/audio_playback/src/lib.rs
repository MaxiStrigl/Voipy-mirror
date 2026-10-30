use anyhow::Result;
use cpal::traits::DeviceTrait;
use cpal::traits::{HostTrait, StreamTrait};
use cpal::{Device, SampleFormat, StreamConfig, SupportedStreamConfigRange};
use opus::Decoder;
use ringbuf::HeapRb;
use ringbuf::traits::*;
use rubato::{ResampleError, Resampler, ResamplerConstructionError};
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use thiserror::Error;
use tokio::runtime::Handle;
use webrtc::track::track_remote::TrackRemote;

type ProdType = ringbuf::wrap::caching::Caching<
    Arc<ringbuf::SharedRb<ringbuf::storage::Heap<f32>>>,
    true,
    false,
>;
type ConsType = ringbuf::wrap::caching::Caching<
    Arc<ringbuf::SharedRb<ringbuf::storage::Heap<f32>>>,
    false,
    true,
>;

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
    #[error("An invalid format was provided {0}")]
    UnsupportedFormat(SampleFormat),
    #[error("An invalid number of channels was provided {0}")]
    InvlaidChannelCount(u32),
    #[error("An Opus error occured: {0}")]
    OpusError(#[from] opus::Error),
    #[error("An cpal build stream error occured: {0}")]
    BuildStreamError(#[from] cpal::BuildStreamError),
    #[error("An cpal devices enumerations error occured: {0}")]
    DevicesError(#[from] cpal::DevicesError),
    #[error("An rubato error occured: {0}")]
    RubatoError(#[from] ResamplerConstructionError),
    #[error("an rubato resample error occured: {0}")]
    RubatoResampleError(#[from] ResampleError),
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
    /// Prefered output device by name (None = default)
    pub output_device: Option<String>,
    /// Target jitter buffer ms
    pub jitter_buffer_ms: u32,
}

struct JitterMetrics {
    target_samples: Arc<AtomicU32>,
    min_samples: usize,
    max_samples: usize,
    underrun_count: usize,
}

impl JitterMetrics {
    fn new(sample_rate: u32, channels: u16, target_ms: u32) -> Self {
        let base = samples_for_ms(sample_rate, channels, 20);
        let target = samples_for_ms(sample_rate, channels, target_ms);

        Self {
            target_samples: Arc::new(AtomicU32::new(target.clamp(base, base * 25) as u32)),
            min_samples: base,
            max_samples: base * 25,
            underrun_count: 0,
        }
    }

    fn get_target_samples(&self) -> usize {
        self.target_samples.load(Ordering::Relaxed) as usize
    }

    /// Increase target if a underrun happens
    fn record_underrun(&mut self) {
        self.underrun_count += 1;
        let next = (self.get_target_samples() + self.min_samples).min(self.max_samples);
        self.target_samples.store(next as u32, Ordering::Relaxed);
    }

    /// Slowly decay target if things are stable
    fn decay_target(&mut self) {
        if self.underrun_count == 0 {
            let next = (self.get_target_samples() - 1).max(self.max_samples);
            self.target_samples.store(next as u32, Ordering::Relaxed);
        } else {
            self.underrun_count = 0;
        }
    }
}

fn samples_for_ms(sample_rate: u32, channels: u16, ms: u32) -> usize {
    (sample_rate as usize * channels as usize * ms as usize) / 1000
}

pub struct Playback {
    _stream: cpal::Stream,
    volume: Arc<AtomicU32>,
    deafen: Arc<AtomicBool>,
    sample_rate: u32,
    channels: u16,
    jitter_target: Arc<AtomicU32>,
    shutdown: Arc<AtomicBool>,
}

impl Playback {
    pub fn new(config: PlaybackConfig) -> Result<Self, PlaybackError> {
        let (device, stream_config, sample_format) = setup_device(&config)?;

        let (mut prod, cons): (ProdType, ConsType) = setup_ringbuffer();
        let volume = Arc::new(AtomicU32::new(1.0f32.to_bits()));
        let deafen = Arc::new(AtomicBool::new(false));
        let shutdown = Arc::new(AtomicBool::new(false));

        let sample_rate = stream_config.sample_rate as u32;
        let channels = stream_config.channels;
        let metrics = JitterMetrics::new(sample_rate, channels, config.jitter_buffer_ms);
        let jitter_target = metrics.target_samples.clone();

        let stream = setup_output_stream(
            sample_format,
            device,
            &stream_config,
            cons,
            volume.clone(),
            deafen.clone(),
            metrics,
        )?;

        let track = config.track;
        let mut decoder = Decoder::new(48000, opus::Channels::Mono)?;
        let mut decoder_buffer = vec![0.0f32; 5760];

        let mut resample_staging_buffer = Vec::<f32>::new();

        let (mut resampler, mut resample_in_buf, mut resample_out_buf) =
            if stream_config.sample_rate != 48000 {
                let resampler = rubato::FftFixedInOut::<f32>::new(
                    48000,
                    stream_config.sample_rate as usize,
                    1024,
                    1,
                )?;
                let in_buf = resampler.input_buffer_allocate(true);
                let out_buf = resampler.output_buffer_allocate(true);

                (Some(resampler), in_buf, out_buf)
            } else {
                (None, Vec::new(), Vec::new())
            };

        let task_shutdown = shutdown.clone();

        // tokio buffer to receive packages over the internet
        config.tokio_handle.spawn(async move {
            while !task_shutdown.load(Ordering::Relaxed) {
                // while let Ok((packet, _)) = track.read_rtp().await {
                match track.read_rtp().await {
                    Ok((packet, _)) => {
                        let payload = packet.payload;

                        match decoder.decode_float(&payload, &mut decoder_buffer, false) {
                            Ok(count) => {
                                for i in 0..count {
                                    resample_staging_buffer.push(decoder_buffer[i]);
                                }
                            }
                            Err(_) => {
                                eprintln!("Failed to decode")
                            }
                        }

                        if let Some(ref mut resampler) = resampler {
                            while resample_staging_buffer.len() >= resampler.input_frames_next() {
                                let frames_needed = resampler.input_frames_next();

                                resample_in_buf[0]
                                    .copy_from_slice(&resample_staging_buffer[..frames_needed]);
                                resample_staging_buffer.drain(..frames_needed);

                                match resampler.process_into_buffer(
                                    &resample_in_buf,
                                    &mut resample_out_buf,
                                    None,
                                ) {
                                    Ok((_, len)) => {
                                        for &sample in &resample_out_buf[0][..len] {
                                            for _ in 0..stream_config.channels {
                                                let _ = prod.try_push(sample);
                                            }
                                        }
                                    }
                                    Err(e) => eprintln!("resample error: {}", e),
                                }
                            }
                        } else {
                            for &sample in resample_staging_buffer.iter() {
                                for _ in 0..stream_config.channels {
                                    let _ = prod.try_push(sample);
                                }
                            }
                            resample_staging_buffer.clear();
                        }
                    }
                    Err(_) => break,
                }
            }
        });

        stream.play().map_err(|_| {
            PlaybackError::BuildStreamError(cpal::BuildStreamError::DeviceNotAvailable)
        })?;

        Ok(Self {
            _stream: stream,
            volume,
            deafen,
            sample_rate,
            channels,
            jitter_target,
            shutdown,
        })
    }

    pub fn get_volume(&self) -> f32 {
        f32::from_bits(self.volume.load(Ordering::Relaxed))
    }

    pub fn set_volume(&mut self, value: f32) {
        self.volume.store(value.to_bits(), Ordering::Release);
    }

    pub fn get_deafen(&self) -> bool {
        self.deafen.load(Ordering::Relaxed)
    }

    pub fn set_deafen(&mut self, value: bool) {
        self.deafen.store(value, Ordering::Release);
    }

    pub fn set_jitter_buffer_ms(&mut self, ms: u32) {
        let base = samples_for_ms(self.sample_rate, self.channels, 20);
        let target = samples_for_ms(self.sample_rate, self.channels, ms);
        let clamped = target.clamp(base, base * 25) as u32;
        self.jitter_target.store(clamped, Relaxed);
    }
}

impl Drop for Playback {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

fn setup_output_stream(
    sample_format: SampleFormat,
    device: Device,
    stream_config: &StreamConfig,
    mut consumer: ConsType,
    volume: Arc<AtomicU32>,
    deafen: Arc<AtomicBool>,
    mut metrics: JitterMetrics,
) -> Result<cpal::Stream, PlaybackError> {
    let mut is_buffering = true;
    let mut decay_counter = 0;

    let mut process_audio = move |data: &mut [f32]| {
        let occupied = consumer.occupied_len();

        if is_buffering {
            if occupied >= metrics.get_target_samples() {
                is_buffering = false;
            } else {
                data.fill(0.0);
                return;
            }
        }

        let read = consumer.pop_slice(data);

        let is_deafened = deafen.load(Ordering::Relaxed);

        if is_deafened {
            data.fill(0.0);
        } else {
            let vol = f32::from_bits(volume.load(Ordering::Relaxed));

            if (vol - 1.0).abs() > f32::EPSILON {
                for sample in data.iter_mut() {
                    *sample *= vol;
                }
            }
        }

        if read < data.len() {
            // Underrun
            data[read..].fill(0.0);
            metrics.record_underrun();
            is_buffering = true;
        } else {
            // stability decay
            // every 500 cycles (about 5-10secs) I try to reduce the buffer size
            // I just randomly chose that value xD
            decay_counter += 1;
            if decay_counter > 250 {
                metrics.decay_target();
                decay_counter = 0;
            }
        }
    };

    let error_callback = move |e| {
        eprintln!("Output stream error: {}", e);
    };

    let stream = match sample_format {
        SampleFormat::F32 => device.build_output_stream(
            stream_config,
            move |data: &mut [f32], _| process_audio(data),
            error_callback,
            None,
        ),
        _ => {
            eprintln!("Output not implemented: {}", sample_format);
            return Err(PlaybackError::UnsupportedFormat(sample_format));
        }
    }?;
    Ok(stream)
}

fn setup_ringbuffer() -> (
    ringbuf::wrap::caching::Caching<
        Arc<ringbuf::SharedRb<ringbuf::storage::Heap<f32>>>,
        true,
        false,
    >,
    ringbuf::wrap::caching::Caching<
        Arc<ringbuf::SharedRb<ringbuf::storage::Heap<f32>>>,
        false,
        true,
    >,
) {
    let ring_buffer = HeapRb::<f32>::new(48000);
    ring_buffer.split()
}

fn setup_device(
    config: &PlaybackConfig,
) -> Result<(Device, StreamConfig, SampleFormat), PlaybackError> {
    let host = cpal::default_host();
    let device = match &config.output_device {
        Some(name) => host
            .devices()?
            .find(|d| d.description().map(|n| n.name() == name).unwrap_or(false))
            .ok_or(PlaybackError::NoOutputDevice)?,
        None => host
            .default_output_device()
            .ok_or(PlaybackError::NoOutputDevice)?,
    };

    println!("Using output device: {}", device.description()?.name());

    let configs: Vec<_> = device.supported_output_configs()?.collect();
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
        .ok_or(PlaybackError::NoSupportedConfig)?;

    let final_rate =
        target_rate.clamp(best_config.min_sample_rate(), best_config.max_sample_rate());
    let final_config = best_config.with_sample_rate(final_rate);

    Ok((device, final_config.config(), final_config.sample_format()))
}
