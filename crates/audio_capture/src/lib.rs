use anyhow::Result;
use cpal::traits::HostTrait;
use cpal::traits::{DeviceTrait, StreamTrait};
use opus::Encoder;
use rubato::{FftFixedInOut, Resampler, ResamplerConstructionError};
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use webrtc::media::Sample;
use webrtc::track::track_local::track_local_static_sample::TrackLocalStaticSample;
use webrtc_audio_processing::VoiceDetectionLikelihood;
use webrtc_audio_processing::{
    Config as ApConfig, GainControl, GainControlMode, InitializationConfig, Processor,
    VoiceDetection,
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

pub struct Capture {
    _stream: cpal::Stream,
    processor: Arc<Mutex<AudioProcessor>>,
}

struct AudioProcessor {
    ap: Processor,
    resampler: Option<Box<FftFixedInOut<f32>>>,
    input_channels: u16,
    output_channels: u16,
    ap_buffer: Vec<f32>,
    chunk_size: usize,
    volume: f32,
}

impl AudioProcessor {
    fn new(
        input_sample_rate: u32,
        input_channels: u16,
        ouptput_sample_rate: u32,
        output_channels: u16,
    ) -> Result<Self, CaptureError> {
        let processing_config = InitializationConfig {
            num_capture_channels: input_channels as i32,
            num_render_channels: input_channels as i32,
            ..InitializationConfig::default()
        };

        let mut ap = Processor::new(&processing_config).unwrap();

        let ap_config = ApConfig {
            echo_cancellation: None,
            gain_control: Some(GainControl {
                mode: GainControlMode::AdaptiveDigital,
                target_level_dbfs: 3,
                compression_gain_db: 15,
                enable_limiter: true,
            }),
            noise_suppression: None,
            voice_detection: Some(VoiceDetection {
                detection_likelihood: VoiceDetectionLikelihood::High,
            }),
            enable_transient_suppressor: true,
            enable_high_pass_filter: true,
        };
        ap.set_config(ap_config.clone());

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
            ap,
            resampler,
            input_channels,
            output_channels,
            ap_buffer: Vec::new(),
            chunk_size,
            volume: 1.0,
        })
    }

    fn set_volume(&mut self, volume: f32) {
        self.volume = volume;
    }

    fn process(&mut self, data: &[f32]) -> Vec<f32> {
        self.ap_buffer.extend_from_slice(data);

        let num_chunks = self.ap_buffer.len() / self.chunk_size;
        if num_chunks == 0 {
            return Vec::new();
        }

        let processable_len = num_chunks * self.chunk_size;
        let mut processable_data = self.ap_buffer.drain(..processable_len).collect::<Vec<_>>();

        for chunk in processable_data.chunks_mut(self.chunk_size) {
            if let Err(e) = self.ap.process_capture_frame(chunk) {
                eprintln!("audio processing error: {}", e);
            }
        }

        let mut processed_data = std::borrow::Cow::from(processable_data);


        if let Some(resampler) = &mut self.resampler {
            let channels = self.input_channels as usize;
            let num_frames = processed_data.len() / channels;
            let mut deinterleaved = vec![vec![0.0; num_frames]; channels];

            for i in 0..num_frames {
                for j in 0..channels {
                    deinterleaved[j][i] = processed_data[i * channels + j];
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

                    let mut interleaved = vec![0.0; resampled_frames * resampled_channels];

                    for i in 0..resampled_frames {
                        for j in 0..resampled_channels {
                            interleaved[i * resampled_channels + j] = resampled_deinterleaved[j][i];
                        }
                    }
                    processed_data = std::borrow::Cow::from(interleaved);
                }
                Err(e) => {
                    eprintln!("Resampling error: {}", e);
                    return Vec::new();
                }
            }
        }

        // Channel conversion
        if self.input_channels != self.output_channels {
            let mut converted = Vec::new();
            if self.input_channels == 2 && self.output_channels == 1 {
                for frame in processed_data.chunks_exact(2) {
                    converted.push((frame[0] + frame[1]) / 2.0);
                }
                processed_data = std::borrow::Cow::from(converted);
            } else if self.input_channels == 1 && self.output_channels == 2 {
                for sample in processed_data.iter() {
                    converted.push(*sample);
                    converted.push(*sample);
                }
                processed_data = std::borrow::Cow::from(converted);
            }
        }
        let mut processed_data = processed_data.into_owned();

        if self.volume != 1.0 {
            for sample in &mut processed_data {
                *sample *= self.volume;
            }
        }

        processed_data
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

        let mut audio_processor = Arc::new(Mutex::new(AudioProcessor::new(
            stream_config.sample_rate.0,
            stream_config.channels,
            config.sample_rate,
            config.channels,
        )?));

        let samples_per_snippet =
            (config.channels as u32 * config.sample_rate * config.snippet_duration / 1000) as usize;

        let opus_channels = match config.channels {
            1 => opus::Channels::Mono,
            2 => opus::Channels::Stereo,
            _ => panic!("Invalid amount of channels"),
        };

        let mut encoder = Encoder::new(config.sample_rate, opus_channels, opus::Application::Voip)?;

        let mut audio_buffer: Vec<f32> = Vec::new();
        let mut opus_buffer = vec![0u8; 4096];

        let (audio_sender, mut audio_receiver) = mpsc::unbounded_channel::<Vec<u8>>();
        let track = config.track;
        let snippet_duration_ms = config.snippet_duration;

        config.tokio_handle.spawn(async move {
            let snippet_duration = Duration::from_millis(snippet_duration_ms as u64);

            while let Some(data_copy) = audio_receiver.recv().await {
                let sample = Sample {
                    data: data_copy.into(),
                    duration: snippet_duration,
                    ..Default::default()
                };

                if let Err(e) = track.write_sample(&sample).await {
                    eprintln!("Failed to write sample on audio-track: {}", e);
                }
            }
        });

        let processor_clone = audio_processor.clone();
        let input_callback = move |data: &[f32], _: &cpal::InputCallbackInfo| {
            let processed_data = processor_clone.lock().unwrap().process(data);

            audio_buffer.extend_from_slice(&processed_data);

            while audio_buffer.len() >= samples_per_snippet {
                let chunk = &audio_buffer[..samples_per_snippet];

                match encoder.encode_float(chunk, &mut opus_buffer) {
                    Ok(compressed_size) => {
                        let compressed_data = &opus_buffer[..compressed_size];
                        if let Err(e) = audio_sender.send(compressed_data.to_vec()) {
                            eprintln!("Failed to send audio packet to channel: {}", e);
                        }
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

        Ok(Self {
            _stream: stream,
            processor: audio_processor,
        })
    }

    pub fn set_volume(&mut self, volume: f32) {
        if let Ok(mut processor) = self.processor.lock() {
            processor.set_volume(volume);
        }
    }
}
