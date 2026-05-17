use anyhow::Result;
use daemon_protocol::voicechat::{
    AudioConfig, ConfigureRequest, ConnectionConfig, GetConfigResponse, ProcessingConfig,
    UserConfig,
};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

use crate::cli::ConfigSetArgs;

macro_rules! update_if_some {
    ($target:expr, $source:expr) => {
        if let Some(value) = $source {
            $target = value
        }
    };
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub user: User,
    pub connection: Connection,
    pub audio: Audio,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            user: User::default(),
            connection: Connection::default(),
            audio: Audio::default(),
        }
    }
}

impl From<GetConfigResponse> for Config {
    fn from(proto: GetConfigResponse) -> Self {
        let user = proto.user.unwrap_or_default();
        let connection = proto.connection.unwrap_or_default();
        let audio = proto.audio.unwrap_or_default();
        let processing = audio.processing.unwrap_or_default();

        Config {
            user: User { name: user.name },
            connection: Connection {
                server_ip: connection.server_ip,
                signaling_port: connection.signaling_port as u16,
                daemon_port: Connection::default().daemon_port,
                stun_servers: connection.stun_servers,
                ice_timeout_seconds: connection.ice_timeout_seconds,
            },
            audio: Audio {
                input_volume: audio.input_volume,
                output_volume: audio.output_volume,
                input_device: audio.input_device.unwrap_or_else(|| "default".to_string()),
                output_device: audio.output_device.unwrap_or_else(|| "default".to_string()),
                input_latency_ms: audio.input_latency_ms,
                output_jitter_buffer_ms: audio.output_jitter_buffer_ms,
                processing: Processing {
                    vad_threshold: processing.vad_threshold,
                    noise_suppression: processing.noise_supression,
                    automatic_gain_control: processing.automatic_gain_control,
                    agc_target_level: processing.agc_tartet_level,
                    agc_max_gain: processing.agc_max_gain,
                    high_pass_filter_hz: processing.high_pass_filter_hz,
                    low_pass_filter_hz: processing.low_pass_filter_hz,
                },
            },
        }
    }
}

impl Into<ConfigureRequest> for Config {
    fn into(self) -> ConfigureRequest {
        ConfigureRequest {
            user: Some(UserConfig {
                name: self.user.name,
            }),
            connection: Some(ConnectionConfig {
                server_ip: self.connection.server_ip,
                signaling_port: self.connection.signaling_port as u32,
                stun_servers: self.connection.stun_servers,
                ice_timeout_seconds: self.connection.ice_timeout_seconds,
            }),
            audio: Some(AudioConfig {
                input_volume: self.audio.input_volume,
                output_volume: self.audio.output_volume,
                input_device: Some(self.audio.input_device),
                output_device: Some(self.audio.output_device),
                input_latency_ms: self.audio.input_latency_ms,
                output_jitter_buffer_ms: self.audio.output_jitter_buffer_ms,
                processing: Some(ProcessingConfig {
                    vad_threshold: self.audio.processing.vad_threshold,
                    noise_supression: self.audio.processing.noise_suppression,
                    automatic_gain_control: self.audio.processing.automatic_gain_control,
                    agc_tartet_level: self.audio.processing.agc_target_level,
                    agc_max_gain: self.audio.processing.agc_max_gain,
                    high_pass_filter_hz: self.audio.processing.high_pass_filter_hz,
                    low_pass_filter_hz: self.audio.processing.low_pass_filter_hz,
                }),
            }),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct User {
    pub name: String,
}

impl Default for User {
    fn default() -> Self {
        Self {
            name: String::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Connection {
    /// IP of the signaling server
    pub server_ip: String,

    /// Port for the WebSocket signaling connection
    pub signaling_port: u16,

    /// Port for the local gRPC daemon
    pub daemon_port: u16,

    /// List of STUN servers to navigate NAT/Firewalls
    pub stun_servers: Vec<String>,

    /// Timeout in seconds before giving up on ICE candidate
    pub ice_timeout_seconds: u64,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            server_ip: "127.0.0.1".into(),
            signaling_port: 40000,
            daemon_port: 40001,
            stun_servers: vec![
                "stun:stun.l.google.com:19302".into(),
                "stun:stun1.l.google.com:19302".into(),
            ],
            ice_timeout_seconds: 5,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Audio {
    pub input_volume: f32,
    pub output_volume: f32,

    /// Specific name of the microphone to use (e.g. todo!)
    pub input_device: String,

    /// Specific name of the speakers to use (e.g. todo!)
    pub output_device: String,

    /// Input ring-buffer latency in milliseconds
    /// Higher = safer against dropouts, Lower = less latency
    pub input_latency_ms: u32,

    /// Jitter buffer latency in milliseconds for playback.
    ///  Controls how much audio gets buffered before playing to smooth out
    ///  network packet loss
    pub output_jitter_buffer_ms: u32,

    pub processing: Processing,
}

impl Default for Audio {
    fn default() -> Self {
        Self {
            input_volume: 1.0,
            output_volume: 1.0,
            input_device: "default".to_owned(),
            output_device: "default".to_owned(),
            input_latency_ms: 200,
            output_jitter_buffer_ms: 60,
            processing: Processing::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct Processing {
    /// Voice Activity Detection threshold (0.0 to 1.0)
    pub vad_threshold: f32,

    /// Enable/Disable Noise Suppression
    pub noise_suppression: bool,

    /// Enable/Disable Automatic Gain Control
    pub automatic_gain_control: bool,

    /// AGC Target Level
    pub agc_target_level: f32,

    /// AGC Max Gain
    pub agc_max_gain: f32,

    /// High-pass filter cutoff in Hz
    pub high_pass_filter_hz: f32,

    /// Low-pass filter cutoff in Hz
    pub low_pass_filter_hz: f32,
}

impl Default for Processing {
    fn default() -> Self {
        Self {
            vad_threshold: 0.4,
            noise_suppression: true,
            automatic_gain_control: true,
            agc_target_level: 0.05,
            agc_max_gain: 6.0,
            high_pass_filter_hz: 120.0,
            low_pass_filter_hz: 6000.0,
        }
    }
}

impl Config {
    pub fn load(path: Option<String>) -> Result<Self> {
        let config_path = Self::get_config_path(path);

        if let Some(path) = config_path {
            if path.exists() {
                let content = fs::read_to_string(path)?;

                return Ok(toml::from_str(&content)?);
            } else {
                let config = Config::default();
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, toml::to_string_pretty(&config)?)?;
                return Ok(config);
            }
        }
        Ok(Config::default())
    }

    pub fn save(&self, path: Option<String>) -> Result<()> {
        let config_path = Self::get_config_path(path);

        if let Some(path) = config_path {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, toml::to_string_pretty(self)?)?;
        }
        Ok(())
    }

    fn get_config_path(path: Option<String>) -> Option<PathBuf> {
        path.map(PathBuf::from).or_else(|| {
            directories::BaseDirs::new().map(|d| d.config_dir().join("voipyc").join("config.toml"))
        })
    }

    pub fn apply_args(&mut self, args: ConfigSetArgs) {
        // User
        update_if_some!(self.user.name, args.username);

        // Connection
        update_if_some!(self.connection.server_ip, args.server_addr);
        if let Some(port) = args.server_port {
            self.connection.signaling_port = port
        };
        if let Some(timeout) = args.ice_timeout_seconds {
            self.connection.ice_timeout_seconds = timeout as u64;
        }

        // Audio
        update_if_some!(self.audio.input_volume, args.input_volume);
        update_if_some!(self.audio.output_volume, args.output_volume);
        update_if_some!(self.audio.input_device, args.input_device);
        update_if_some!(self.audio.input_latency_ms, args.input_latency_ms);
        update_if_some!(self.audio.output_jitter_buffer_ms, args.jitter_buffer_ms);

        update_if_some!(self.audio.processing.vad_threshold, args.vad_threshold);
        update_if_some!(
            self.audio.processing.noise_suppression,
            args.noise_suppression
        );
        update_if_some!(
            self.audio.processing.automatic_gain_control,
            args.automatic_gain_control
        );
        update_if_some!(
            self.audio.processing.agc_target_level,
            args.agc_target_level
        );
        update_if_some!(self.audio.processing.agc_max_gain, args.agc_max_gain);
    }
}
