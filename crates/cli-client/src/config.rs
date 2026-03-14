use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};

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

    pub fn _save(&self, path: Option<String>) -> Result<()> {
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
}
