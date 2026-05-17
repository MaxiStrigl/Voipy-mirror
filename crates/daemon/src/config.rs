use daemon_protocol::voicechat as dp;
use tonic::IntoRequest;
use webrtc::util::Conn;

const DEFAULT_SIGNALING_PORT: u16 = 40000;
const DEFAULT_ICE_TIMEOUT_SECONDS: u16 = 5;
const DEFAULT_LATENCY_MS: u32 = 200;
const DEFAULT_JITTER_MS: u32 = 200;

pub struct DaemonConfig {
    pub user: UserConfig,
    pub connection: ConnectionConfig,
    pub audio: AudioConfig,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            user: Default::default(),
            connection: Default::default(),
            audio: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct UserConfig {
    name: String,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub server_ip: String,
    pub signaling_port: u16,
    pub stun_servers: Vec<String>,
    pub ice_timeout_seconds: u64,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            server_ip: String::new(),
            signaling_port: DEFAULT_SIGNALING_PORT,
            stun_servers: Default::default(),
            ice_timeout_seconds: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AudioConfig {
    pub input_volume: f32,
    pub output_volume: f32,
    pub input_device: Option<String>,
    pub output_device: Option<String>,

    pub input_latency_ms: u32,
    pub output_jitter_buffer_ms: u32,
    pub processing: ProcessingConfig,
}

impl Default for AudioConfig {
    fn default() -> Self {
        Self {
            input_volume: 1.0,
            output_volume: 1.0,
            input_device: None,
            output_device: None,
            input_latency_ms: DEFAULT_LATENCY_MS,
            output_jitter_buffer_ms: DEFAULT_JITTER_MS,
            processing: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessingConfig {
    pub vad_threshold: f32,
    pub noise_suppression: bool,
    pub automatic_gain_control: bool,
    pub agc_target_level: f32,
    pub agc_max_gain: f32,
    pub high_pass_filter_hz: f32,
    pub low_pass_filter_hz: f32,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            vad_threshold: 0.3,
            noise_suppression: true,
            automatic_gain_control: false,
            agc_target_level: 0.05,
            agc_max_gain: 6.0,
            high_pass_filter_hz: 120.0,
            low_pass_filter_hz: 6000.0,
        }
    }
}

impl DaemonConfig {
    pub fn from_config_request(req: &dp::ConfigureRequest) -> Self {
        Self {
            user: req
                .user
                .as_ref()
                .map(UserConfig::from_request)
                .unwrap_or_default(),
            connection: req
                .connection
                .as_ref()
                .map(ConnectionConfig::from_request)
                .unwrap_or_default(),
            audio: req
                .audio
                .as_ref()
                .map(AudioConfig::from_request)
                .unwrap_or_default(),
        }
    }

    pub fn to_config_response(&self) -> dp::GetConfigResponse {
        dp::GetConfigResponse {
            user: Some(self.user.to_respone()),
            connection: Some(self.connection.to_respone()),
            audio: Some(self.audio.to_respone()),
        }
    }
}

impl UserConfig {
    fn from_request(req: &dp::UserConfig) -> Self {
        Self {
            name: req.name.clone(),
        }
    }

    fn to_respone(&self) -> dp::UserConfig {
        dp::UserConfig {
            name: self.name.clone(),
        }
    }
}

impl ConnectionConfig {
    fn from_request(req: &dp::ConnectionConfig) -> Self {
        Self {
            server_ip: req.server_ip.clone(),
            signaling_port: req.signaling_port as u16,
            stun_servers: req.stun_servers.clone(),
            ice_timeout_seconds: req.ice_timeout_seconds,
        }
    }

    fn to_respone(&self) -> dp::ConnectionConfig {
        dp::ConnectionConfig {
            server_ip: self.server_ip.clone(),
            signaling_port: self.signaling_port as u32,
            stun_servers: self.stun_servers.clone(),
            ice_timeout_seconds: self.ice_timeout_seconds,
        }
    }
}

impl AudioConfig {
    fn from_request(req: &dp::AudioConfig) -> Self {
        Self {
            input_volume: req.input_volume,
            output_volume: req.output_volume,
            input_device: req.input_device.clone(),
            output_device: req.output_device.clone(),
            input_latency_ms: req.input_latency_ms,
            output_jitter_buffer_ms: req.output_jitter_buffer_ms,
            processing: req
                .processing
                .as_ref()
                .map(ProcessingConfig::from_request)
                .unwrap_or_default(),
        }
    }

    fn to_respone(&self) -> dp::AudioConfig {
        dp::AudioConfig {
            input_volume: self.input_volume,
            output_volume: self.output_volume,
            input_device: self.input_device.clone(),
            output_device: self.output_device.clone(),
            input_latency_ms: self.input_latency_ms,
            output_jitter_buffer_ms: self.output_jitter_buffer_ms,
            processing: Some(self.processing.to_respone()),
        }
    }
}

impl ProcessingConfig {
    fn from_request(req: &dp::ProcessingConfig) -> Self {
        Self {
            vad_threshold: req.vad_threshold,
            noise_suppression: req.noise_supression,
            automatic_gain_control: req.automatic_gain_control,
            agc_target_level: req.agc_tartet_level,
            agc_max_gain: req.agc_max_gain,
            high_pass_filter_hz: req.high_pass_filter_hz,
            low_pass_filter_hz: req.low_pass_filter_hz,
        }
    }

    fn to_respone(&self) -> dp::ProcessingConfig {
        dp::ProcessingConfig {
            vad_threshold: self.vad_threshold,
            noise_supression: self.noise_suppression,
            automatic_gain_control: self.automatic_gain_control,
            agc_tartet_level: self.agc_target_level,
            agc_max_gain: self.agc_max_gain,
            high_pass_filter_hz: self.high_pass_filter_hz,
            low_pass_filter_hz: self.low_pass_filter_hz,
        }
    }
}
