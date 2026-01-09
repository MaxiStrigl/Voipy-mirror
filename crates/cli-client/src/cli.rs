use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(version, about, infer_subcommands = true)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    #[command(flatten)]
    pub config: ConfigArgs,
}

#[derive(Args, Debug)]
pub struct ConfigArgs {
    // --- Meta ---
    /// Save these configuration changes to the config file
    #[arg(long, global = true)]
    pub save: Option<bool>, 
    
    /// Use the provided config file 
    #[arg(long, global = true)]
    pub config_file: Option<String>,

    // --- User Settings ---
    /// Override username (display name)
    #[arg(long, global = true)]
    pub username: Option<String>,

    // --- Connection ---
    /// Signaling Server IP
    #[arg(long, global = true)]
    pub server_ip: Option<String>,

    /// Signaling Server Port  
    #[arg(long, global = true)]
    pub signaling_port: Option<u16>,

    /// Local Daemon Port 
    #[arg(long, global = true)]
    pub daemon_port: Option<u16>,

    /// Comma separated list of STUN servers
    #[arg(long, global = true, value_delimiter = ',')]
    pub stun_servers: Option<Vec<String>>,

    /// ICE candidate timeout in seconds
    #[arg(long, global = true)]
    pub ice_timeout_seconds: Option<u64>,

    // --- Audio Genera ---

    /// Input Volume multiplyer
    #[arg(long, global = true)]
    pub input_volume: Option<f32>,
    
    /// Output Volume multiplyer
    #[arg(long, global = true)]
    pub output_volume: Option<f32>,

    /// Input Device Name
    #[arg(long, global = true)]
    pub input_device: Option<String>,

    /// Output Device Name
    #[arg(long, global = true)]
    pub output_device: Option<String>,

    /// Input Latency (ms)
    #[arg(long, global = true)]
    pub input_latency_ms: Option<u32>,

    /// Output Jitter Buffer (ms)
    #[arg(long, global = true)]
    pub output_jitter_buffer_ms: Option<u32>,

    // --- Audio Processing ---
    
    /// VAD threshold (0.0 - 1.0)
    #[arg(long, global = true)]
    pub vad_threshold: Option<f32>,

    /// Enable/Disable Noise Supression
    #[arg(long, global = true)]
    pub noise_supression: Option<bool>,

    /// Enable/Disable AGC
    #[arg(long, global = true)]
    pub automatic_gain_control: Option<bool>,

    /// AGC target level
    #[arg(long, global = true)]
    pub agc_target_level: Option<f32>,

    /// AGC max gain
    #[arg(long, global = true)]
    pub agc_max_gain: Option<f32>,

    /// High-pass filter Hz
    #[arg(long, global = true)]
    pub high_pass_filter_hz: Option<f32>,

    /// Low-pass filter Hz
    #[arg(long, global = true)]
    pub low_pass_filter_hz: Option<f32>
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start a call with the other person connected to the server
    Call,
    ///  End current call
    Hangup,
    /// Get daemon status
    Status,
    
    /// Start the interactive command shell
    #[command(alias = "interacitve")]
    Shell,

    /// Start the visual TUI
    Tui,

    /// Shutdown the daemon
    Shutdown
}
