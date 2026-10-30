use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "voipyc")]
#[command(version, about, infer_subcommands = true)]
// #[command(arg_required_else_help = true)]
pub struct Cli {
    /// Local Daemon port (for grpc)
    #[arg(long, default_value = "40001")]
    pub grpc_port: u16,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Clone, Debug, PartialEq)]
pub enum Command {
    Connect {
        // --- Connection Management ---
        /// Connect to a signaling server
        address: String,
        /// Server port
        #[arg(short, long, default_value = "40000")]
        port: u16,
    },

    /// Disconnect from the current signaling server
    Disconnect,

    // --- Call Management ---
    /// Start a call with the connected peer
    Call,
    /// End the current call
    Hangup,

    // --- Audio Control (Runtime) ---
    /// Mute or unmute the microphone
    Mute {
        #[arg(action = clap::ArgAction::Set)]
        active: bool,
    },
    /// Deafen or undeafen the output (speakers)
    Deafen {
        #[arg(action = clap::ArgAction::Set)]
        active: bool,
    },
    /// Set input/output volume
    Volume {
        /// Volume level
        volume: f32,
        /// Apply to input (microphone)
        #[arg(short, long)]
        input: bool,
        /// Apply to output (speakers)
        #[arg(short, long, default_value_t = true, overrides_with("input"))]
        output: bool,
    },

    // --- Configuration --
    /// Manage daemon configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// List available audio devices
    Devices {
        /// List input devices
        #[arg(short, long)]
        input: bool,

        /// List output devices
        #[arg(short, long)]
        output: bool,
    },

    // --- Status & Monitoring ---
    /// Get current daemon status
    Status,
    /// Subscribe to real-time status
    Monitor,

    // -- System ---
    /// Shutdown the daemon gracefully
    Shutdown,
    /// Start the interactive command shell
    #[command(alias = "interactive")]
    Shell,
    /// Start the visual TUI
    Tui,
}

#[derive(Subcommand, Clone, Debug, PartialEq)]
pub enum ConfigAction {
    /// Get the current value of a setting (e.g. 'user.name')
    Get { key: Option<String> },
    /// Manually set configuration variables.
    Set(ConfigSetArgs),
    /// Load configuration from a specified file and apply it.
    Load {
        /// Path to the .toml config file. (Defaults to the OS config directory.)
        path: Option<String>,
    },
    /// Save the current configuration to a file
    Save {
        /// Path to save the file to. (Defaults to the OS config directory.)
        path: Option<String>,
    },
    /// List all available configuration keys and their current values
    List,
}

#[derive(Args, Clone, Debug, PartialEq)]
pub struct ConfigSetArgs {
    // --- User Settings -
    /// Override username
    #[arg(long)]
    pub username: Option<String>,

    // --- Connection Settings ---
    /// Signaling Server IP/URL
    #[arg(long)]
    pub server_addr: Option<String>,

    /// Signaling Server Port
    #[arg(long)]
    pub server_port: Option<u16>,

    /// ICE candidate timeout in seconds
    #[arg(long)]
    pub ice_timeout_seconds: Option<u64>,

    #[arg(long)]
    pub stun_servers: Option<Vec<String>>,

    // --- Audio General ---
    /// Input Volume multiplier
    #[arg(long)]
    pub input_volume: Option<f32>,

    /// Output Volume multiplier
    #[arg(long)]
    pub output_volume: Option<f32>,

    /// Input Device Name
    #[arg(long)]
    pub input_device: Option<String>,

    /// Output Device Name
    #[arg(long)]
    pub output_device: Option<String>,

    /// Input Latency (ms)
    #[arg(long)]
    pub input_latency_ms: Option<u32>,

    /// Jitter Buffer (ms)
    #[arg(long)]
    pub jitter_buffer_ms: Option<u32>,

    // --- Audio Processing ---
    /// (Voice Activation Detection) VAD threshold (0.0 - 1.0)
    #[arg(long)]
    pub vad_threshold: Option<f32>,

    /// Enable/Disable Noise Suppression
    #[arg(long)]
    pub noise_suppression: Option<bool>,

    #[arg(long)]
    pub high_pass_filter_hz: Option<f32>,

    #[arg(long)]
    pub low_pass_filter_hz: Option<f32>,

    /// Enable/Disable Automatic Gain Control (AGC)
    #[arg(long)]
    pub automatic_gain_control: Option<bool>,

    /// AGC target level
    #[arg(long)]
    pub agc_target_level: Option<f32>,

    /// AGC max gain
    #[arg(long)]
    pub agc_max_gain: Option<f32>,
}
