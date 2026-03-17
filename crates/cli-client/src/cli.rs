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
    pub ice_timeout_seconds: Option<u16>,

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

// #[derive(Args, Debug)]
// pub struct ConfigArgs {
//     // --- Meta ---
//     /// Don't save these configuration changes to the config file
//     #[arg(long, global = true)]
//     pub dont_save: Option<bool>,

//     /// Use the provided config file
//     #[arg(long, global = true)]
//     pub config_file: Option<String>,

//     // --- User Settings ---
//     /// Override username (display name)
//     #[arg(long, global = true)]
//     pub username: Option<String>,

//     // --- Connection ---
//     /// Signaling Server IP
//     #[arg(long, global = true)]
//     pub server_ip: Option<String>,

//     /// Signaling Server Port
//     #[arg(long, global = true)]
//     pub signaling_port: Option<u16>,

//     /// Local Daemon Port
//     #[arg(long, global = true, default_value = "40001")]
//     pub daemon_port: u16,

//     /// Comma separated list of STUN servers
//     #[arg(long, global = true, value_delimiter = ',')]
//     pub stun_servers: Option<Vec<String>>,

//     /// ICE candidate timeout in seconds
//     #[arg(long, global = true)]
//     pub ice_timeout_seconds: Option<u64>,

//     // --- Audio Genera ---
//     /// Input Volume multiplyer
//     #[arg(long, global = true)]
//     pub input_volume: Option<f32>,

//     /// Output Volume multiplyer
//     #[arg(long, global = true)]
//     pub output_volume: Option<f32>,

//     /// Input Device Name
//     #[arg(long, global = true)]
//     pub input_device: Option<String>,

//     /// Output Device Name
//     #[arg(long, global = true)]
//     pub output_device: Option<String>,

//     /// Input Latency (ms)
//     #[arg(long, global = true)]
//     pub input_latency_ms: Option<u32>,

//     /// Output Jitter Buffer (ms)
//     #[arg(long, global = true)]
//     pub output_jitter_buffer_ms: Option<u32>,

//     // --- Audio Processing ---
//     /// VAD threshold (0.0 - 1.0)
//     #[arg(long, global = true)]
//     pub vad_threshold: Option<f32>,

//     /// Enable/Disable Noise Suppression
//     #[arg(long, global = true)]
//     pub noise_supression: Option<bool>,

//     /// Enable/Disable AGC
//     #[arg(long, global = true)]
//     pub automatic_gain_control: Option<bool>,

//     /// AGC target level
//     #[arg(long, global = true)]
//     pub agc_target_level: Option<f32>,

//     /// AGC max gain
//     #[arg(long, global = true)]
//     pub agc_max_gain: Option<f32>,

//     /// High-pass filter Hz
//     #[arg(long, global = true)]
//     pub high_pass_filter_hz: Option<f32>,

//     /// Low-pass filter Hz
//     #[arg(long, global = true)]
//     pub low_pass_filter_hz: Option<f32>,
// }

// #[derive(Subcommand, Clone, Debug, PartialEq)]
// pub enum Commands {
//     /// Start a call with the other person connected to the server
//     Call,
//     ///  End current call
//     Hangup,
//     /// Get daemon status
//     Status,

//     /// Start the interactive command shell
//     #[command(alias = "interactive")]
//     Shell,

//     /// Start the visual TUI
//     Tui,

//     /// Shutdown the daemon
//     Shutdown,

//     /// Connect to a server
//     Connect,

//     /// Disconnect from a server
//     Disconnect,

//     /// Set input volume
//     SetInputVolume,

//     /// Set output volume
//     SetOutputVolume,
// }
