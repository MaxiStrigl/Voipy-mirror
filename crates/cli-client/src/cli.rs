use clap::{Parser, Subcommand, ValueEnum};

#[derive(Parser)]
#[command(version, about, infer_subcommands = true)]
pub struct Cli {
    #[arg(long, global = true, default_value_t = 40001)]
    pub port: u16,

    #[command(subcommand)]
    pub command: Option<Commands>,

    #[arg(short, long, action = clap::ArgAction::SetTrue)]
    pub interactive: bool,
}

#[derive(Subcommand)]
pub enum DaemonCommands {
    Start {
        #[arg(long)]
        signaling_ip: String,
    },
    Stop {},
}

#[derive(Copy, Clone, ValueEnum)]
pub enum VolumeAction {
    #[value(alias = "g")]
    Get,
    #[value(alias = "s")]
    Set,
    #[value(alias = "m")]
    Mute,
}

#[derive(Copy, Clone, PartialEq, ValueEnum)]
pub enum VolumeTarget {
    #[value(name = "self", alias = "s")]
    Self_,
    #[value(alias = "o")]
    Other,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(alias = "call-start")]
    /// Starts a new call
    Start {
        #[arg(long)]
        ip: Option<String>,
    },
    /// Stop a call
    #[command(alias = "call-stop")]
    Stop,
    /// Get Status
    #[command(alias = "call-status")]
    Status,
    /// Manage Daemon
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },

    /// Volume Settings
    Volume {
        #[arg(value_enum)]
        /// Action to execute.
        action: VolumeAction,
        #[arg(value_enum)]
        /// Target for the action.
        target: VolumeTarget,
        /// New value for 'set' command.
        value: Option<f32>,
    },
}
