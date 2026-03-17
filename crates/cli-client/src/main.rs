mod cli;
mod client;
mod config;

use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::voicechat_daemon_client::VoicechatDaemonClient;

use crate::{
    cli::{Cli, Command, ConfigAction},
    client::{
        connect, disconnect, get_status, hangup, is_daemon_running, set_config, set_deafen,
        set_input_volume, set_mute, set_output_volume, start_call, start_daemon,
    },
    config::Config,
};

macro_rules! update_if_some {
    ($target:expr, $source:expr) => {
        if let Some(value) = $source {
            $target = value
        }
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let command = cli.command;
    let grpc_addr = format!("http://[::1]:{}", cli.grpc_port);

    // Check if daemon is running and start it if not
    let mut client = if is_daemon_running().await == false {
        let result = start_daemon().await;

        // Get the config
        let config = Config::load(None)?;

        // Connect to the client
        let mut client = VoicechatDaemonClient::connect(grpc_addr).await?;

        // Apply the config
        set_config(&mut client, config).await?;

        if let Err(e) = result {
            return Err(e);
        }
        client
    } else {
        VoicechatDaemonClient::connect(grpc_addr).await?
    };

    match command {
        Command::Connect { address, port } => connect(&mut client, address, port).await?,

        Command::Disconnect => disconnect(&mut client).await?,

        Command::Call => start_call(&mut client).await?,

        Command::Hangup => hangup(&mut client).await?,

        Command::Mute { active } => set_mute(&mut client, active).await?,
        Command::Deafen { active } => set_deafen(&mut client, active).await?,
        Command::Volume {
            volume,
            input,
            output,
        } => {
            if input {
                set_input_volume(&mut client, volume).await?;
            }
            if output {
                set_output_volume(&mut client, volume).await?;
            }
        }
        Command::Config { action } => handle_config(&mut client, action).await?,
        Command::Devices { input, output } => todo!(),
        Command::Status => get_status(&mut client).await?,
        Command::Monitor => todo!(),
        Command::Shutdown => todo!(),
        Command::Shell => todo!(),
        Command::Tui => todo!(),
    }

    Ok(())
}

async fn handle_config(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    action: ConfigAction,
) -> Result<()> {
    match action {
        ConfigAction::Get { key } => todo!(),
        ConfigAction::Set(config_set_args) => todo!(),
        ConfigAction::Load { path } => todo!(),
        ConfigAction::Save { path } => todo!(),
        ConfigAction::List => todo!(),
    }
}

// fn apply_cli_to_config(cli: Cli, config: &mut Config) {
//     // User
//     let u = &mut config.user;
//     update_if_some!(u.name, cli.config.username);

//     // Connection
//     let c = &mut config.connection;
//     update_if_some!(c.server_ip, cli.config.server_ip);
//     update_if_some!(c.signaling_port, cli.config.signaling_port);
//     c.daemon_port = cli.config.daemon_port;
//     update_if_some!(c.stun_servers, cli.config.stun_servers);
//     update_if_some!(c.ice_timeout_seconds, cli.config.ice_timeout_seconds);

//     // Audio
//     let a = &mut config.audio;
//     update_if_some!(a.input_volume, cli.config.input_volume);
//     update_if_some!(a.output_volume, cli.config.output_volume);
//     update_if_some!(a.input_device, cli.config.input_device);
//     update_if_some!(a.input_latency_ms, cli.config.input_latency_ms);
//     update_if_some!(
//         a.output_jitter_buffer_ms,
//         cli.config.output_jitter_buffer_ms
//     );

//     let p = &mut config.audio.processing;
//     update_if_some!(p.vad_threshold, cli.config.vad_threshold);
//     update_if_some!(p.noise_suppression, cli.config.noise_supression);
//     update_if_some!(p.automatic_gain_control, cli.config.automatic_gain_control);
//     update_if_some!(p.agc_target_level, cli.config.agc_target_level);
//     update_if_some!(p.agc_max_gain, cli.config.agc_max_gain);
//     update_if_some!(p.high_pass_filter_hz, cli.config.high_pass_filter_hz);
//     update_if_some!(p.low_pass_filter_hz, cli.config.low_pass_filter_hz);
// }
