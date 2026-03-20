mod cli;
mod client;
mod config;

use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::voicechat_daemon_client::VoicechatDaemonClient;

use crate::{
    cli::{Cli, Command, ConfigAction},
    client::{
        connect, disconnect, get_current_config, get_devices, get_monitor, get_status, hangup,
        is_daemon_running, send_shutdown, set_config, set_deafen, set_input_volume, set_mute,
        set_output_volume, start_call, start_daemon,
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
        Command::Devices { input, output } => get_devices(&mut client, input, output).await?,
        Command::Status => get_status(&mut client).await?,
        Command::Monitor => get_monitor(&mut client).await?,
        Command::Shutdown => send_shutdown(&mut client).await?,
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
        ConfigAction::Get { key } => {
            let resp = get_current_config(client).await?;
            let config = Config::from(resp);

            if let Some(k) = key {
                match k.as_str() {
                    _ => (),
                }
            } else {
                println!("{}", toml::to_string_pretty(&config)?);
            }
        }
        ConfigAction::Set(args) => {
            let resp = get_current_config(client).await?;
            let mut config = Config::from(resp);

            let user = &mut config.user;
            let connection = &mut config.connection;
            let audio = &mut config.audio;
            let processing = &mut audio.processing;

            update_if_some!(user.name, args.username);

            update_if_some!(connection.server_ip, args.server_addr);
            update_if_some!(connection.signaling_port, args.server_port);
            update_if_some!(connection.ice_timeout_seconds, args.ice_timeout_seconds);
            update_if_some!(connection.stun_servers, args.stun_servers);

            update_if_some!(audio.input_device, args.input_device);
            update_if_some!(audio.output_device, args.output_device);
            update_if_some!(audio.input_volume, args.input_volume);
            update_if_some!(audio.output_volume, args.output_volume);
            update_if_some!(audio.input_latency_ms, args.input_latency_ms);
            update_if_some!(audio.output_jitter_buffer_ms, args.jitter_buffer_ms);

            update_if_some!(processing.agc_max_gain, args.agc_max_gain);
            update_if_some!(processing.agc_target_level, args.agc_target_level);
            update_if_some!(
                processing.automatic_gain_control,
                args.automatic_gain_control
            );
            update_if_some!(processing.high_pass_filter_hz, args.high_pass_filter_hz);
            update_if_some!(processing.low_pass_filter_hz, args.low_pass_filter_hz);
            update_if_some!(processing.noise_suppression, args.noise_suppression);
            update_if_some!(processing.vad_threshold, args.vad_threshold);

            set_config(client, config).await?;
        }
        ConfigAction::Load { path } => {
            let config = Config::load(path)?;
            set_config(client, config).await?;
            println!("Configuration Loaded")
        }
        ConfigAction::Save { path } => {
            let resp = get_current_config(client).await?;
            let config = Config::from(resp);
            config.save(path)?;
        }
        ConfigAction::List => {
            let resp = get_current_config(client).await?;
            let config = Config::from(resp);
            println!("{}", toml::to_string_pretty(&config)?);
        }
    }

    Ok(())
}
