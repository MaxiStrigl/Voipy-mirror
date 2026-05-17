mod cli;
mod client;
mod config;

use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::{
    GetConfigRequest, voicechat_daemon_client::VoicechatDaemonClient,
};
use serde_json::Value;

use crate::{
    cli::{Cli, Command, ConfigAction},
    client::{
        connect, disconnect, get_current_config, get_devices, get_monitor, get_status, hangup,
        is_daemon_running, send_shutdown, set_config, set_deafen, set_input_volume, set_mute,
        set_output_volume, start_call, start_daemon,
    },
    config::Config,
};

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
            let response = client.get_config(GetConfigRequest {}).await?.into_inner();
            let config: Config = response.into();

            if let Some(path) = key {
                let mut current_value: Value = serde_json::to_value(config)?;

                for part in path.split('.') {
                    if let Value::Object(map) = current_value {
                        if let Some(v) = map.get(part) {
                            current_value = v.clone();
                        } else {
                            println!("Key '{}' not found", path);
                            return Ok(());
                        }
                    } else {
                        println!("Key '{}' is invalid", path);
                        return Ok(());
                    }
                }
                println!("{} = {}", path, current_value);
            } else {
                println!("Please provide a key (e.g. 'audio.input_volume')");
            }
            Ok(())
        }
        ConfigAction::Set(config_set_args) => {
            let response = client.get_config(GetConfigRequest {}).await?.into_inner();
            let mut config: Config = response.into();

            config.apply_args(config_set_args);

            config.save(None)?;

            set_config(client, config).await?;

            Ok(())
        }
        ConfigAction::Load { path } => {
            let config = Config::load(path)?;
            set_config(client, config).await
        }
        ConfigAction::Save { path } => {
            let response = get_current_config(client).await?;
            let config = Config::from(response);
            config.save(path)
        }
        ConfigAction::List => {
            let response = get_current_config(client).await?;
            let config: Config = response.into();
            let json = serde_json::to_string_pretty(&config)?;
            println!("{}", json);
            Ok(())
        }
    }
}
