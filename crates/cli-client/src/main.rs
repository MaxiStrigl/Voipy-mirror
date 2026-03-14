mod cli;
mod client;
mod config;

use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::voicechat_daemon_client::VoicechatDaemonClient;

use crate::{
    cli::{Cli, Commands},
    client::{connect, disconnect, hangup, is_daemon_running, start_call, start_daemon},
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

    let command = cli.command.clone();

    let config_file = cli.config.config_file.clone();

    let mut config = Config::load(config_file)?;

    apply_cli_to_config(cli, &mut config);
    if is_daemon_running().await == false {
        let result = start_daemon().await;

        if let Err(e) = result {
            return Err(e);
        }
    }

    if command.is_none() {
        return Ok(());
    }

    let command = command.unwrap();

    let mut client = VoicechatDaemonClient::connect("http://[::1]:40001").await?;

    let res = match command {
        Commands::Call => start_call(&mut client).await,
        Commands::Hangup => hangup(&mut client).await,
        Commands::Status => todo!(),
        Commands::Shell => todo!(),
        Commands::Tui => todo!(),
        Commands::Shutdown => todo!(),
        Commands::Connect => {
            connect(
                &mut client,
                config.connection.server_ip,
                config.connection.signaling_port,
            )
            .await
        }
        Commands::Disconnect => disconnect(&mut client).await,
    };

    if let Err(e) = res {
        println!("Error: {}", e);
    } else {
        println!("Done");
    }

    Ok(())

    // if let Some(Commands::Daemon { command }) = &cli.command {
    //     match command {
    //         DaemonCommands::Start { signaling_ip } => {
    //             if !is_daemon_running().await {
    //                 start_daemon(signaling_ip).await?;
    //             }
    //         }
    //         DaemonCommands::Stop {} => {
    //             if is_daemon_running().await {
    //                 let request = tonic::Request::new(ShutdownRequest {});
    //                 println!("Sending daemon shutdown request");
    //                 let mut client = VoicechatDaemonClient::connect("http://[::1]:40001").await?;
    //                 println!("Shutdown daemon");
    //                 client.shutdown(request).await?;
    //             } else {
    //                 println!("Daemon is already dead.");
    //             }
    //             return Ok(());
    //         }
    //     }
    // };
    //
    // if let Some(Commands::Start { ip }) = &cli.command {
    //     if !is_daemon_running().await && ip.is_some() {
    //         start_daemon(&ip.clone().unwrap()).await?;
    //     }
    // }
    //
    // if cli.interactive {
    //     println!("Entering interactive mode. Type 'help' for commands, 'exit' to quit");
    //     println!("Just kidding. Imagine Maxi really did that. lol");
    //     return Ok(());
    // } else if cli.command.is_some() {
    //     if !is_daemon_running().await {
    //         eprintln!("Daemon not started. START IT PLS. BECAUSE I CAN'T. THX FOR NOTHING MAXI");
    //     }
    //
    //     let mut client =
    //         VoicechatDaemonClient::connect(format!("http://[::1]:{}", cli.port)).await?;
    //
    //     handle_command(cli.command.unwrap(), &mut client).await?;
    // };

    // Ok(())
}

fn apply_cli_to_config(cli: Cli, config: &mut Config) {
    // User
    let u = &mut config.user;
    update_if_some!(u.name, cli.config.username);

    // Connection
    let c = &mut config.connection;
    update_if_some!(c.server_ip, cli.config.server_ip);
    update_if_some!(c.signaling_port, cli.config.signaling_port);
    update_if_some!(c.daemon_port, cli.config.daemon_port);
    update_if_some!(c.stun_servers, cli.config.stun_servers);
    update_if_some!(c.ice_timeout_seconds, cli.config.ice_timeout_seconds);

    // Audio
    let a = &mut config.audio;
    update_if_some!(a.input_volume, cli.config.input_volume);
    update_if_some!(a.output_volume, cli.config.output_volume);
    update_if_some!(a.input_device, cli.config.input_device);
    update_if_some!(a.input_latency_ms, cli.config.input_latency_ms);
    update_if_some!(
        a.output_jitter_buffer_ms,
        cli.config.output_jitter_buffer_ms
    );

    let p = &mut config.audio.processing;
    update_if_some!(p.vad_threshold, cli.config.vad_threshold);
    update_if_some!(p.noise_suppression, cli.config.noise_supression);
    update_if_some!(p.automatic_gain_control, cli.config.automatic_gain_control);
    update_if_some!(p.agc_target_level, cli.config.agc_target_level);
    update_if_some!(p.agc_max_gain, cli.config.agc_max_gain);
    update_if_some!(p.high_pass_filter_hz, cli.config.high_pass_filter_hz);
    update_if_some!(p.low_pass_filter_hz, cli.config.low_pass_filter_hz);
}
