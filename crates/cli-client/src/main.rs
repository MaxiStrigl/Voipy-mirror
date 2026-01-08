mod cli;
mod client;
mod config;

use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::{ShutdownRequest, voicechat_daemon_client::VoicechatDaemonClient};

use crate::{
    cli::{Cli, Commands, DaemonCommands},
    client::{handle_command, is_daemon_running, start_daemon},
    config::Config,
};

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let _config = Config::load();

    if let Some(Commands::Daemon { command }) = &cli.command {
        match command {
            DaemonCommands::Start { signaling_ip } => {
                if !is_daemon_running().await {
                    start_daemon(signaling_ip).await?;
                }
            }
            DaemonCommands::Stop {} => {
                if is_daemon_running().await {
                    let request = tonic::Request::new(ShutdownRequest {});
                    println!("Sending daemon shutdown request");
                    let mut client = VoicechatDaemonClient::connect("http://[::1]:40001").await?;
                    println!("Shutdown daemon");
                    client.shutdown(request).await?;
                } else {
                    println!("Daemon is already dead.");
                }
                return Ok(());
            }
        }
    };

    if let Some(Commands::Start { ip }) = &cli.command {
        if !is_daemon_running().await && ip.is_some() {
            start_daemon(&ip.clone().unwrap()).await?;
        }
    }

    if cli.interactive {
        println!("Entering interactive mode. Type 'help' for commands, 'exit' to quit");
        println!("Just kidding. Imagine Maxi really did that. lol");
        return Ok(());
    } else if cli.command.is_some() {
        if !is_daemon_running().await {
            eprintln!("Daemon not started. START IT PLS. BECAUSE I CAN'T. THX FOR NOTHING MAXI");
        }

        let mut client =
            VoicechatDaemonClient::connect(format!("http://[::1]:{}", cli.port)).await?;

        handle_command(cli.command.unwrap(), &mut client).await?;
    };

    Ok(())
}
