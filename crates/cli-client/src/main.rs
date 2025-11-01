use std::{env, process::Stdio, time::Duration};

use anyhow::Result;
use clap::{Parser, Subcommand};
use daemon_protocol::voicechat::{
    voicechat_daemon_client::VoicechatDaemonClient, ShutdownRequest, StartRequest, StopRequest
};

use std::process::Command as StdCommand;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, action = clap::ArgAction::SetTrue)]
    interactive: bool,
}

#[derive(Subcommand)]
enum DaemonCommands {
    Start {
        #[arg(long)]
        signaling_ip: String,
    },
    Stop {},
}

#[derive(Subcommand)]
enum Commands {
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
}

async fn is_daemon_running() -> bool {
    VoicechatDaemonClient::connect("http://[::1]:40001")
        .await
        .is_ok()
}

async fn handle_command(
    command: Commands,
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<()> {
    match command {
        Commands::Start { .. } => {
            let request = tonic::Request::new(StartRequest {});
            println!("Sending start request...");
            let response = client.start(request).await;
            println!("Received answer {:?}", response);
        }
        Commands::Stop => {
            let request = tonic::Request::new(StopRequest {});
            println!("Sending stop request...");
            let response = client.stop(request).await;
            println!("Received answer {:?}", response);
        }
        Commands::Status => todo!(),
        Commands::Daemon { .. } => {}
    }

    Ok(())
}

async fn start_daemon(signaling_ip: &str) -> Result<()> {
    println!("Attempting to start daemon");

    let args = ["--signaling-ip", signaling_ip];
    let mut daemon_started = false;

    let binary_name = format!("daemon{}", env::consts::EXE_SUFFIX);
    let binary_path_cwd = format!(".{}{}", std::path::MAIN_SEPARATOR, binary_name);

    // Try path first
    if StdCommand::new(&binary_name)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
    {
        println!("Started daemon from PATH");
        daemon_started = true;
    }

    if !daemon_started {
        if StdCommand::new(&binary_path_cwd)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            println!("Started daemon from cwd");
            daemon_started = true;
        }
    }

    if !daemon_started {
        if StdCommand::new("cargo")
            .args(&[
                "run",
                "--package",
                "daemon",
                "--",
                "--signaling-ip",
                signaling_ip,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            println!("Started daemon using cargo.");
            daemon_started = true;
        }
    }

    if !daemon_started {
        return Err(anyhow::anyhow!(
            "Couldn't start daemon using any method (PATH, cwd, cargo)"
        ));
    }

    tokio::time::sleep(Duration::from_secs(4)).await;

    Ok(())
}


#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

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
                }
                else {
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

        let mut client = VoicechatDaemonClient::connect("http://[::1]:40001").await?;

        handle_command(cli.command.unwrap(), &mut client).await?;
    };

    Ok(())
}
