use std::{env, process::Stdio, time::Duration};

use anyhow::Result;
use clap::{Command, Parser, Subcommand};
use deamon_protocol::voicechat::{
    voicechat_deamon_client::VoicechatDeamonClient, ShutdownRequest, StartRequest, StopRequest
};

use std::process::Command as StdCommand;

async fn start_or_connect_to_deamon() {}

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[arg(short, long, action = clap::ArgAction::SetTrue)]
    interactive: bool,
}

#[derive(Subcommand)]
enum DeamonCommands {
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
    /// Manage Deamon
    Daemon {
        #[command(subcommand)]
        command: DeamonCommands,
    },
}

async fn is_deamon_running() -> bool {
    VoicechatDeamonClient::connect("http://[::1]:40001")
        .await
        .is_ok()
}

async fn handle_command(
    command: Commands,
    client: &mut VoicechatDeamonClient<tonic::transport::Channel>,
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

async fn start_deamon(signaling_ip: &str) -> Result<()> {
    println!("Attempting to start deamon");

    let args = ["--signaling-ip", signaling_ip];
    let mut deamon_started = false;

    let binary_name = format!("deamon{}", env::consts::EXE_SUFFIX);
    let binary_path_cwd = format!(".{}{}", std::path::MAIN_SEPARATOR, binary_name);

    // Try path first
    if StdCommand::new(&binary_name)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
    {
        println!("Started deamon from PATH");
        deamon_started = true;
    }

    if !deamon_started {
        if StdCommand::new(&binary_path_cwd)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            println!("Started deamon from cwd");
            deamon_started = true;
        }
    }

    if !deamon_started {
        if StdCommand::new("cargo")
            .args(&[
                "run",
                "--package",
                "deamon",
                "--",
                "--signaling_ip",
                signaling_ip,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            println!("Started deamon using cargo.");
            deamon_started = true;
        }
    }

    if !deamon_started {
        return Err(anyhow::anyhow!(
            "Couldn't start deamon using any method (PATH, cwd, cargo)"
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
            DeamonCommands::Start { signaling_ip } => {
                if !is_deamon_running().await {
                    start_deamon(signaling_ip).await?;
                }
            }
            DeamonCommands::Stop {} => {
                if is_deamon_running().await {
                    let request = tonic::Request::new(ShutdownRequest {});
                    println!("Sending deamon shutdown request");
                    let mut client = VoicechatDeamonClient::connect("http://[::1]:40001").await?;
                    println!("Shutdown deamon");
                    client.shutdown(request).await?;
                }
                else {
                    println!("Deamon is already dead.");
                }
                return Ok(());
            }
        }
    };

    if let Some(Commands::Start { ip }) = &cli.command {
        if !is_deamon_running().await && ip.is_some() {
            start_deamon(&ip.clone().unwrap()).await?;
        }
    }

    if cli.interactive {
        println!("Entering interactive mode. Type 'help' for commands, 'exit' to quit");
        println!("Just kidding. Imagine Maxi really did that. lol");
        return Ok(());
    } else if cli.command.is_some() {
        if !is_deamon_running().await {
            eprintln!("Deamon not started. START IT PLS. BECAUSE I CAN'T. THX FOR NOTHING MAXI");
        }

        let mut client = VoicechatDeamonClient::connect("http://[::1]:40001").await?;

        handle_command(cli.command.unwrap(), &mut client).await?;
    };

    Ok(())
}
