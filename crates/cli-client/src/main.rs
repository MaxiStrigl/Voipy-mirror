use anyhow::Result;
use clap::{Parser, Subcommand};
use deamon_protocol::voicechat::{voicechat_deamon_client::VoicechatDeamonClient, StartRequest, StopRequest};

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
    Start,
    /// Stop a call
    Stop,
    /// Get Status
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
        Commands::Start => {
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Some(Commands::Daemon { command }) = &cli.command {
        match command {
            DeamonCommands::Start { signaling_ip } => {
                if !is_deamon_running().await {
                    eprintln!(
                        "Deamon not started. START IT PLS. BECAUSE I CAN'T. THX FOR NOTHING MAXI"
                    );
                    return Ok(());
                }
                return Ok(());
            }
            DeamonCommands::Stop {} => {
                eprintln!("Bro do it yourself. Use btop, killall or some shit.");
                return Ok(());
            }
        }
    };

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
