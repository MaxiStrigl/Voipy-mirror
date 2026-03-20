mod cli;
mod grpc_manager;
mod webrtc;

use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::{
    GetStatusResponse, voicechat_daemon_server::VoicechatDaemonServer,
};
use tokio::sync::{mpsc, watch};
use tonic::transport::Server;

use crate::{
    cli::Cli,
    grpc_manager::VoiceChatService,
    webrtc::{Command, manager::WebRtcManager},
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::try_parse()?;
    let addr = format!("[::1]:{}", args.grpc_port).parse()?;

    let (command_sender, command_receiver) = mpsc::channel::<Command>(10);
    let (status_sender, status_receiver) = watch::channel(GetStatusResponse::default());
    let (shutdown_sender, mut shutdown_receiver) = watch::channel(());

    let mut manager = WebRtcManager::new(command_receiver, status_sender)?;

    let manager_handle = tokio::spawn(async move { manager.run().await });

    let daemon = VoiceChatService::new(command_sender, status_receiver, shutdown_sender);

    Server::builder()
        .add_service(VoicechatDaemonServer::new(daemon))
        .serve_with_shutdown(addr, async {
            shutdown_receiver.changed().await.ok();
            println!("gRPC server shutting down");
        })
        .await?;

    println!("gRPC Server stopped, cleaning up manager");
    let _ = manager_handle.await;

    Ok(())
}
