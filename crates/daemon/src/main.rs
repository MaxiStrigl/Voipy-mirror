mod webrtc_manager;

use crate::webrtc_manager::WebRtcManager;
use anyhow::Result;
use clap::Parser;
use daemon_protocol::voicechat::{
    voicechat_daemon_server::{VoicechatDaemon, VoicechatDaemonServer}, GetInputVolumeRequest, GetInputVolumeResponse, GetStatusRequest, GetStatusResponse, SetInputVolumeRequest, SetInputVolumeResponse, ShutdownRequest, ShutdownResponse, StartRequest, StartResponse, StopRequest, StopResponse
};
use tokio::sync::{mpsc, watch};
use tonic::{Request as TonicRequest, Response, transport::Server};
use webrtc_manager::Command;

pub struct VoiceChatService {
    command_sender: mpsc::Sender<Command>,
    shutdown_sender: watch::Sender<()>,
}

impl VoiceChatService {
    fn new(command_sender: mpsc::Sender<Command>, shutdown_sender: watch::Sender<()>) -> Self {
        Self {
            command_sender,
            shutdown_sender,
        }
    }
}

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    #[arg(long)]
    signaling_ip: String,
    #[arg(long, default_value_t = 40001)]
    grpc_port: u16,
}

impl VoiceChatService {
    async fn send_command(&self, command: Command) -> Result<(), tonic::Status> {
        self.command_sender
            .send(command)
            .await
            .map_err(|_| tonic::Status::internal("Failed to send command to WebRTC manager"))
    }
}

#[tonic::async_trait]
impl VoicechatDaemon for VoiceChatService {
    async fn start(
        &self,
        _: TonicRequest<StartRequest>,
    ) -> Result<Response<StartResponse>, tonic::Status> {
        println!("gRPC: Received Start Call");
        self.send_command(Command::StartCall).await?;
        Ok(Response::new(StartResponse {}))
    }

    async fn stop(
        &self,
        _request: TonicRequest<StopRequest>,
    ) -> Result<Response<StopResponse>, tonic::Status> {
        println!("gRPC: Received Stop request");
        self.send_command(Command::StopCall).await?;
        Ok(Response::new(StopResponse {}))
    }

    async fn get_status(
        &self,
        _: TonicRequest<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, tonic::Status> {
        let reply = GetStatusResponse {
            status: 0,
            error_message: "".to_string(),
        };

        println!("gRPC: Received GetStatus request");

        Ok(Response::new(reply))
    }

    async fn shutdown(
        &self,
        _: TonicRequest<ShutdownRequest>,
    ) -> Result<Response<ShutdownResponse>, tonic::Status> {
        println!("gRPC: Received Shutdown request");

        self.send_command(Command::Shutdown).await?;

        if self.shutdown_sender.send(()).is_err() {
            return Err(tonic::Status::internal(
                "Failed to send shutdown signal to gRPC server",
            ));
        }

        Ok(Response::new(ShutdownResponse {}))
    }

    async fn get_input_volume(
        &self,
        _: TonicRequest<GetInputVolumeRequest>,
    ) -> Result<Response<GetInputVolumeResponse>, tonic::Status> {
        todo!()
    }

    async fn set_input_volume(
        &self,
        request: TonicRequest<SetInputVolumeRequest>,
    ) -> Result<Response<SetInputVolumeResponse>, tonic::Status> {
        let volume = request.into_inner().volume;

        self.send_command(Command::SetInputVolume(volume)).await?;
        Ok(Response::new(SetInputVolumeResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let addr = format!("[::1]:{}", cli.grpc_port).parse()?;

    let (command_sender, command_receiver) = mpsc::channel(10);
    let (shutdown_sender, mut shutdown_receiver) = watch::channel(());

    tokio::spawn(async move {
        let maybe_manager = WebRtcManager::new(command_receiver, cli.signaling_ip);
        match maybe_manager {
            Ok(mut manager) => {
                if let Err(e) = manager.run().await {
                    eprintln!("WebRtc Manager failed: {}", e);
                }
            }
            Err(e) => {
                eprintln!("Failed to create manager: {}", e);
            }
        }
    });

    let daemon = VoiceChatService::new(command_sender, shutdown_sender);

    println!("Daemon listening on {}", addr);

    Server::builder()
        .add_service(VoicechatDaemonServer::new(daemon))
        .serve_with_shutdown(addr, async {
            shutdown_receiver.changed().await.ok();
            println!("gRPC server shutting down");
        })
        .await?;

    Ok(())
}
