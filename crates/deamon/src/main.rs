mod webrtc_manager;

use crate::webrtc_manager::WebRtcManager;
use deamon_protocol::voicechat::{voicechat_deamon_server::{VoicechatDeamon, VoicechatDeamonServer}, GetStatusRequest, GetStatusResponse, ShutdownRequest, ShutdownResponse, StartRequest, StartResponse, StopRequest, StopResponse};
use tokio::sync::{broadcast::channel, mpsc, watch};
use tonic::{Request as TonicRequest, Response, transport::Server};
use webrtc_manager::Command;

pub struct VoiceChatService {
    command_sender: mpsc::Sender<Command>,
    shutdown_sender: watch::Sender<()>
}

impl VoiceChatService {
    fn new(command_sender: mpsc::Sender<Command>, shutdown_sender: watch::Sender<()>) -> Self {
        Self { command_sender,  shutdown_sender}
    }
}

#[tonic::async_trait]
impl VoicechatDeamon for VoiceChatService {
    async fn start(
        &self,
        _: TonicRequest<StartRequest>,
    ) -> Result<Response<StartResponse>, tonic::Status> {
        println!("gRPC: Received Start Call");

        if self.command_sender.send(Command::StartCall).await.is_err() {
            return Err(tonic::Status::internal(
                "Failed to send start command to WebRTC manager",
            ));
        }

        Ok(Response::new(StartResponse {}))
    }

    async fn stop(
        &self,
        _request: TonicRequest<StopRequest>,
    ) -> Result<Response<StopResponse>, tonic::Status> {
        println!("gRPC: Received Stop request");

        if self.command_sender.send(Command::StopCall).await.is_err() {
            return Err(tonic::Status::internal(
                "Failed to send stop command to WebRTC manager",
            ));
        }

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

        if self.command_sender.send(Command::Shutdown).await.is_err() {
            eprintln!("WebRtcManager was already shut down");
        }

        if self.shutdown_sender.send(()).is_err() {
            return Err(tonic::Status::internal("Failed to send shutdown signal to server"));
        }


        Ok(Response::new(ShutdownResponse {}))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:40001".parse()?;

    let signaling_server = "0.0.0.0";

    let (command_sender, command_receiver) = mpsc::channel(10);
    let (shutdown_sender, mut shutdown_receiver) = watch::channel(());

    tokio::spawn(async move {
        let mut manager = WebRtcManager::new(command_receiver, signaling_server.to_string());
        if let Err(e) = manager.run().await {
            eprintln!("WebRtc Manager failed: {}", e);
        }
    });

    let deamon = VoiceChatService::new(command_sender, shutdown_sender);

    println!("Daemon listening on {}", addr);

    Server::builder()
        .add_service(VoicechatDeamonServer::new(deamon))
        .serve_with_shutdown(addr, async {
            shutdown_receiver.changed().await.ok();
            println!("gRPC server shutting down");
        })
        .await?;

    Ok(())
}
