mod webrtc_manager;

use std::sync::{Arc, Mutex};

use deamon_protocol::voicechat::{
    voicechat_deamon_server::{VoicechatDeamon, VoicechatDeamonServer},
    *,
};
use tokio::sync::{
    mpsc,
    watch::{self, Receiver, Sender},
};
use tokio::task::JoinHandle;
use tonic::{Request, Response, transport::Server};

use webrtc_manager::Command;
use webrtc_manager::run_call;

use crate::webrtc_manager::WebRtcManager;

pub struct VoiceChatService {
    command_sender: mpsc::Sender<Command>,
}

impl VoiceChatService {
    fn new(command_sender: mpsc::Sender<Command>) -> Self {
        Self { command_sender }
    }
}

#[tonic::async_trait]
impl VoicechatDeamon for VoiceChatService {
    async fn start(
        &self,
        request: Request<StartRequest>,
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
        _request: Request<StopRequest>,
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
        request: Request<GetStatusRequest>,
    ) -> Result<Response<GetStatusResponse>, tonic::Status> {
        let reply = GetStatusResponse {
            status: 0,
            error_message: "".to_string(),
        };

        println!("gRPC: Received GetStatus request");

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:40001".parse()?;

    let signaling_server = "0.0.0.0";

    let (tx, rx) = mpsc::channel(10);

    tokio::spawn(async move {
        let mut manager = WebRtcManager::new(rx, signaling_server.to_string());
        if let Err(e) = manager.run().await {
            eprintln!("WebRtc Manager failed: {}", e);
        }
    });

    let deamon = VoiceChatService::new(tx);

    println!("Daemon listening on {}", addr);

    Server::builder()
        .add_service(VoicechatDeamonServer::new(deamon))
        .serve(addr)
        .await?;

    Ok(())
}
