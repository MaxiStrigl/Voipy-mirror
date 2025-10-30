mod call;

use core::sync;
use std::sync::{Arc, Mutex};

use deamon_protocol::voicechat::{
    voicechat_deamon_server::{VoicechatDeamon, VoicechatDeamonServer},
    *,
};
use tokio::sync::watch::{self, Receiver, Sender};
use tokio::task::JoinHandle;
use tonic::{Request, Response, transport::Server};


struct CallState {
    handle: JoinHandle<()>,
    stop_sender: Sender<&'static str>
}


pub struct VoiceChatService {
    call_state: Arc<Mutex<Option<CallState>>>,
}

impl VoiceChatService {
    fn new() -> Self{
        Self {
            call_state: Arc::new(Mutex::new(None))
        }
    }
}

#[tonic::async_trait]
impl VoicechatDeamon for VoiceChatService {
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, tonic::Status> {
        let mut state = self.call_state.lock().unwrap();

        if state.is_some() {
            return Err(tonic::Status::failed_precondition("Call already in progress"));
        }

        let req = request.into_inner();
        let server_ip = req.server_ip;
        let is_caller = true;

        println!(
            "gRPC: Received Start Request with server_ip: {}",
            server_ip
        );

        let (tx, mut rx) = watch::channel("stop_call");

        let handle = tokio::spawn(async move {
            if let Err(e) = call::run_call(server_ip, is_caller, rx).await {
                eprintln!("Call failed: {}", e);
            }
        });

        *state = Some(CallState {handle, stop_sender: tx} );

        Ok(Response::new(StartResponse {}))
    }

    async fn stop(
        &self,
        _request: Request<StopRequest>,
    ) -> Result<Response<StopResponse>, tonic::Status> {
        println!("gRPC: Received Stop request");

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

        println!(
            "gRPC: Received GetStatus request"
        );

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:40001".parse()?;
    let deamon = VoiceChatService::new();

    println!("Daemon listening on {}", addr);

    Server::builder()
        .add_service(VoicechatDeamonServer::new(deamon))
        .serve(addr)
        .await?;

    Ok(())
}
