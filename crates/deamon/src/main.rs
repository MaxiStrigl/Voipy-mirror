use deamon_protocol::voicechat::{
    get_status_response::Status,
    voicechat_deamon_server::{VoicechatDeamon, VoicechatDeamonServer},
    *,
};
use tonic::{Request, Response, transport::Server};

pub struct MyVoiceChatDeamon {}

#[tonic::async_trait]
impl VoicechatDeamon for MyVoiceChatDeamon {
    async fn start(
        &self,
        request: Request<StartRequest>,
    ) -> Result<Response<StartResponse>, tonic::Status> {
        println!(
            "gRPC: Received Start Request with server_ip: {}",
            request.into_inner().server_ip
        );

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
    let deamon = MyVoiceChatDeamon {};

    println!("Daemon listening on {}", addr);

    Server::builder()
        .add_service(VoicechatDeamonServer::new(deamon))
        .serve(addr)
        .await?;

    Ok(())
}
