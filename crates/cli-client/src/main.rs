use anyhow::Result;
use deamon_protocol::voicechat::{StartRequest, voicechat_deamon_client::VoicechatDeamonClient};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = VoicechatDeamonClient::connect("http://[::1]:40001").await?;

    let message = "asdf".to_string();

    let request = tonic::Request::new(StartRequest { server_ip: message });

    println!("CLI: sending start request...");

    let response = client.start(request).await?;

    println!("CLI: Received answer: {:?}", response);

    Ok(())
}
