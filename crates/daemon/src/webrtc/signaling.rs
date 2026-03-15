use anyhow::Result;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;

/// The Messages exchanged with the signaling server
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum SignalMessage {
    Offer(String),
    Answer(String),
    Candidate(RTCIceCandidateInit),
}

pub struct SignalingConnection {
    ws_sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    ws_receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl SignalingConnection {
    pub async fn connect(host: &str, port: u16) -> Result<Self> {
        let url = format!("ws://{}:{}/signaling_route", host, port);

        let (ws_stream, _) = connect_async(&url).await?;

        let (ws_sender, ws_receiver) = ws_stream.split();

        Ok(Self {
            ws_sender,
            ws_receiver,
        })
    }

    pub async fn send(&mut self, msg: SignalMessage) -> Result<()> {
        let json = serde_json::to_string(&msg)?;

        self.ws_sender.send(Message::Text(json.into())).await?;
        Ok(())
    }

    pub async fn recv(&mut self) -> Result<Option<SignalMessage>> {
        while let Some(msg_result) = self.ws_receiver.next().await {
            let msg = msg_result?;

            match msg {
                Message::Text(text) => {
                    let signal_msg: SignalMessage = serde_json::from_str(&text)?;
                    return Ok(Some(signal_msg));
                }
                Message::Close(_) => {
                    return Ok(None);
                }
                _ => {}
            }
        }

        Ok(None)
    }
}
