use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Packet {
    Handshake { password: String },
    Audio { data: Vec<u8> },
}

/// The type for the signaling message
#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum SingalType {
    Offer,
    Answer,
    Candidate,
}

/// The signaling message structure
#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct SignalingMessage {
    /// Type of the message
    #[serde(rename = "type")]
    pub type_: SingalType,

    /// The payload (e.g. ICE candidate)
    pub data: String,
}
