use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Packet {
    Handshake { password: String},
    Audio { data: Vec<u8>},
}
