use std::{
    collections::HashMap,
    net::{SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use bincode::config;
use protocol::Packet;

fn main() -> std::io::Result<()> {
    let server_password = std::env::args().nth(1);

    if server_password.is_none() {
        println!("Usage: server <password>");
    }

    let server_password = server_password.unwrap();

    let socket = UdpSocket::bind("0.0.0.0:40000")?;

    let mut clients: HashMap<SocketAddr, Instant> = HashMap::new();

    let mut buf = [0u8; 4096];

    loop {
        if let Ok((size, src)) = socket.recv_from(&mut buf) {
            let data = &buf[..size];

            let config = config::standard();
            let decoded_data: Packet = bincode::serde::decode_from_slice(&data, config).unwrap().0;

            match decoded_data {
                Packet::Handshake { password } => {
                    if server_password == password {
                        clients.insert(src, Instant::now());
                    }
                }
                Packet::Audio { .. } => {
                    if clients.contains_key(&src) {
                        clients.insert(src, Instant::now());
                    } else {
                        continue;
                    }

                    for (dest, _) in &clients {
                        if src != *dest {
                            socket.send_to(data, dest)?;
                        }
                    }
                }
            }
        }

        clients.retain(|_, last_seen| last_seen.elapsed() < Duration::from_secs(10));
    }
}
