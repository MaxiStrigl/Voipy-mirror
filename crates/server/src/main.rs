use std::{collections::HashMap, net::{SocketAddr, UdpSocket}, time::{Duration, Instant}};

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:40000")?;

    let mut clients: HashMap<SocketAddr, Instant> = HashMap::new();

    let mut buf = [0u8; 4096];

    loop {
        if let Ok((size, src)) = socket.recv_from(&mut buf) {
            let data = &buf[..size];

            clients.insert(src, Instant::now());

            for (dest, _) in &clients {
                if src != *dest {
                    socket.send_to(data, dest)?;
                }
            }
        }

        clients.retain(|_, last_seen| last_seen.elapsed() < Duration::from_secs(10));
    }
}
