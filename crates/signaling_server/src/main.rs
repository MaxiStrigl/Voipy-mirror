use std::{
    collections::HashMap,
    sync::Arc,
};

use futures_util::{stream::SplitSink, SinkExt, StreamExt};

use tokio::sync::Mutex;
use warp::{
    Filter,
    filters::ws::{Message, WebSocket},
};

type Clients = Arc<Mutex<HashMap<usize, SplitSink<WebSocket, Message>>>>;

#[tokio::main]
async fn main() {
    let clients = Clients::default();

    let clients_filter = warp::any().map(move || clients.clone());

    let ws_route = warp::path("signaling_route")
        .and(warp::ws())
        .and(clients_filter)
        .map(|ws: warp::ws::Ws, clients: Clients| {
            ws.on_upgrade(move |websocket| handle_client_connection(websocket, clients))
        });

    println!("Listening on 0.0.0.0:40000");
    warp::serve(ws_route).run(([0, 0, 0, 0], 40000)).await;
}

/// Handles the new Client websocket connection (For now only 2 clients)
async fn handle_client_connection(websocket: WebSocket, clients: Clients) {
    let client_id: usize;
    let (sender, mut receiver) = websocket.split();

    {
        let mut clients_guard = clients.lock().await;
        if clients_guard.len() >= 2 {
            println!("Server is full, rejecting new client");
            return;
        }

        client_id = if clients_guard.get(&1).is_none() {
            1
        } else {
            2
        };
        clients_guard.insert(client_id, sender);
    }
    println!("Client {} connected", client_id);

    while let Some(result) = receiver.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("An error occured: {}", err);
                break;
            }
        };

        if msg.is_text() {
            let receiver_id = if client_id == 1 { 2 } else { 1 };

            let mut lock_guard = clients.lock().await;

            if let Some(receiver_sink) = lock_guard.get_mut(&receiver_id) {
                if let Err(e) = receiver_sink.send(msg).await {
                    eprintln!("Failed to forward message to client: {}: {}", receiver_id, e);
                }
                else {
                    println!("Forwarded message from {} to {}", client_id, receiver_id);
                }
            } else {
                println!("Client {} not yet connected", receiver_id);
            }
            
        } else if msg.is_close() {
            println!("Client {} send close message", client_id);
            break;
        }
    }

    clients.lock().await.remove(&client_id);
    println!("Client {} disconnected", client_id);
}
