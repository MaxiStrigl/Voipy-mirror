use std::sync::Arc;

use tokio::sync::mpsc;
use webrtc::peer_connection::RTCPeerConnection;

use crate::webrtc::manager::ManagerInternalEvent;

pub fn setup_pc_handler(
    pc: &Arc<RTCPeerConnection>,
    internal_tx: mpsc::UnboundedSender<ManagerInternalEvent>,
) {
    let tx = internal_tx.clone();
    pc.on_track(Box::new(move |track, _, _| {
        let tx_inner = tx.clone();
        Box::pin(async move {
            println!("WebRTC: Remote track received");
            let _ = tx_inner.send(ManagerInternalEvent::RemoteTrackReceived(track));
        })
    }));

    let tx_ice = internal_tx.clone();
    pc.on_ice_candidate(Box::new(move |c| {
        let tx_inner = tx_ice.clone();
        Box::pin(async move {
            println!("WebRTC: Ice candidate received");
            if let Some(candidate) = c {
                match candidate.to_json() {
                    Ok(candidate_json) => {
                        let _ = tx_inner
                            .send(ManagerInternalEvent::IceCandidateGenerated(candidate_json));
                    }
                    Err(_) => {
                        println!("webRTC: ICE candidate gathering complete");
                    }
                }
            } else {
            }
        })
    }));
}
