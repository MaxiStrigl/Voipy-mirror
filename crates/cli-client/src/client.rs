use anyhow::Result;
use daemon_protocol::voicechat::{
    CallRequest, ConnectRequest, DisconnectRequest, HangupRequest,
    voicechat_daemon_client::VoicechatDaemonClient,
};
use std::{env, process::Stdio, time::Duration};

use std::process::Command as StdCommand;

pub async fn is_daemon_running() -> bool {
    VoicechatDaemonClient::connect("http://[::1]:40001")
        .await
        .is_ok()
}

pub async fn start_call(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<(), String> {
    let request = tonic::Request::new(CallRequest {});
    println!("Sending start request...");

    match client.call(request).await {
        Ok(response) => {
            let answer = response.into_inner();

            if answer.success {
                Ok(())
            } else {
                Err(format!("Failed to perform call: {}", answer.error_message))
            }
        }
        Err(status) => Err(status.message().to_string()),
    }
}

pub async fn hangup(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<(), String> {
    let request = tonic::Request::new(HangupRequest {});
    println!("Sending hangup request...");

    match client.hangup(request).await {
        Ok(response) => {
            let answer = response.into_inner();

            if answer.success {
                Ok(())
            } else {
                Err(format!("Failed to hangup call: {}", answer.error_message))
            }
        }
        Err(status) => Err(status.message().to_string()),
    }
}

pub async fn connect(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    address: String,
    port: u16,
) -> Result<(), String> {
    println!("Connecting to {}", &address);
    let request = tonic::Request::new(ConnectRequest {
        address,
        port: port as u32,
    });

    match client.connect_to_server(request).await {
        Ok(response) => {
            let answer = response.into_inner();

            if answer.success {
                Ok(())
            } else {
                Err("Failed to connect to server".into())
            }
        }
        Err(status) => Err(status.message().to_string()),
    }
}

pub async fn disconnect(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<(), String> {
    println!("Dsconnecting from server");
    let request = tonic::Request::new(DisconnectRequest {});

    match client.disconnect_from_server(request).await {
        Ok(response) => {
            let answer = response.into_inner();

            if answer.success {
                Ok(())
            } else {
                Err("Failed to disconnect from server".into())
            }
        }
        Err(status) => Err(status.message().to_string()),
    }
}

pub async fn start_daemon() -> Result<()> {
    println!("Attempting to start daemon");

    let mut daemon_started = false;

    let binary_name = format!("voipyd{}", env::consts::EXE_SUFFIX);
    let binary_path_cwd = format!(".{}{}", std::path::MAIN_SEPARATOR, binary_name);

    // Try path first
    if StdCommand::new(&binary_name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .is_ok()
    {
        println!("Started daemon from PATH");
        daemon_started = true;
    }

    if !daemon_started {
        if StdCommand::new(&binary_path_cwd)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            println!("Started daemon from cwd");
            daemon_started = true;
        }
    }

    if !daemon_started {
        if StdCommand::new("cargo")
            .args(&["run", "--package", "daemon"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .is_ok()
        {
            println!("Started daemon using cargo.");
            daemon_started = true;
        }
    }

    if !daemon_started {
        return Err(anyhow::anyhow!(
            "Couldn't start daemon using any method (PATH, cwd, cargo)"
        ));
    }

    tokio::time::sleep(Duration::from_secs(4)).await;

    Ok(())
}
