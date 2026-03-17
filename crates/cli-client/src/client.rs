use anyhow::Result;
use daemon_protocol::voicechat::{
    CallRequest, ConnectRequest, DisconnectRequest, HangupRequest,
    voicechat_daemon_client::VoicechatDaemonClient,
};
use daemon_protocol::voicechat::{
    DeafenRequest, DeafenResponse, GetConfigRequest, GetConfigResponse, GetStatusRequest,
    MuteRequest, SetInputVolumeRequest, SetOutputVolumeRequest,
};
use std::{env, process::Stdio, time::Duration};

use std::process::Command as StdCommand;

use crate::config::Config;

pub async fn is_daemon_running() -> bool {
    VoicechatDaemonClient::connect("http://[::1]:40001")
        .await
        .is_ok()
}

pub async fn start_call(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<()> {
    let request = tonic::Request::new(CallRequest {});
    println!("Sending start request...");

    client.call(request).await?;
    Ok(())
}

pub async fn hangup(client: &mut VoicechatDaemonClient<tonic::transport::Channel>) -> Result<()> {
    let request = tonic::Request::new(HangupRequest {});
    println!("Sending hangup request...");
    client.hangup(request).await;
    Ok(())
}

pub async fn connect(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    address: String,
    port: u16,
) -> Result<()> {
    println!("Connecting to {}", &address);
    let request = tonic::Request::new(ConnectRequest {
        address,
        port: port as u32,
    });

    client.connect_to_server(request).await?;
    Ok(())
}

pub async fn disconnect(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<()> {
    println!("Disconnecting from server");
    let request = tonic::Request::new(DisconnectRequest {});

    client.disconnect_from_server(request).await?;

    Ok(())
}

pub async fn set_input_volume(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    volume: f32,
) -> Result<()> {
    println!("Setting input volume");
    let request = tonic::Request::new(SetInputVolumeRequest { volume });

    let _ = client.set_input_volume(request).await;

    Ok(())
}

pub async fn set_output_volume(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    volume: f32,
) -> Result<()> {
    println!("Setting output volume");
    let request = tonic::Request::new(SetOutputVolumeRequest { volume });

    let _ = client.set_output_volume(request).await?;

    Ok(())
}

pub async fn get_status(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<()> {
    println!("Getting status");
    let request = tonic::Request::new(GetStatusRequest {});

    let res = client.get_status(request).await?;

    println!("{:?}", res.into_inner());

    Ok(())
}

pub async fn set_config(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    config: Config,
) -> Result<()> {
    // let request = tonic::Request::new(ConfigureRequest { user: todo!(), connection: todo!(), audio: todo!() })
    Ok(())
}

pub async fn get_current_config(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<GetConfigResponse> {
    println!("Getting config");
    let request = tonic::Request::new(GetConfigRequest {});

    let res = client.get_config(request).await?;

    Ok(res.into_inner())
}

pub async fn set_mute(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    active: bool,
) -> Result<()> {
    println!("Setting mute enabled: {}", active);
    let request = tonic::Request::new(MuteRequest { active });

    client.set_mute(request).await?;

    Ok(())
}

pub async fn set_deafen(
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
    active: bool,
) -> Result<()> {
    println!("Setting deafen enabled: {}", active);
    let request = tonic::Request::new(DeafenRequest { active });

    client.set_deafen(request).await?;

    Ok(())
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
