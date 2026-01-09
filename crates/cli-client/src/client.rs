use crate::cli::*;
use std::{
    env,
    process::Stdio,
    time::Duration,
};

use anyhow::Result;
use daemon_protocol::voicechat::{
    SetInputVolumeRequest, SetOutputVolumeRequest, StartRequest, StopRequest,
    voicechat_daemon_client::VoicechatDaemonClient,
};

use std::process::Command as StdCommand;


pub async fn is_daemon_running() -> bool {
    VoicechatDaemonClient::connect("http://[::1]:40001")
        .await
        .is_ok()
}

pub async fn handle_command(
    command: Commands,
    client: &mut VoicechatDaemonClient<tonic::transport::Channel>,
) -> Result<()> {
    // match command {
    //     Commands::Start { .. } => {
    //         let request = tonic::Request::new(StartRequest {});
    //         println!("Sending start request...");
    //         let response = client.start(request).await;
    //         println!("Received answer {:?}", response);
    //     }
    //     Commands::Stop => {
    //         let request = tonic::Request::new(StopRequest {});
    //         println!("Sending stop request...");
    //         let response = client.stop(request).await;
    //         println!("Received answer {:?}", response);
    //     }
    //     Commands::Status => todo!(),
    //     Commands::Daemon { .. } => {}
    //     Commands::Volume {
    //         action,
    //         target,
    //         value,
    //     } => match action {
    //         VolumeAction::Set => {
    //             if target == VolumeTarget::Self_ {
    //                 if let Some(value) = value {
    //                     let request = tonic::Request::new(SetInputVolumeRequest { volume: value });
    //                     println!("Sending set volume Request {}", value);
    //                     let response = client.set_input_volume(request).await;
    //                     println!("Received answer {:?}", response);
    //                 } else {
    //                     println!("Value is required for 'set' action.");
    //                 }
    //             } else {
    //                 if let Some(value) = value {
    //                     let request = tonic::Request::new(SetOutputVolumeRequest { volume: value });
    //                     println!("Sending set volume Request {}", value);
    //                     let response = client.set_output_volume(request).await;
    //                     println!("Received answer {:?}", response);
    //                 } else {
    //                     println!("Value is required for 'set' action.");
    //                 }
    //             }
    //         }
    //         VolumeAction::Get => {
    //             println!("Get volume not yet implemented.");
    //         }
    //         VolumeAction::Mute => {
    //             println!("Mute volume not yet implemented.");
    //         }
    //     },
    // }

    Ok(())
}

pub async fn start_daemon(signaling_ip: &str) -> Result<()> {
    println!("Attempting to start daemon");

    let args = ["--signaling-ip", signaling_ip];
    let mut daemon_started = false;

    let binary_name = format!("voipyd{}", env::consts::EXE_SUFFIX);
    let binary_path_cwd = format!(".{}{}", std::path::MAIN_SEPARATOR, binary_name);

    // Try path first
    if StdCommand::new(&binary_name)
        .args(args)
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
            .args(args)
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
            .args(&[
                "run",
                "--package",
                "daemon",
                "--",
                "--signaling-ip",
                signaling_ip,
            ])
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
