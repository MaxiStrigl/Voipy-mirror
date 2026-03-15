use clap::Parser;

#[derive(Parser)]
#[command(name = "voipyd")]
#[command(version, about)]
pub struct Cli {
    /// Specify the local port for the grpc connection
    #[arg(long, default_value_t = 40001)]
    pub grpc_port: u16,
}
