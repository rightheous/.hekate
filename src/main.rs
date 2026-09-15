use clap::Parser;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    hekate::interfaces::cli::run(hekate::interfaces::cli::Cli::parse()).await
}
