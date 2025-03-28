mod webrtc;

use clap::Parser;

use color_eyre::Result;

#[derive(Parser)]
struct Args {
    // #[arg(short, long)]
    // file: std::path::PathBuf,
    #[arg(short, long)]
    bootstrap: bool,
    #[arg(short, long)]
    name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let args = Args::parse();

    Ok(())
}

// trait Connectionで通信(WebRTCとかHTTPとか)を抽象化するといいかも?Prototypeだからそこまではできない
