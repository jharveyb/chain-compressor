use std::path::PathBuf;

use bitcoincore_rpc::{Auth, Client, RpcApi};
use chain_compressor::*;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    CheckConnection {
        #[arg(long)]
        url: String,

        #[arg(long)]
        user: String,

        #[arg(long)]
        pass: String,
    },
    CompressZstd {
        #[arg(long)]
        url: String,

        #[arg(long)]
        user: String,

        #[arg(long)]
        pass: String,

        #[arg(long)]
        write: Option<bool>,

        #[arg(long)]
        outdir: Option<PathBuf>,

        #[arg(long)]
        start_height: u64,

        #[arg(long)]
        count: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Commands::CheckConnection { url, user, pass } => {
            let rpc = Client::new(url, Auth::UserPass(user.to_string(), pass.to_string()))?;
            println!("Connected to node");
            let mut netinfo = rpc.get_network_info()?;
            netinfo.networks = vec![];
            netinfo.local_addresses = vec![];
            let mut chaininfo = rpc.get_blockchain_info()?;
            chaininfo.chain_work = vec![];
            println!("{:#?}", netinfo);
            println!("{:#?}", chaininfo);

            Ok(())
        }
        Commands::CompressZstd {
            url,
            user,
            pass,
            write,
            outdir,
            start_height,
            count,
        } => {
            let rpc = Client::new(url, Auth::UserPass(user.to_string(), pass.to_string()))?;
            let chaininfo = rpc.get_blockchain_info()?;
            let chain_tip_height = chaininfo.blocks;

            let tip_block = get_block(&rpc, chain_tip_height)?;
            let block_stats = zstd_block(tip_block, chain_tip_height).await?;
            println!(
                "height: {}, orig. size: {}, cmp. size: {}",
                block_stats.0, block_stats.1, block_stats.2
            );

            Ok(())
        }
    }
}
