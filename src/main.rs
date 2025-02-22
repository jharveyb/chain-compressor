use std::fs::{self, File};
use std::path::PathBuf;

use bitcoin::Block;
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
        blockdir: Option<PathBuf>,

        #[arg(long)]
        statdir: Option<PathBuf>,

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
            blockdir,
            statdir,
            start_height,
            count,
        } => {
            let rpc = Client::new(url, Auth::UserPass(user.to_string(), pass.to_string()))?;
            let chaininfo = rpc.get_blockchain_info()?;
            let chain_tip_height = chaininfo.blocks;

            let block_nums = match count {
                Some(block_range) => *start_height..(start_height + block_range),
                None => 0_u64..chain_tip_height,
            };

            if let Some(blockdir) = blockdir {
                match fs::exists(blockdir) {
                    Ok(true) => {}
                    _ => fs::create_dir_all(blockdir)?,
                }
            }

            let zstd_stats_filename = "zstd_cmp_stats.csv";
            let mut zstdstatwriter = if let Some(statdir) = statdir {
                if !fs::exists(statdir)? {
                    fs::create_dir_all(statdir)?;
                }

                let zstdstatfile = statdir.join(zstd_stats_filename);
                if !fs::exists(&zstdstatfile)? {
                    let _ = File::create(&zstdstatfile)?;
                    // close file immediately
                }

                let stats_file = File::options().append(true).open(&zstdstatfile)?;
                Some(csv::Writer::from_writer(stats_file))
            } else {
                None
            };

            let mut block_buf: Block;
            for height in block_nums {
                block_buf = get_block(&rpc, height)?;
                let (block_stats, block_bytes) = zstd_block(block_buf, height).await?;
                println!("{:?}", block_stats);

                if let Some(blockdir) = blockdir {
                    let filename = format!("{}.blk", height);
                    let filepath = blockdir.join(filename);
                    fs::write(filepath, &block_bytes)?;
                }

                if let Some(zstdstatwriter) = zstdstatwriter.as_mut() {
                    zstdstatwriter.serialize(block_stats)?;
                }
            }

            Ok(())
        }
    }
}
