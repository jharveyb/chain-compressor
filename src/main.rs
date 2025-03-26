use std::fs::{self, File};
use std::path::PathBuf;

use bitcoincore_rpc::{Auth, Client, RpcApi};
use chain_compressor::*;
use clap::{Args, Parser, Subcommand};
use csv::WriterBuilder;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args)]
struct RpcConn {
    #[arg(long)]
    url: String,

    #[arg(long)]
    user: String,

    #[arg(long)]
    pass: String,
}

#[derive(Args)]
struct PerfParams {
    #[arg(long)]
    rpc_threads: Option<u16>,

    #[arg(long)]
    io_threads: Option<u16>,

    #[arg(long)]
    compute_threads: Option<u16>,
}

#[derive(Subcommand)]
enum Commands {
    CheckConnection {
        #[command(flatten)]
        rpc_conn: RpcConn,
    },
    CompressZstd {
        #[command(flatten)]
        rpc_conn: RpcConn,

        #[command(flatten)]
        perf_args: PerfParams,

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

fn create_rpc_conn(conn: &RpcConn) -> anyhow::Result<Client> {
    Ok(Client::new(
        &conn.url,
        Auth::UserPass(conn.user.clone(), conn.pass.clone()),
    )?)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::CheckConnection { rpc_conn } => {
            let rpc = create_rpc_conn(&rpc_conn)?;
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
            rpc_conn,
            perf_args,
            blockdir,
            statdir,
            start_height,
            count,
        } => {
            // create channels to connect tasks
            let (height_tx, height_rx) = flume::bounded(1000);
            let (blocks_tx, blocks_rx) = flume::bounded(1000);
            let (raw_block_job_tx, raw_block_job_rx) = flume::bounded(1000);
            let (stat_job_tx, stat_job_rx) = flume::bounded(1000);
            let (stats_tx, stats_rx) = flume::bounded(1000);
            let (raw_blocks_tx, raw_blocks_rx) = flume::bounded(1000);
            let cancel_signal = CancellationToken::new();

            // create task pools
            let mut fetch_block_set = JoinSet::new();
            let mut process_block_zstd_set = JoinSet::new();
            let mut write_blocks_set = JoinSet::new();
            let mut write_stats_set = JoinSet::new();
            let mut height_send_set = JoinSet::new();

            let rpc_thread_num = perf_args.rpc_threads.unwrap_or(1);
            let io_thread_num = perf_args.io_threads.unwrap_or(1);
            let compute_thread_num = perf_args.compute_threads.unwrap_or(1);

            let rpc = create_rpc_conn(&rpc_conn)?;
            let chaininfo = rpc.get_blockchain_info()?;
            let chain_tip_height = chaininfo.blocks;

            // set our block height range
            let block_nums = match count {
                Some(block_range) => start_height..(start_height + block_range),
                None => start_height..chain_tip_height,
            };
            let target_job_count = match count {
                Some(block_range) => block_range,
                None => chain_tip_height - start_height,
            };

            // set jobs to detect pipeline completion
            let count_chans = vec![
                ("block write".to_string(), raw_block_job_rx),
                ("stat write".to_string(), stat_job_rx),
            ];
            let completion_set = pipeline_checks(count_chans, &cancel_signal, target_job_count);

            // populate task pools
            for _ in 0..rpc_thread_num {
                let conn = create_rpc_conn(&rpc_conn)?;
                let _ = fetch_block_set.spawn(fetch_block(
                    cancel_signal.child_token(),
                    conn,
                    height_rx.clone(),
                    blocks_tx.clone(),
                ));
            }

            for _ in 0..compute_thread_num {
                let _ = process_block_zstd_set.spawn(process_block_zstd(
                    cancel_signal.child_token(),
                    blocks_rx.clone(),
                    stats_tx.clone(),
                    raw_blocks_tx.clone(),
                ));
            }

            if let Some(blockdir) = blockdir.as_ref() {
                if !fs::exists(blockdir)? {
                    fs::create_dir_all(blockdir)?;
                }
            }

            // create our CSV reader for exporting stats
            let zstd_stats_filename = "zstd_cmp_stats.csv";
            let zstdstatwriter = if let Some(statdir) = statdir.as_ref() {
                if !fs::exists(statdir)? {
                    fs::create_dir_all(statdir)?;
                }

                // create stats file before appending; we shouldn't end up
                // truncating existing contents
                let zstdstatfile = statdir.join(zstd_stats_filename);
                let existing_file = fs::exists(&zstdstatfile)?;
                if !existing_file {
                    let _ = File::create(&zstdstatfile)?;
                }

                let stats_file = File::options().append(true).open(&zstdstatfile)?;
                let writer = match existing_file {
                    // don't write CSV headers again if we're appending
                    true => WriterBuilder::new()
                        .has_headers(false)
                        .from_writer(stats_file),
                    false => WriterBuilder::new().from_writer(stats_file),
                };
                Some(writer)
            } else {
                None
            };

            if let Some(blockdir) = blockdir {
                for _ in 0..io_thread_num {
                    let _ = write_blocks_set.spawn(write_raw_block(
                        cancel_signal.child_token(),
                        raw_blocks_rx.clone(),
                        raw_block_job_tx.clone(),
                        blockdir.clone(),
                    ));
                }
            }

            if let Some(statwriter) = zstdstatwriter {
                let _ = write_stats_set.spawn(write_csv(
                    cancel_signal.child_token(),
                    statwriter,
                    stats_rx.clone(),
                    stat_job_tx.clone(),
                ));
            }

            // begin the pipeline by requesting info on block heights
            let _ = height_send_set.spawn(send_heights(
                cancel_signal.child_token(),
                block_nums,
                height_tx,
            ));

            // wait on the pipeline stages to finish, beginning and end
            height_send_set.join_all().await;
            completion_set.join_all().await;

            // job finished, shut down pipeline
            cancel_signal.cancel();

            Ok(())
        }
    }
}
