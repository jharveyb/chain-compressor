use std::fs::{self};
use std::path::PathBuf;

use anyhow::Ok;
use anyhow::anyhow;
use bitcoincore_rpc::{Auth, Client, RpcApi};
use chain_compressor::*;
use clap::{Args, Parser, Subcommand};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

mod util;

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
    CollectTxCountStats {
        #[command(flatten)]
        perf_args: PerfParams,

        #[arg(long)]
        blocksdir: Option<PathBuf>,

        #[arg(long)]
        statdir: Option<PathBuf>,
    },
    CollectOutputScriptTypeStats {
        #[command(flatten)]
        perf_args: PerfParams,

        #[arg(long)]
        blocksdir: Option<PathBuf>,

        #[arg(long)]
        statdir: Option<PathBuf>,
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
    let (height_tx, height_rx) = flume::bounded(1000);
    let (blocks_tx, blocks_rx) = flume::bounded(1000);
    let (raw_blocks_tx, raw_blocks_rx) = flume::bounded(1000);
    let (stat_job_tx, stat_job_rx) = flume::bounded(1000);
    let cancel_signal = CancellationToken::new();

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
        Commands::CollectTxCountStats {
            perf_args,
            blocksdir,
            statdir,
        } => {
            // Require a blockdir and statdir
            let block_files = glob::glob(
                blocksdir
                    .ok_or_else(|| anyhow!("Must specify a blockdir"))?
                    .join("*.blk")
                    .to_str()
                    .ok_or_else(|| anyhow!("Invalid blockdir pattern"))?,
            )?
            .filter_map(|p| p.ok());

            let stats_file = "block_tx_counts.csv";
            let statdir = statdir.ok_or_else(|| anyhow!("Must specify a statdir"))?;
            let statwriter = util::init_csv_writer(stats_file, &statdir)?;

            let io_thread_num = perf_args.io_threads.unwrap_or(1);
            let compute_thread_num = perf_args.compute_threads.unwrap_or(1);

            // task channels
            let (block_path_tx, block_path_rx) = flume::bounded(1000);
            let (path_count_tx, path_count_rx) = flume::bounded(1000);
            let (stats_tx, stats_rx) = flume::bounded(1000);

            // task pools
            let mut read_blocks_set = JoinSet::new();
            let mut paths_send_set = JoinSet::new();
            let mut write_stats_set = JoinSet::new();
            let mut pipe_close_set = JoinSet::new();

            // Get blocks
            for _ in 0..io_thread_num {
                read_blocks_set.spawn(read_block(
                    cancel_signal.child_token(),
                    block_path_rx.clone(),
                    blocks_tx.clone(),
                ));
            }

            // Process blocks
            let _process_block_set = process_block_workers(
                &cancel_signal,
                blocks_rx,
                count_block_txs,
                stats_tx,
                raw_blocks_tx,
                compute_thread_num,
            );

            // Drain raw_blocks chan
            tokio::spawn(drop_block(
                cancel_signal.child_token(),
                raw_blocks_rx.clone(),
            ));

            write_stats_set.spawn(write_csv(
                cancel_signal.child_token(),
                statwriter,
                stats_rx.clone(),
                stat_job_tx.clone(),
            ));

            // Send filepaths into pipeline
            paths_send_set.spawn(send_paths(
                cancel_signal.child_token(),
                block_files,
                block_path_tx.clone(),
                path_count_tx.clone(),
            ));

            let (paths_sent_tx, paths_sent_rx) = flume::bounded(1000);

            // Monitor progress on both sides of pipeline
            pipe_close_set.spawn(check_matching_job_count(
                cancel_signal.child_token(),
                paths_sent_rx,
                path_count_rx,
                stat_job_rx,
            ));

            // Send a signal once all paths have been sent
            paths_send_set.join_all().await;
            println!("Sent all paths");

            drop(paths_sent_tx);
            println!("Sent close trigger");

            pipe_close_set.join_all().await;
            cancel_signal.cancel();

            Ok(())
        }
        Commands::CollectOutputScriptTypeStats {
            perf_args,
            blocksdir,
            statdir,
        } => {
            // Require a blockdir and statdir
            let block_files = glob::glob(
                blocksdir
                    .ok_or_else(|| anyhow!("Must specify a blockdir"))?
                    .join("*.blk")
                    .to_str()
                    .ok_or_else(|| anyhow!("Invalid blockdir pattern"))?,
            )?
            .filter_map(|p| p.ok());

            let stats_file = "block_output_type_counts.csv";
            let statdir = statdir.ok_or_else(|| anyhow!("Must specify a statdir"))?;
            let statwriter = util::init_csv_writer(stats_file, &statdir)?;

            let io_thread_num = perf_args.io_threads.unwrap_or(1);
            let compute_thread_num = perf_args.compute_threads.unwrap_or(1);

            // task channels
            let (block_path_tx, block_path_rx) = flume::bounded(1000);
            let (path_count_tx, path_count_rx) = flume::bounded(1000);
            let (stats_tx, stats_rx) = flume::bounded(1000);

            // task pools
            let mut read_blocks_set = JoinSet::new();
            let mut paths_send_set = JoinSet::new();
            let mut write_stats_set = JoinSet::new();
            let mut pipe_close_set = JoinSet::new();

            // Get blocks
            for _ in 0..io_thread_num {
                read_blocks_set.spawn(read_block(
                    cancel_signal.child_token(),
                    block_path_rx.clone(),
                    blocks_tx.clone(),
                ));
            }

            // Process blocks
            let _process_block_set = process_block_workers(
                &cancel_signal,
                blocks_rx,
                count_tx_types,
                stats_tx,
                raw_blocks_tx,
                compute_thread_num,
            );

            // Drain raw_blocks chan
            tokio::spawn(drop_block(
                cancel_signal.child_token(),
                raw_blocks_rx.clone(),
            ));

            write_stats_set.spawn(write_csv(
                cancel_signal.child_token(),
                statwriter,
                stats_rx.clone(),
                stat_job_tx.clone(),
            ));

            // Send filepaths into pipeline
            paths_send_set.spawn(send_paths(
                cancel_signal.child_token(),
                block_files,
                block_path_tx.clone(),
                path_count_tx.clone(),
            ));

            let (paths_sent_tx, paths_sent_rx) = flume::bounded(1);

            // Monitor progress on both sides of pipeline
            pipe_close_set.spawn(check_matching_job_count(
                cancel_signal.child_token(),
                paths_sent_rx,
                path_count_rx,
                stat_job_rx,
            ));

            // Send a signal once all paths have been sent
            paths_send_set.join_all().await;
            println!("Sent all paths");

            drop(paths_sent_tx);
            println!("Sent close trigger");

            pipe_close_set.join_all().await;
            cancel_signal.cancel();

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
            let (raw_block_job_tx, raw_block_job_rx) = flume::bounded(1000);
            let (stats_tx, stats_rx) = flume::bounded(1000);

            // create task pools
            let mut fetch_block_set = JoinSet::new();
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
            let target_job_count = count.unwrap_or(chain_tip_height - start_height);

            // set jobs to detect pipeline completion
            let count_chans = vec![
                ("block write".to_string(), raw_block_job_rx),
                ("stat write".to_string(), stat_job_rx),
            ];
            let completion_set = pipeline_checks(count_chans, &cancel_signal, target_job_count);

            // populate task pools
            for _ in 0..rpc_thread_num {
                let conn = create_rpc_conn(&rpc_conn)?;
                fetch_block_set.spawn(fetch_block(
                    cancel_signal.child_token(),
                    conn,
                    height_rx.clone(),
                    blocks_tx.clone(),
                ));
            }

            let process_block_zstd_set = process_block_workers(
                &cancel_signal,
                blocks_rx,
                zstd_block,
                stats_tx,
                raw_blocks_tx,
                compute_thread_num,
            );

            if let Some(blockdir) = blockdir.as_ref() {
                if !fs::exists(blockdir)? {
                    fs::create_dir_all(blockdir)?;
                }
            }

            // create our CSV reader for exporting stats
            let zstd_stats_filename = "zstd_cmp_stats.csv";
            let statwriter = if let Some(dir) = statdir {
                Some(util::init_csv_writer(zstd_stats_filename, &dir)?)
            } else {
                None
            };

            if let Some(blockdir) = blockdir {
                for _ in 0..io_thread_num {
                    write_blocks_set.spawn(write_raw_block(
                        cancel_signal.child_token(),
                        raw_blocks_rx.clone(),
                        raw_block_job_tx.clone(),
                        blockdir.clone(),
                    ));
                }
            }

            if let Some(statwriter) = statwriter {
                write_stats_set.spawn(write_csv(
                    cancel_signal.child_token(),
                    statwriter,
                    stats_rx.clone(),
                    stat_job_tx.clone(),
                ));
            }

            // begin the pipeline by requesting info on block heights
            height_send_set.spawn(send_heights(
                cancel_signal.child_token(),
                block_nums,
                height_tx,
            ));

            // wait on the pipeline stages to finish, beginning and end
            height_send_set.join_all().await;
            process_block_zstd_set.join_all().await;
            completion_set.join_all().await;

            // job finished, shut down pipeline
            cancel_signal.cancel();

            Ok(())
        }
    }
}
