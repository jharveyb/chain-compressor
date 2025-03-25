use std::fs;
use std::ops::Range;
use std::path::PathBuf;

use async_compression::tokio::write::ZstdEncoder;
use bitcoin::Block;
use bitcoin::consensus;
use bitcoincore_rpc::bitcoin::consensus::Encodable;
use bitcoincore_rpc::{Client, RpcApi, bitcoin};
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::task;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

// blocks are average size 900 kB; 727791885830 bytes / 884760 blocks
pub const AVG_BLOCK_SIZE: usize = 900_000;

#[derive(Debug, Serialize, Deserialize)]
pub struct ZstdBlockStats {
    height: u64,
    orig_size: u64,
    cmp_size: u64,
    ratio: f32,
    savings: f32,
}

// lookup block by height, return decoded block and its consensus size
pub fn get_block(rpc: &Client, height: u64) -> anyhow::Result<Block> {
    let blockhash = rpc.get_block_hash(height)?;
    let block_hex = rpc.get_block_hex(&blockhash)?;
    Ok(consensus::encode::deserialize_hex(&block_hex)?)
}

// encode a block, compress with zstd, and report the compressed size
pub async fn zstd_block(block: &Block, height: u64) -> anyhow::Result<(ZstdBlockStats, Vec<u8>)> {
    let mut block_buf = Vec::with_capacity(AVG_BLOCK_SIZE);
    block.consensus_encode(&mut block_buf)?;
    let block_size = block_buf.len();

    // at least as big as uncompressed input
    let mut zstd_buf = Vec::with_capacity(block_size);
    let mut cmp = ZstdEncoder::with_quality(&mut zstd_buf, async_compression::Level::Default);
    cmp.write_all(&block_buf).await?;
    cmp.flush().await?;
    let cmp_size = zstd_buf.len() as u64;
    let ratio = block_size as f32 / cmp_size as f32;
    let savings = 1_f32 - (cmp_size as f32 / block_size as f32);

    let stats_line = ZstdBlockStats {
        height,
        orig_size: block_size as u64,
        cmp_size: zstd_buf.len() as u64,
        ratio,
        savings,
    };
    Ok((stats_line, block_buf))
}

pub async fn fetch_block(
    token: CancellationToken,
    rpc: Client,
    height: flume::Receiver<u64>,
    blocks: flume::Sender<(u64, Block)>,
) -> anyhow::Result<()> {
    while let Some(new_height) = select! {
        _ = token.cancelled() => None,
        new_height = height.recv_async() => Some(new_height?),
    } {
        // our RPC call is blocking
        let block = task::block_in_place(|| get_block(&rpc, new_height))?;
        if new_height % 100 == 0 {
            println!("fetched block {}", new_height);
        }
        blocks.send_async((new_height, block)).await?;
    }
    Ok(())
}

pub async fn process_block_zstd(
    token: CancellationToken,
    blocks_in: flume::Receiver<(u64, Block)>,
    stats: flume::Sender<ZstdBlockStats>,
    blocks_out: flume::Sender<(u64, Vec<u8>)>,
) -> anyhow::Result<()> {
    while let Some(block_with_height) = select! {
        _ = token.cancelled() => None,
        block_with_height = blocks_in.recv_async() => Some(block_with_height?),
    } {
        let (height, block) = block_with_height;
        let (block_stats, raw_block) = zstd_block(&block, height).await?;
        println!("compressed block {}", height);
        stats.send_async(block_stats).await?;
        blocks_out.send_async((height, raw_block)).await?;
    }
    Ok(())
}

pub async fn write_raw_block(
    token: CancellationToken,
    blocks_in: flume::Receiver<(u64, Vec<u8>)>,
    job_out: flume::Sender<()>,
    dir: PathBuf,
) -> anyhow::Result<()> {
    while let Some(block_with_height) = select! {
        _ = token.cancelled() => None,
        block_with_height = blocks_in.recv_async() => Some(block_with_height?),
    } {
        let (height, block) = block_with_height;
        let filename = format!("{}.blk", height);
        let filepath = dir.join(filename);
        tokio::fs::write(filepath, &block).await?;
        if height % 100 == 0 {
            println!("wrote block {}", height);
        }
        job_out.send_async(()).await?;
    }
    Ok(())
}

pub async fn write_csv(
    token: CancellationToken,
    mut statwriter: csv::Writer<fs::File>,
    stats_in: flume::Receiver<ZstdBlockStats>,
    job_out: flume::Sender<()>,
) -> anyhow::Result<()> {
    while let Some(stats) = select! {
        _ = token.cancelled() => None,
        stats = stats_in.recv_async() => Some(stats?),
    } {
        statwriter.serialize(stats)?;
        job_out.send_async(()).await?;
    }
    Ok(())
}

pub async fn send_heights(
    token: CancellationToken,
    heights: Range<u64>,
    height_tx: flume::Sender<u64>,
) -> anyhow::Result<()> {
    for height in heights {
        select! {
            _ = token.cancelled() => break,
        _ = height_tx.send_async(height) => {
            if height % 100 == 0 {
                println!("sent height {}", height);
            }
        },
        }
    }
    Ok(())
}

pub async fn count_msgs(
    token: CancellationToken,
    msgs: flume::Receiver<()>,
    title: String,
    target: u64,
) -> anyhow::Result<u64> {
    let mut count = 0;
    while count < target {
        select! {
            _ = token.cancelled() => break,
            _ = msgs.recv_async() => {
                count += 1;
                if count % 100 == 0 {
                    println!("{title}: received {count} messages");
                }
            }
        }
    }
    Ok(count)
}

pub fn pipeline_checks(
    ack_chans: Vec<(String, flume::Receiver<()>)>,
    cancel_handle: &CancellationToken,
    target: u64,
) -> JoinSet<Result<u64, anyhow::Error>> {
    let mut completion_set = JoinSet::new();
    for (title, ack_chan) in ack_chans.into_iter() {
        completion_set.spawn(count_msgs(
            cancel_handle.child_token(),
            ack_chan,
            title,
            target,
        ));
    }
    completion_set
}
