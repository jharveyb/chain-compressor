use anyhow::anyhow;
use bitcoin::consensus::Decodable;
use std::fs::File;
use std::future::Future;
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

mod util;

// trait for an async fn that accepts (block, height) and returns (stats, new_block)
// where new_block may be a different type, like raw bytes
pub trait BlockProcessor<S, B>
where
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    type Future: Future<Output = anyhow::Result<(S, B)>> + Send + 'static;

    fn run(&self, block: Block, height: u64) -> Self::Future;
}

// Implement the trait for any function that returns a future with the
// correct signature; which can be a simple closure
impl<S, B, F, Fut> BlockProcessor<S, B> for F
where
    F: Fn(Block, u64) -> Fut + Send + Clone + Sync + 'static,
    Fut: Future<Output = anyhow::Result<(S, B)>> + Send + 'static,
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    type Future = Fut;

    fn run(&self, block: Block, height: u64) -> Self::Future {
        self(block, height)
    }
}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct TxPerBlockStats {
    height: u64,
    num_txs: u64,
}

// lookup block by height, return decoded block and its consensus size
pub fn get_block(rpc: &Client, height: u64) -> anyhow::Result<Block> {
    let blockhash = rpc.get_block_hash(height)?;
    let block_hex = rpc.get_block_hex(&blockhash)?;
    Ok(consensus::encode::deserialize_hex(&block_hex)?)
}

// encode a block, compress with zstd, and report the compressed size
pub async fn zstd_block(block: Block, height: u64) -> anyhow::Result<(ZstdBlockStats, Vec<u8>)> {
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

pub async fn count_block_txs(
    block: Block,
    height: u64,
) -> anyhow::Result<(TxPerBlockStats, Vec<u8>)> {
    let stats_line = TxPerBlockStats {
        height,
        num_txs: block.txdata.len() as u64,
    };
    Ok((stats_line, vec![]))
}

pub async fn fetch_block(
    token: CancellationToken,
    rpc: Client,
    height: flume::Receiver<u64>,
    blocks: flume::Sender<(u64, Block)>,
) -> anyhow::Result<()> {
    let mut heights = height.into_stream();
    while let Some(new_height) = util::recv_or_cancel(&token, &mut heights).await {
        // our RPC call is blocking
        let block = task::block_in_place(|| get_block(&rpc, new_height))?;
        if new_height % 100 == 0 {
            println!("fetched block {}", new_height);
        }
        blocks.send_async((new_height, block)).await?;
    }
    Ok(())
}

pub async fn process_block<B, S, P>(
    token: CancellationToken,
    blocks_in: flume::Receiver<(u64, Block)>,
    processor: P,
    stats_out: flume::Sender<S>,
    blocks_out: flume::Sender<(u64, B)>,
) -> anyhow::Result<()>
where
    P: BlockProcessor<S, B>,
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    // closure that creates new futures for each loop iteration
    let process = |block, height| processor.run(block, height);
    let mut block_stream = blocks_in.into_stream();
    while let Some(block_with_height) = util::recv_or_cancel(&token, &mut block_stream).await {
        let (height, block) = block_with_height;
        let (block_stats, new_block) = process(block, height).await?;
        stats_out.send_async(block_stats).await?;
        blocks_out.send_async((height, new_block)).await?;
    }
    Ok(())
}

pub fn process_block_workers<B, S, P>(
    cancel_handle: &CancellationToken,
    blocks_in: flume::Receiver<(u64, Block)>,
    processor: P,
    stats_out: flume::Sender<S>,
    blocks_out: flume::Sender<(u64, B)>,
    worker_count: u16,
) -> JoinSet<Result<(), anyhow::Error>>
where
    P: BlockProcessor<S, B> + Send + Clone + Sync + 'static,
    S: Send + Sync + 'static,
    B: Send + Sync + 'static,
{
    let mut worker_set = JoinSet::new();
    for _ in 0..worker_count {
        let processor = processor.clone();
        let _ = worker_set.spawn(process_block(
            cancel_handle.child_token(),
            blocks_in.clone(),
            processor,
            stats_out.clone(),
            blocks_out.clone(),
        ));
    }
    worker_set
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
        let (block_stats, raw_block) = zstd_block(block, height).await?;
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
    let mut blocks = blocks_in.into_stream();
    while let Some((height, block)) = util::recv_or_cancel(&token, &mut blocks).await {
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

pub async fn read_block(
    token: CancellationToken,
    paths_in: flume::Receiver<PathBuf>,
    blocks_out: flume::Sender<(u64, Block)>,
) -> anyhow::Result<()> {
    let mut paths = paths_in.into_stream();
    while let Some(blockpath) = util::recv_or_cancel(&token, &mut paths).await {
        let raw_block = tokio::fs::read(&blockpath).await?;
        let height = blockpath
            .file_stem()
            .and_then(|s| s.to_str().and_then(|n| n.parse::<u64>().ok()));
        let height = height.ok_or_else(|| anyhow!("Invalid block filename"))?;
        let block =
            task::spawn_blocking(move || Block::consensus_decode(&mut raw_block.as_slice()))
                .await??;
        blocks_out.send_async((height, block)).await?;
    }
    Ok(())
}

pub async fn drop_block(
    token: CancellationToken,
    blocks_in: flume::Receiver<(u64, Vec<u8>)>,
) -> anyhow::Result<()> {
    let mut blocks = blocks_in.into_stream();
    // wait and no-op
    while let Some((_, _)) = util::recv_or_cancel(&token, &mut blocks).await {}
    Ok(())
}

pub async fn write_csv<S>(
    token: CancellationToken,
    mut statwriter: csv::Writer<File>,
    stats_in: flume::Receiver<S>,
    job_out: flume::Sender<()>,
) -> anyhow::Result<()>
where
    S: Send + Sync + 'static + Serialize,
{
    let mut stats = stats_in.into_stream();
    while let Some(stat_line) = util::recv_or_cancel(&token, &mut stats).await {
        statwriter.serialize(stat_line)?;
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

pub async fn send_paths(
    token: CancellationToken,
    paths: impl Iterator<Item = PathBuf>,
    paths_tx: flume::Sender<PathBuf>,
    job_count: flume::Sender<()>,
) -> anyhow::Result<()> {
    for (i, path) in paths.enumerate() {
        select! {
            _ = token.cancelled() => break,
            _ = paths_tx.send_async(path) => {
                job_count.send_async(()).await?;
                if i % 100 == 0 {
                    println!("sent path #{}00", i);
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
    let mut msg_stream = msgs.into_stream();
    while count < target {
        if (util::recv_or_cancel(&token, &mut msg_stream).await).is_some() {
            count += 1;
            if count % 100 == 0 {
                println!("{title}: received {count} messages");
            }
        }
    }
    Ok(count)
}

pub async fn check_matching_job_count(
    token: CancellationToken,
    close: flume::Receiver<()>,
    start: flume::Receiver<()>,
    finish: flume::Receiver<()>,
) -> anyhow::Result<()> {
    let (mut start_count, mut finish_count) = (0, 0);
    let mut closed = false;
    loop {
        select! {
            _ = token.cancelled() => break,
            _ = start.recv_async() => start_count += 1,
            _ = finish.recv_async() => {
                finish_count += 1;
                if closed && start_count == finish_count {
                    break;
                }
            },
            closer = close.recv_async() => {
                if closer.is_err() {
                    closed = true;
                }
            }
        }
    }
    Ok(())
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
