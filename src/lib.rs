use async_compression::tokio::write::ZstdEncoder;
use bitcoin::Block;
use bitcoin::consensus;
use bitcoincore_rpc::bitcoin::consensus::Encodable;
use bitcoincore_rpc::{Client, RpcApi, bitcoin};
use tokio::io::AsyncWriteExt;

// blocks are average size 900 kB; 727791885830 bytes / 884760 blocks
pub const AVG_BLOCK_SIZE: usize = 900_000;

// lookup block by height, return decoded block and its consensus size
pub fn get_block(rpc: &Client, height: u64) -> anyhow::Result<Block> {
    let blockhash = rpc.get_block_hash(height)?;
    let block_hex = rpc.get_block_hex(&blockhash)?;
    Ok(consensus::encode::deserialize_hex(&block_hex)?)
}

// encode a block, compress with zstd, and report the compressed size
pub async fn zstd_block(block: Block, height: u64) -> anyhow::Result<(u64, usize, usize, Vec<u8>)> {
    let mut block_buf = Vec::with_capacity(AVG_BLOCK_SIZE);
    block.consensus_encode(&mut block_buf)?;
    let block_size = block_buf.len();

    // at least as big as uncompressed input
    let mut zstd_buf = Vec::with_capacity(block_size);
    let mut cmp = ZstdEncoder::with_quality(&mut zstd_buf, async_compression::Level::Default);
    cmp.write_all(&block_buf).await?;
    cmp.flush().await?;

    let cmp_size = zstd_buf.len();
    Ok((height, block_size, cmp_size, block_buf))
}
