use csv::WriterBuilder;
use futures::Stream;
use futures::StreamExt;
use std::fs::{self, File};
use std::path::PathBuf;
use tokio::select;
use tokio_util::sync::CancellationToken;

pub async fn recv_or_cancel<T>(
    token: &CancellationToken,
    receiver: &mut (impl Stream<Item = T> + Unpin),
) -> Option<T> {
    select! {
        _ = token.cancelled() => None,
        // propagate the None from an empty stream
        result = receiver.next() => result,
    }
}

pub fn init_csv_writer(filename: &str, statdir: &PathBuf) -> anyhow::Result<csv::Writer<fs::File>> {
    // create our CSV reader for exporting stats
    if !fs::exists(statdir)? {
        fs::create_dir_all(statdir)?;
    }

    // create stats file before appending; we shouldn't end up
    // truncating existing contents
    let stats_path = statdir.join(filename);
    let existing_file = fs::exists(&stats_path)?;
    if !existing_file {
        let _ = File::create(&stats_path)?;
    }

    let stats_file = File::options().append(true).open(&stats_path)?;
    let mut writer = WriterBuilder::new();

    // don't write CSV headers again if we're appending
    if existing_file {
        writer.has_headers(true);
    }

    Ok(writer.from_writer(stats_file))
}
