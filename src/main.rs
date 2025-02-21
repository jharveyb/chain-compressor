use bitcoincore_rpc::{Auth, Client, RpcApi};
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

            // let chain_tip = chaininfo.blocks;
            Ok(())
        }
    }
}
