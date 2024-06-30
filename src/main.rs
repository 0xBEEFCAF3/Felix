use std::collections::HashSet;

use anyhow::Result;
use bitcoin::{Block, Transaction, Witness};
use bitcoincore_rpc::{Auth, Client as BitcoinRpc, RawTx, RpcApi};
use ciborium;
use clap::Parser;
use plotters::prelude::*;

use log::{info, trace};
use sled::Db;

/// OP_CAT in hex
const OP_CAT: u8 = 0x7e;
/// Sled key for checkpoint
const CHECKPOINT_SLED_KEY: &str = "CHECKPOINT";
/// tip - BLOCK_DEPTH is when the indexer will stop. This is to avoid reorgs
/// even signet reorgs
const BLOCK_DEPTH: u64 = 6;

/// Simple program to greet a person
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// bitcoind url
    #[arg(long)]
    bitcoind_url: String,

    /// bitcoind url
    #[arg(long)]
    bitcoind_port: String,

    /// bitcoind user
    #[arg(long)]
    bitcoind_username: String,

    /// bitcoind url
    #[arg(long)]
    bitcoind_password: String,

    /// optional starting block, default is 193536
    #[arg(long, default_value = "193536")]
    start_block: u64,

    #[arg()]
    command: String,
}

struct App {
    bitcoind_rpc: BitcoinRpc,
    start_block: u64,
    db: Db,
}

impl App {
    fn new(args: Args) -> Self {
        info!(">>>>> args: {:?}", args);
        let auth = Auth::UserPass(args.bitcoind_username, args.bitcoind_password);
        let bitcoind_rpc = BitcoinRpc::new(
            format!("http://{}:{}", args.bitcoind_url, args.bitcoind_port).as_str(),
            auth,
        )
        .expect("connect to bitcoind");
        // test the connection
        bitcoind_rpc.get_block_count().expect("get block count");

        Self {
            bitcoind_rpc,
            start_block: args.start_block,
            db: sled::open("db").expect("open db"),
        }
    }

    fn start_index(&mut self) -> Result<()> {
        // get tip
        let tip = self.bitcoind_rpc.get_block_count()?;
        let index_till = tip - BLOCK_DEPTH;

        // get checkpoint
        let checkpoint = self.retrieve_check_point()?;
        info!("Current checkpoint height: {}", checkpoint);

        for height in checkpoint..index_till {
            let block = self.bitcoind_rpc.get_block_hash(height)?;
            let block = self.bitcoind_rpc.get_block(&block)?;
            self.parse_block(height, block)?;
            self.insert_check_point(height)?;
        }

        Ok(())
    }

    fn insert_check_point(&mut self, height: u64) -> Result<()> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&height, &mut bytes)?;
        self.db.insert(CHECKPOINT_SLED_KEY, bytes)?;
        self.db.flush()?;

        Ok(())
    }

    fn retrieve_check_point(&self) -> Result<u64> {
        // if no checkpoint, start from start_block
        if self.db.get(CHECKPOINT_SLED_KEY)?.is_none() {
            return Ok(self.start_block);
        }

        let checkpoint = self.db.get(CHECKPOINT_SLED_KEY)?.expect("checkpoint");
        let height = ciborium::from_reader::<u64, _>(checkpoint.as_ref())?;
        Ok(height)
    }

    fn insert_tx(&mut self, height: u64, tx: Transaction) -> Result<()> {
        let mut set = {
            if let Some(current_txs) = self.db.get(height.to_string())? {
                let map = ciborium::from_reader::<HashSet<Transaction>, _>(current_txs.as_ref())?;
                map
            } else {
                HashSet::new()
            }
        };

        set.insert(tx);
        let mut bytes = Vec::new();
        ciborium::into_writer(&set, &mut bytes)?;
        self.db.insert(height.to_string(), bytes)?;
        self.db.flush()?;

        Ok(())
    }

    fn parse_block(&mut self, height: u64, block: Block) -> Result<()> {
        info!("parsing block height: {}", height);
        info!("total txs in block: {}", block.txdata.len());
        let mut cat_count = 0;
        block.txdata.iter().for_each(|tx| {
            // Convert the entire transaction to raw hex and check of CAT usage
            // there could be other 7e's in there but this will filter out most of txs
            // that dont contain cat
            let raw_hex = tx.raw_hex();
            if raw_hex.contains(&OP_CAT.to_string()) {
                // info!("total inputs in tx: {}", tx.input.len());
                tx.input.iter().for_each(|input| {
                    if !input.witness.is_empty() && witness_includes_cat(&input.witness) {
                        trace!("found cat in witness for txid: {}", tx.compute_txid());
                        let _ = self.insert_tx(height, tx.clone()).expect("to insert tx");
                        cat_count += 1;
                    }
                })
            }
        });
        info!("block height: {}, cat txs: {}", height, cat_count);
        Ok(())
    }

    fn get_total_cat_txs(&self) -> Result<u64> {
        let mut total_cats = 0;
        let starting_height = self.start_block;
        let tip = self.bitcoind_rpc.get_block_count()? - BLOCK_DEPTH;
        for i in starting_height..tip {
            if let Some(txs) = self.db.get(i.to_string())? {
                let set = ciborium::from_reader::<HashSet<Transaction>, _>(txs.as_ref())?;
                total_cats += set.len() as u64;
            }
        }
        Ok(total_cats)
    }

    fn get_cats_starting_from(&self, height: u64) -> Result<Vec<u64>> {
        let mut total_cats = vec![];
        let tip = self.bitcoind_rpc.get_block_count()? - BLOCK_DEPTH;
        for i in height..tip {
            if let Some(txs) = self.db.get(i.to_string())? {
                let set = ciborium::from_reader::<HashSet<Transaction>, _>(txs.as_ref())?;
                total_cats.push(set.len() as u64);
            }
        }
        Ok(total_cats)
    }

    fn create_plots(&self) -> Result<()> {
        let tip = self.bitcoind_rpc.get_block_count()? - BLOCK_DEPTH;
        let height_range = (self.start_block as i32)..(tip as i32);
        let total_cats = self.get_cats_starting_from(self.start_block)?;
        let root = BitMapBackend::new("output/total_cat_txs.png", (640, 480)).into_drawing_area();
        root.fill(&WHITE)?;
        let mut chart = ChartBuilder::on(&root)
            .caption("CATS over time", ("sans-serif", 50).into_font())
            .margin(10)
            .x_label_area_size(30)
            .y_label_area_size(40)
            .build_cartesian_2d(height_range.clone(), 0..1000)?;

        chart
            .configure_mesh()
            .x_desc("block heights")
            .y_desc("txs using CAT")
            .draw()?;

        let binding = total_cats.iter().map(|c| *c as i32).collect::<Vec<i32>>();
        let heights = height_range.clone().into_iter().collect::<Vec<i32>>();
        let cats_to_plot: Vec<(i32, i32)> = binding
            .iter()
            .zip(heights.iter())
            .map(|(c, h)| (*h, *c))
            .collect();

        chart
            .draw_series(LineSeries::new(cats_to_plot, &RED))?
            .label("Txs using CAT")
            .legend(|(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], &RED));

        chart
            .configure_series_labels()
            .background_style(&WHITE.mix(0.8))
            .border_style(&BLACK)
            .draw()?;

        root.present()?;
        Ok(())
    }
}

fn witness_includes_cat(witness: &Witness) -> bool {
    witness.iter().any(|w| w.iter().any(|b| b == &OP_CAT))
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .filter_module("sled::", log::LevelFilter::Info)
        .filter_module("bitcoincore_rpc::", log::LevelFilter::Info)
        .init();

    let args = Args::parse();
    let mut app = App::new(args.clone());

    // Read the last argument as a command
    let command = std::env::args().last().unwrap();

    match command.as_str() {
        "start_index" => {
            app.start_index().expect("start indexing");
        }
        "get_checkpoint" => {
            let checkpoint = app.retrieve_check_point().expect("get checkpoint");
            let tip = app.bitcoind_rpc.get_block_count().expect("get block count");
            info!("checkpoint: {}", checkpoint);
            info!("tip: {}", tip);
        }
        "get_total_cat_txs" => {
            let total_cats = app.get_total_cat_txs().expect("get total cat txs");
            info!("total cat txs: {}", total_cats);
        }
        "plot" => app.create_plots().expect("create plots"),
        _ => {
            info!("No command found");
        }
    }
}
