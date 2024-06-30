use std::collections::HashSet;

use anyhow::Result;
use bitcoin::{Block, Script, Transaction, Witness};
use bitcoincore_rpc::{Auth, Client as BitcoinRpc, RpcApi};
use ciborium;
use clap::Parser;
use log::info;
use plotters::prelude::*;
use sled::Db;

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

    /// db path
    #[arg(long, default_value = "db")]
    db_path: String,

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
        info!("opening db at: {}", args.db_path);
        Self {
            bitcoind_rpc,
            start_block: args.start_block,
            db: sled::open(args.db_path).expect("open db"),
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
        for tx in block.txdata.iter() {
            for input in tx.input.iter() {
                if witness_includes_cat(&input.witness) {
                    // Double check that the prevout is a P2TR
                    let prevout = self
                        .bitcoind_rpc
                        .get_raw_transaction(&input.previous_output.txid, None)?;
                    let prev_output = prevout.output[input.previous_output.vout as usize].clone();
                    let script_pubkey = prev_output.script_pubkey.clone();
                    if script_pubkey.is_p2tr() {
                        info!("found cat in witness for txid: {}", tx.compute_txid());
                        let _ = self.insert_tx(height, tx.clone()).expect("to insert tx");
                        cat_count += 1;
                    }
                }
            }
        }
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

    /// Return a vector of tuples of block height and total cat txs for that block
    fn get_cats_in_range(&self, start: u64, finish: u64) -> Result<Vec<(i32, i32)>> {
        let mut total_cats = vec![];
        for i in start..finish {
            if let Some(txs) = self.db.get(i.to_string())? {
                let set = ciborium::from_reader::<HashSet<Transaction>, _>(txs.as_ref())?;
                total_cats.push((i as i32, set.len() as i32));
            } else {
                total_cats.push((i as i32, 0));
            }
        }
        Ok(total_cats)
    }

    fn create_plots(&self) -> Result<()> {
        let tip = self.bitcoind_rpc.get_block_count()? - BLOCK_DEPTH;
        let height_range = (self.start_block as i32)..(tip as i32);
        let total_cats = self.get_cats_in_range(self.start_block, tip)?;
        let root = BitMapBackend::new("output/total_cat_txs.png", (1500, 800)).into_drawing_area();
        root.fill(&WHITE)?;
        let mut chart = ChartBuilder::on(&root)
            .caption("CATS over time", ("sans-serif", 50).into_font())
            .margin(10)
            .x_label_area_size(30)
            .y_label_area_size(40)
            .build_cartesian_2d(height_range.clone(), 0..300)?;

        chart
            .configure_mesh()
            .x_desc("block heights")
            .y_desc("txs using CAT")
            .draw()?;

        chart
            .draw_series(LineSeries::new(total_cats, &RED))?
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
    // get the second to last element in the witness which should be the tapscript
    // ignoring all annex things
    if witness.len() <= 2 {
        return false;
    }

    let tapscript = Script::from_bytes(witness.nth(witness.len() - 2).expect("witness"));
    tapscript.to_asm_string().contains("OP_CAT")
}

fn main() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .filter_module("sled::", log::LevelFilter::Info)
        .filter_module("bitcoincore_rpc::", log::LevelFilter::Info)
        .init();

    let args = Args::parse();
    let mut app = App::new(args.clone());

    // let tx = app
    //     .bitcoind_rpc
    //     .get_raw_transaction(
    //         &Txid::from_str("f7db5fd1d1355448dfb0f4b956257b65e648855ba89d74f7e5dded3f7d7eec91")
    //             .unwrap(),
    //         None,
    //     )
    //     .expect("get tx");

    // let witness = tx.input[0].witness.clone();
    // let res = witness_includes_cat(&witness);
    // println!("{:?}", res);

    // println!("{:?}", witness);
    // panic!("done");

    // Read the last argument as a command
    let command = std::env::args().last().expect("need a command");

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

    // let txs = app.db.get("194229".to_string()).expect("get txs");
    // if let Some(txs) = txs {
    //     let set =
    //         ciborium::from_reader::<HashSet<Transaction>, _>(txs.as_ref()).expect("from reader");
    //     info!("total txs: {}", set.len());
    //     // print txids
    //     set.iter().for_each(|tx| {
    //         info!("txid: {}", tx.compute_txid());
    //     });
    // }
}
