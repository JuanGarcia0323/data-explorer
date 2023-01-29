mod config;
mod data_analisis;
mod data_handler;
mod utils;

use azure_storage_blobs::prelude::Blob;
use config::Config;
use data_analisis::multi_thread_analisis;
use data_handler::DataHandler;
use polars::prelude::DataFrame;
use utils::{get_input, measure_time};

#[tokio::main]
async fn main() {
    measure_time(Some("All the execution took"), excute()).await;

    let finish = Some("The execution end, press enter to continue");
    get_input(finish);
}

async fn excute() {
    let config = Config::new();
    let mut blob_handler = DataHandler::new(&config.container_name, &config.connection_string);
    let mut results: Vec<DataFrame> = vec![];

    println!(
        "Starting search for {}, with the value of: {:?}",
        &config.name_blob, &config.value
    );

    let analisis = measure_time(
        Some("Analysing and getting the data at the same time took"),
        blob_handler.analyse_while_download(
            |b: &Blob| config.regx.is_match(&b.name),
            |b: Vec<Blob>| multi_thread_analisis(&config, b),
            config.thread_calling,
        ),
    )
    .await;

    for r in analisis {
        for handle in r {
            let result_thread = handle.await.unwrap();
            for r in result_thread {
                if r.is_some() {
                    results.push(r.unwrap());
                }
            }
        }
    }

    println!("result len: {}", results.len());
}
