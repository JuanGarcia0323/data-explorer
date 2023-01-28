mod config;
mod data_anlisis;
mod data_handler;
mod utils;

use azure_storage_blobs::prelude::Blob;
use config::Config;
use data_anlisis::multi_thread_analisis;
use data_handler::DataHandler;
use utils::{get_input, measure_time};

#[tokio::main]
async fn main() {
    measure_time(Some("All the execution took"), excute()).await;
}

async fn excute() {
    let config = Config::new();
    let mut blob_handler = DataHandler::new(&config.container_name, &config.connection_string);

    println!(
        "Starting search for {}, with the value of: {:?}",
        &config.name_blob, &config.value
    );

    // let filtered_blobs = measure_time(
    //     Some("Gettin and filtering the data, took"),
    //     get_filtered_data(&config, &mut blob_handler),
    // )
    // .await;

    // println!("len filtered blobs: {}", filtered_blobs.len());

    let result = measure_time(
        Some("Analysing and getting the data at the same time took"),
        blob_handler.analyse_while_download(
            |b: &Blob| config.regx.is_match(&b.name),
            |b: Vec<Blob>| multi_thread_analisis(&config, b),
        ),
    )
    .await;

    for r in result {
        let result = measure_time(Some("Analisis of thread took"), r).await;
        println!("result len: {}", result.len());
    }

    let finish = Some("The execution end, press enter to continue");
    get_input(finish);
}
