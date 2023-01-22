mod config;
mod data_handler;
mod dev_tools;

use config::Config;
use std::time::Instant;

use azure_storage_blobs::blob::Blob;
use data_handler::DataHandler;
use dev_tools::DevTools;
use polars::prelude::DataFrame;

#[tokio::main]
async fn main() {
    let config = Config::new();
    println!(
        "Starting search for {}, with the value of: {}",
        &config.name_blob, &config.value
    );

    let mut blob_handler = DataHandler::new(&config.container_name, &config.connection_string);

    let start_time = Instant::now();
    let filtered_blobs = filter_data(&config, &mut blob_handler).await;
    let duration_filtering = start_time.elapsed().as_secs_f32();

    println!("len filtered blobs: {}", filtered_blobs.len());
    let start_time = Instant::now();
    let result = analyse_data(&config, &mut blob_handler, filtered_blobs).await;
    let duration_analysing = start_time.elapsed().as_secs_f32();
    println!("Getting the data took: {}s", duration_filtering);
    println!("Anlysing the data took: {}s", duration_analysing);

    if result.is_some() {
        println!("{}", result.unwrap());
    }
    let finish = Some(String::from("The execution end, press enter to continue"));
    DevTools::get_input(finish);
}

async fn filter_data(config: &Config, handler: &mut DataHandler) -> Vec<Blob> {
    let blobs = handler
        .get_blobs(|b: &Blob| config.regx.is_match(&b.name))
        .await;
    return blobs;
}

async fn analyse_data(
    config: &Config,
    handler: &mut DataHandler,
    blobs: Vec<Blob>,
) -> Option<DataFrame> {
    println!("========================== Starting analisis ==========================");
    for b in blobs {
        let data = handler.get_specific_blob(&b.name).await;
        let mut df = DataHandler::get_data_frame(data, &config.file_type, &config.column_filter);
        let founded = DataHandler::filter_df_equal(&df, &config.column_filter, &config.value);

        if founded {
            println!("========================== FOUNDED ==========================");
            println!("{}", b.name);
            DataHandler::save_file(&mut df, &b.name, &config.path_save_files);
            return Some(df);
        }
    }
    return None;
}
