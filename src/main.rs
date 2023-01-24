mod config;
mod data_handler;
mod dev_tools;

use config::Config;
use std::time::Instant;
use tokio::task::JoinHandle;

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
    // let result = analyse_data(&config, &mut blob_handler, filtered_blobs).await;
    let result = multi_thread_analisis(&config, filtered_blobs).await;
    println!("{} results founded", result.len());

    let duration_analysing = start_time.elapsed().as_secs_f32();
    println!("Getting the data took: {duration_filtering}s");
    println!("Anlysing the data took: {duration_analysing}s");

    let finish = Some(String::from("The execution end, press enter to continue"));
    DevTools::get_input(finish);
}

async fn multi_thread_analisis(config: &Config, blobs: Vec<Blob>) -> Vec<DataFrame> {
    let mut counter = 0;
    let thread_slicing = config.thread_slicing.clone();
    let limit = blobs.len();
    let mut threads: u32 = 0;
    let mut handles: Vec<JoinHandle<Vec<Option<DataFrame>>>> = vec![];
    let mut result: Vec<DataFrame> = vec![];

    while counter < limit {
        if counter + thread_slicing > limit {
            let rest = limit - counter;
            let sliced_blobs = blobs[counter..counter + rest].to_owned();
            counter = counter + rest;
            let handle = thread_process(sliced_blobs);
            threads = threads + 1;
            handles.push(handle);
            continue;
        }

        let sliced_blobs = blobs[counter..counter + thread_slicing].to_owned();
        let handle = thread_process(sliced_blobs);
        threads = threads + 1;
        handles.push(handle);
        counter = counter + thread_slicing;
    }

    println!("{threads} threads were created");

    for handle in handles {
        let result_thread = handle.await.unwrap();
        for r in result_thread {
            if r.is_some() {
                result.push(r.unwrap())
            }
        }
    }

    return result;
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
    return_first: bool,
) -> Vec<Option<DataFrame>> {
    let mut results: Vec<Option<DataFrame>> = vec![];
    for b in blobs {
        let data = handler.get_specific_blob(&b.name).await;
        let mut df = DataHandler::get_data_frame(data, &config.file_type, &config.column_filter);
        let founded = DataHandler::filter_df_equal(&df, &config.column_filter, &config.value);

        if founded && return_first {
            results.push(Some(df));
            return results;
        }

        if founded {
            println!("========================== FOUNDED ==========================");
            println!("{}", b.name);
            DataHandler::save_file(&mut df, &b.name, &config.path_save_files);
            results.push(Some(df));
        }
    }
    return results;
}

fn thread_process(sliced_blobs: Vec<Blob>) -> JoinHandle<Vec<Option<DataFrame>>> {
    tokio::spawn(async move {
        let config = Config::new();
        let mut handler = DataHandler::new(&config.container_name, &config.connection_string);
        analyse_data(&config, &mut handler, sliced_blobs, false).await
    })
}
