mod data_handler;
mod dev_tools;
use std::time::Instant;

use azure_storage_blobs::blob::Blob;
use data_handler::DataHandler;
use polars::{
    prelude::{BooleanType, ChunkedArray, DataFrame, NumericNative},
    series::ChunkCompare,
};
// use dev_tools::DevTools;
// const CONNECTION_STRING:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

const BLOB_FOR_TEST: &str = "reject_data/rawdata_201809";

struct ConfigModel<T: NumericNative> {
    name: String,
    column_filter: String,
    value: T,
}
impl<T: NumericNative> ConfigModel<T> {
    fn new(name: String, column_filter: String, value: T) -> ConfigModel<T> {
        return ConfigModel {
            name,
            column_filter,
            value,
        };
    }
}

#[tokio::main]
async fn main() {
    let start_time = Instant::now();
    let mut blob_handler = DataHandler::new("historicaliods", CONNECTION_STRING);
    let blob_test = String::from(BLOB_FOR_TEST);
    let config: ConfigModel<u32> = ConfigModel::new(blob_test, String::from("RcdIdx"), 130096);
    let filtered_blobs = filter_data(&config, &mut blob_handler).await;
    analyse_data(&config, &mut blob_handler, filtered_blobs).await;
    let duration = start_time.elapsed().as_secs_f32();
    println!(
        "Looking for:{} with the value of {}",
        config.column_filter, config.value
    );
    println!("The execution took: {}s", duration)
}

async fn filter_data<T: NumericNative>(
    config: &ConfigModel<T>,
    handler: &mut DataHandler,
) -> Vec<Blob> {
    let blobs = handler.get_blobs().await;
    let result = DataHandler::filter_blobs(blobs, |b: &Blob| b.name.contains(&*config.name));
    return result;
}

async fn analyse_data<T: NumericNative>(
    config: &ConfigModel<T>,
    handler: &mut DataHandler,
    blobs: Vec<Blob>,
) -> bool {
    let count: u32 = 0;
    for b in blobs {
        let data = handler.get_specific_blob(&b.name).await;
        let df = DataHandler::get_data_frame(data);
        let founded = DataHandler::filter_df_equal(df, &config.column_filter, config.value);

        if founded.is_empty() {
            println!("{}", founded);
            return true;
        }
    }
    println!("Iterations:{}", count);
    return false;
}
