mod data_handler;
mod dev_tools;
use std::time::Instant;

use azure_storage_blobs::blob::Blob;
use data_handler::DataHandler;
use dev_tools::DevTools;
use polars::{io::csv, prelude::NumericNative};
// use dev_tools::DevTools;
// const CONNECTION_STRING_IODS:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

// const CONNECTION_STRING_MIP:&str = "DefaultEndpointsProtocol=https;AccountName=famcaremipstoragedev;AccountKey=SHpMv/XYB8s8sSzVQacyUDyaTCyKnHEUJwcyHrCeaB1F38Eqjj5opNV1aYSU1CPHaAtb4Cnkxwgr5DhomhVSmg==;EndpointSuffix=core.windows.net";

// ================================= Data for MIP testing ===============================
// const BLOB_FOR_TEST: &str = "MEHOOPANY-RTCIS_SYSDTL_TRN-2022";
// "rtcis-processed"
// 40037008630574700
// ================================= Data for IODS testing ===============================
// Blob_name -> reject_data/rawdata_20180902_9_8
// Container_name -> historicaliods
// Column_name -> RcdIdx
// Value -> 20762

struct ConfigModel<T: NumericNative> {
    name: String,
    column_filter: String,
    value: T,
    file_type: String,
}
impl<T: NumericNative> ConfigModel<T> {
    fn new(name: String, column_filter: String, value: T, file_type: String) -> ConfigModel<T> {
        let file_type = file_type.to_lowercase();

        if file_type != "csv" && file_type != "parquet" {
            panic!("The only file types admited are: csv, parquet")
        }

        return ConfigModel {
            name,
            column_filter,
            value,
            file_type,
        };
    }
}

#[tokio::main]
async fn main() {
    let message = Some(String::from("Insert the conection-string:"));
    let connection_string = DevTools::get_input(message);

    let message = Some(String::from("Insert the name of the blob:"));
    let blob_for_test = DevTools::get_input(message);

    let message = Some(String::from("Insert the name of the container:"));
    let container_name = DevTools::get_input(message);

    let message = Some(String::from("Insert the name of the column to filter:"));
    let column = DevTools::get_input(message);

    let message = Some(String::from("Insert the value that we are looking for:"));
    let value: u64 = DevTools::get_input(message).parse().unwrap();

    let message = Some(String::from(
        "Insert the type of file that we are looking for:",
    ));
    let file_type: String = DevTools::get_input(message).parse().unwrap();

    let start_time = Instant::now();
    let mut blob_handler = DataHandler::new(&container_name, &connection_string);
    let blob_test = String::from(blob_for_test);
    let config: ConfigModel<u64> = ConfigModel::new(blob_test, column, value, file_type);
    let filtered_blobs = filter_data(&config, &mut blob_handler).await;
    let duration_filtering = start_time.elapsed().as_secs_f32();
    let start_time = Instant::now();
    analyse_data(&config, &mut blob_handler, filtered_blobs).await;
    let duration_analysing = start_time.elapsed().as_secs_f32();
    println!(
        "Looking for:{} with the value of {}",
        config.column_filter, config.value
    );
    println!("Getting the data took: {}s", duration_filtering);
    println!("Anlysing the data took: {}s", duration_analysing);
    let finish = Some(String::from("The execution end, press enter to continue"));
    DevTools::get_input(finish);
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
    for b in blobs {
        let data = handler.get_specific_blob(&b.name).await;
        let df = DataHandler::get_data_frame(data, &config.file_type);
        let founded = DataHandler::filter_df_equal(df, &config.column_filter, config.value);

        if founded.is_empty() {
            println!("{}", b.name);
            println!("{}", founded);
            return true;
        }
    }
    return false;
}
