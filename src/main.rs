mod data_handler;
mod dev_tools;
use std::time::Instant;

use azure_storage_blobs::blob::Blob;
use data_handler::DataHandler;
use dev_tools::DevTools;
use polars::prelude::DataFrame;
use regex::Regex;
// use dev_tools::DevTools;
// const CONNECTION_STRING_IODS:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

// const CONNECTION_STRING_MIP:&str = "DefaultEndpointsProtocol=https;AccountName=famcaremipstoragedev;AccountKey=SHpMv/XYB8s8sSzVQacyUDyaTCyKnHEUJwcyHrCeaB1F38Eqjj5opNV1aYSU1CPHaAtb4Cnkxwgr5DhomhVSmg==;EndpointSuffix=core.windows.net";

// ================================= Data for MIP testing ===============================
// const BLOB_FOR_TEST: &str = "MEHOOPANY-RTCIS_SYSDTL_TRN-20220926-054802";
// "rtcis-processed"
// 0040037009630663372
// ================================= Data for IODS testing ===============================
// Blob_name -> reject_data/rawdata_20180902_9_8
// Container_name -> historicaliods
// Column_name -> RcdIdx
// Value -> 20762

struct ConfigModel {
    name: String,
    column_filter: String,
    value: String,
    file_type: String,
    regx: Regex,
    path_save_files: String,
}
impl ConfigModel {
    fn new(name: String, column_filter: String, value: String, file_type: String) -> ConfigModel {
        let file_type = file_type.to_lowercase();

        if file_type != "csv" && file_type != "parquet" {
            panic!("The only file types admited are: csv, parquet")
        }

        let regx = Regex::new(&format!("({})(.*)({})", name, file_type)).unwrap();
        let path_save_files: String = String::from("./");

        return ConfigModel {
            name,
            column_filter,
            value,
            file_type,
            regx,
            path_save_files,
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
    let value = DevTools::get_input(message);

    let message = Some(String::from(
        "Insert the type of file that we are looking for:",
    ));
    let file_type = DevTools::get_input(message);

    let start_time = Instant::now();
    let mut blob_handler = DataHandler::new(&container_name, &connection_string);
    let blob_test = String::from(blob_for_test);
    let config: ConfigModel = ConfigModel::new(blob_test, column, value, file_type);
    let filtered_blobs = filter_data(&config, &mut blob_handler).await;
    let duration_filtering = start_time.elapsed().as_secs_f32();
    println!("len filtered blobs: {}", filtered_blobs.len());
    let start_time = Instant::now();
    let result = analyse_data(&config, &mut blob_handler, filtered_blobs).await;
    let duration_analysing = start_time.elapsed().as_secs_f32();
    println!(
        "Looking for:{} with the value of {}",
        config.column_filter, config.value
    );
    println!("Getting the data took: {}s", duration_filtering);
    println!("Anlysing the data took: {}s", duration_analysing);
    if result.is_some() {
        println!("{}", result.unwrap());
    }
    let finish = Some(String::from("The execution end, press enter to continue"));
    DevTools::get_input(finish);
}

async fn filter_data(config: &ConfigModel, handler: &mut DataHandler) -> Vec<Blob> {
    let blobs = handler
        .get_blobs(|b: &Blob| config.regx.is_match(&b.name))
        .await;
    // let result = DataHandler::filter_blobs(blobs, |b: &Blob| config.regx.is_match(&b.name));
    return blobs;
}

async fn analyse_data(
    config: &ConfigModel,
    handler: &mut DataHandler,
    blobs: Vec<Blob>,
) -> Option<DataFrame> {
    println!("Starting analisis with the value:{}", &config.value);
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
