mod data_handler;
mod dev_tools;
use data_handler::DataHandler;
use dev_tools::DevTools;

const CONNECTION_STRING:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

const BLOB_FOR_TEST: &str = "reject_data/rawdata_20180202_6_10.parquet";
#[tokio::main]
async fn main() {
    let mut dev_tools = DevTools::new(String::from("Blob Transfer"), 1..5000);
    let mut blob_handler = DataHandler::new("historicaliods", CONNECTION_STRING);
    let container_result = blob_handler.get_blobs().await;
    let name_random_blob = container_result
        .blobs
        .blobs()
        .cloned()
        .enumerate()
        .find(|(i, _)| *i == random_number as usize)
        .unwrap()
        .1
        .name;
    let filtered_blob = DataHandler::filter_blobs(container_result, |b| b.name == name_random_blob);
    for i in filtered_blob {
        let especific_blob = blob_handler.get_specific_blob(&i.name).await;
        let df = DataHandler::get_data_frame(especific_blob);
        let filtered_df = DataHandler::filter_df_equal(df, "FaultCode", "12099");
        println!("{}", filtered_df)
    }
}
