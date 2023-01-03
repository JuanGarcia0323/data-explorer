mod data_handler;
use data_handler::DataHandler;

const CONNECTION_STRING:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

const BLOB_FOR_TEST: &str = "reject_data/rawdata_20180202_6_10.parquet";
#[tokio::main]
async fn main() {
    let mut blob_handler = DataHandler::new("historicaliods", CONNECTION_STRING);
    let container_result = blob_handler.get_blobs().await;
    println!("{}", container_result.blobs.blobs().into_iter().count());
    let especific_blob = blob_handler
        .get_specific_blob(&String::from(BLOB_FOR_TEST))
        .await;

    println!("{}", blob_handler.get_data_frame(especific_blob))
}
