use azure_storage::ConnectionString;
use azure_storage_blobs::prelude::*;
use futures::StreamExt;
use polars::prelude::*;
use std::{error::Error, io::Cursor};

// Todo
// Generate wraper-error to have more exact errors while writing code
// Make a function to measure memory on dev-tools
// Create get_input function
// Create filter_data
// Filter_df
// Analyze_df
// Generate parallelism

const CONNECTION_STRING:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

pub async fn get_data() -> Result<DataFrame, Box<dyn Error>> {
    println!("Started");
    let connection_string = ConnectionString::new(&CONNECTION_STRING).unwrap();
    let blob_service = BlobServiceClient::new(
        connection_string.account_name.unwrap(),
        connection_string.storage_credentials()?,
    );
    let container_client = blob_service.container_client("historicaliods"); // Select the container

    let page = container_client // List 5000 blobs inside of the container
        .list_blobs()
        .into_stream()
        .next()
        .await
        .expect("test");

    for i in page.unwrap().blobs.blobs().into_iter() {
        //Iterate ober all the blobs and print their name
        println!("{}", i.name)
    }

    println!("====================================================================");

    let blob_stream = container_client // Download specific blob using the name
        .blob_client("reject_data/rawdata_20180202_6_10.parquet")
        .get()
        .into_stream()
        .next()
        .await
        .expect("The program failed while downloading the blob")
        .unwrap();

    let result = blob_stream // Collect all the information on Bytes
        .data
        .collect()
        .await
        .expect("Error while dowloading blob_result data");

    let reader = Cursor::new(result); // Create a Cursor pointing towards the Bytes that compound the Blob
    let polars_result: DataFrame = ParquetReader::new(reader).finish()?; // Read the Cursor and create a DataFrame

    println!("{}", polars_result); // Print the result
    Ok(polars_result)
}
