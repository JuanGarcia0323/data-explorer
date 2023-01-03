use azure_storage::ConnectionString;
use azure_storage_blobs::{container::operations::ListBlobsResponse, prelude::*};
use bytes::Bytes;
use futures::StreamExt;
use polars::{export::chrono::format::Item, prelude::*};
use std::{error::Error, io::Cursor, time::Instant};

// ============ Todo ============
// Generate wraper-error to have more exact errors while writing code
// Make a function to measure memory on dev-tools
// Create get_input function
// Create filter_data
// Filter_df
// Analyze_df
// Generate parallelism
// Move every config data to env
// Add some kind of error handler inside of methods of DataHandler (in the best case something generic)
// Find how to use Proxie
// const CONNECTION_STRING:&str = "DefaultEndpointsProtocol=https;AccountName=azdsiodsbcdev;AccountKey=62gIy1bkl1D2atNDMqSv5sKLKMbOQLPlnIifO48qKMz88D+iDE7G1Yg7nlfi4pBQKqCQ89HEtPqv01GWowgzzA==;EndpointSuffix=core.windows.net";

pub struct DataHandler {
    container_client: Option<ContainerClient>,
    list_blobs: Vec<Blob>,
}
impl DataHandler {
    pub fn new(container_name: &str, connection_string: &str) -> DataHandler {
        let connection_string = ConnectionString::new(connection_string).unwrap();
        let blob_service = BlobServiceClient::new(
            connection_string.account_name.unwrap(),
            connection_string.storage_credentials().unwrap(),
        );
        let container_client = Some(blob_service.container_client(container_name));

        return DataHandler {
            container_client,
            list_blobs: vec![],
        };
    }

    pub async fn get_blobs(
        &mut self,
        // mut filter_function: impl FnMut(Blob) -> bool,
    ) -> ListBlobsResponse {
        // let result = self
        //     .container_client
        //     .as_ref()
        //     .unwrap()
        //     .list_blobs()
        //     .into_stream()
        //     .next()
        //     .await
        //     .into_iter()
        //     .filter(|b| {
        //         let blob = b.unwrap().blobs.blobs();
        //         let filter_result = filter_function(b);
        //         return filter_result;
        //     });

        let result = self
            .container_client
            .as_ref()
            .unwrap()
            .list_blobs()
            .into_stream()
            .next()
            .await
            .expect("An error ocurred while getting the blobs")
            .unwrap();
        let start_time = Instant::now();
        result
            .blobs
            .blobs()
            .for_each(|b| self.list_blobs.push(b.clone()));
        let duration = start_time.elapsed().as_secs_f32();
        println!("Saving blob inside of the Struct took: {}s", duration);
        return result;
    }

    pub async fn get_specific_blob(&self, blob_name: &String) -> Bytes {
        let blob_stream = self
            .container_client
            .as_ref()
            .unwrap() // Download specific blob using the name
            .blob_client(blob_name)
            .get()
            .into_stream()
            .next()
            .await
            .expect("The program failed while downloading the blob")
            .unwrap();

        blob_stream // Collect all the information on Bytes
            .data
            .collect()
            .await
            .expect("Error while dowloading blob_result data")
    }

    pub fn get_data_frame(data: Bytes) -> DataFrame {
        let reader = Cursor::new(data); // Create a Cursor pointing towards the Bytes that compound the Blob
        ParquetReader::new(reader).finish().unwrap()
        // Read the Cursor and create a DataFrame
    }

    pub fn filter_blobs(data: ListBlobsResponse, filter: Option<impl FnMut(&Blob) -> bool>) {
        let result = data
            .blobs
            .blobs()
            .into_iter()
            .filter(|b| filter.unwrap()(*b));
    }
}
// pub async fn get_data() -> Result<DataFrame, Box<dyn Error>> {
//     println!("Started");
//     let connection_string = ConnectionString::new(&CONNECTION_STRING)?;
//     let blob_service = BlobServiceClient::new(
//         connection_string.account_name.unwrap(),
//         connection_string.storage_credentials()?,
//     );
//     let container_client = blob_service.container_client("historicaliods"); // Select the container

//     let page = container_client // List 5000 blobs inside of the container
//         .list_blobs()
//         .into_stream()
//         .next()
//         .await
//         .expect("An error ocurred while geting the blobs");

//     for i in page?.blobs.blobs().into_iter() {
//         //Iterate ober all the blobs and print their name
//         println!(
//             "name:{}, includes 20180202? {}",
//             i.name,
//             i.name.contains("20180202")
//         )
//     }

//     println!("====================================================================");

//     let blob_stream = container_client // Download specific blob using the name
//         .blob_client("reject_data/rawdata_20180202_6_10.parquet")
//         .get()
//         .into_stream()
//         .next()
//         .await
//         .expect("The program failed while downloading the blob")?;

//     let result = blob_stream // Collect all the information on Bytes
//         .data
//         .collect()
//         .await
//         .expect("Error while dowloading blob_result data");

//     let reader = Cursor::new(result); // Create a Cursor pointing towards the Bytes that compound the Blob
//     let polars_result: DataFrame = ParquetReader::new(reader).finish()?; // Read the Cursor and create a DataFrame

//     println!("{}", polars_result); // Print the result
//     Ok(polars_result)
// }
