use azure_storage::ConnectionString;
use azure_storage_blobs::{container::operations::ListBlobsResponse, prelude::*};
use bytes::Bytes;
use futures::StreamExt;
use polars::prelude::*;
use std::{io::Cursor, time::Instant};

// ============ Todo ============
// Evaluate while downloading blob-files
// Generate wraper-error to have more exact errors while writing code
// Make a function to measure memory on dev-tools
// Create get_input function
// Create filter_data
// Filter_df
// Generate parallelism
// Move every config data to env
// Add some kind of error handler inside of methods of DataHandler (in the best case something generic)
// Find how to use Proxie

pub struct DataHandler {
    container_client: Option<ContainerClient>,
}
impl DataHandler {
    pub fn new(container_name: &str, connection_string: &str) -> DataHandler {
        let connection_string = ConnectionString::new(connection_string).unwrap();
        let blob_service = BlobServiceClient::new(
            connection_string.account_name.unwrap(),
            connection_string.storage_credentials().unwrap(),
        );
        let container_client = Some(blob_service.container_client(container_name));

        return DataHandler { container_client };
    }

    pub async fn get_blobs(&mut self) -> ListBlobsResponse {
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

    pub fn filter_blobs(data: ListBlobsResponse, filter: impl FnMut(&Blob) -> bool) -> Vec<Blob> {
        data.blobs
            .blobs()
            .into_iter()
            .cloned()
            .filter(filter)
            .collect::<Vec<Blob>>()
    }

    pub fn filter_df_equal(df: DataFrame, column: &str, value: impl NumericNative) -> DataFrame {
        let filter: ChunkedArray<BooleanType> = df.column(column).unwrap().equal(value).unwrap();
        let result = df.filter(&filter).unwrap();
        return result;
    }

    // pub fn filter_dataframe(data:DataFrame, filter:)
}
