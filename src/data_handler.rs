use azure_storage::ConnectionString;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::StreamExt;
use polars::{export::num::PrimInt, prelude::*};
use std::{io::Cursor, num::NonZeroU32};

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

    pub async fn get_blobs(&mut self) -> Vec<Blob> {
        let mut blobs: Vec<Blob> = vec![];
        let mut stream = self
            .container_client
            .as_mut()
            .unwrap()
            .list_blobs()
            .max_results(NonZeroU32::new(3u32).unwrap())
            .into_stream();

        while let Some(value) = stream.next().await {
            value
                .unwrap()
                .blobs
                .blobs()
                .for_each(|b| blobs.push(b.to_owned()));
        }

        println!("Len Blobs:{}", blobs.len());
        return blobs;
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

    pub fn get_data_frame(data: Bytes, file_type: &String) -> DataFrame {
        let reader = Cursor::new(data); // Create a Cursor pointing towards the Bytes that compound the Blob
        if file_type == "csv" {
            return CsvReader::new(reader)
                .with_ignore_parser_errors(true)
                .finish()
                .unwrap();
        }
        ParquetReader::new(reader).finish().unwrap()
        // Read the Cursor and create a DataFrame
    }

    pub fn filter_blobs(data: Vec<Blob>, filter: impl FnMut(&Blob) -> bool) -> Vec<Blob> {
        let result: Vec<Blob> = data.into_iter().filter(filter).collect();
        return result;
    }

    pub fn filter_df_equal(df: DataFrame, column: &str, value: impl NumericNative) -> DataFrame {
        let filter = df.column(column).unwrap().equal(value).unwrap();
        let result = df.filter(&filter).unwrap();
        return result;
    }

    // pub fn filter_dataframe(data:DataFrame, filter:)
}
