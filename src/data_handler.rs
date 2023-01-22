use azure_storage::ConnectionString;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::StreamExt;
// use polars::lazy::*;
use polars::prelude::*;
use std::{fs::File, io::Cursor, num::NonZeroU32};

// ============ Todo ============
// Convert filter df into a filter lazy-df
// Use pagination to avoid download all the blobs
// Generate wraper-error to have more exact errors while writing code
// Make a function to measure memory on dev-tools
// Create get_input function
// Generate parallelism
// Move every config data to env
// Add some kind of error handler inside of methods of DataHandler (in the best case something generic)
// Find a way to use Proxie

pub struct DataHandler {
    container_client: Option<ContainerClient>,
}
impl DataHandler {
    pub fn new(container_name: &str, connection_string: &str) -> Self {
        let connection_string = ConnectionString::new(connection_string).unwrap();
        let blob_service = BlobServiceClient::new(
            connection_string.account_name.unwrap(),
            connection_string.storage_credentials().unwrap(),
        );
        let container_client = Some(blob_service.container_client(container_name));

        return DataHandler { container_client };
    }

    pub async fn get_blobs(&mut self, filter: impl Fn(&Blob) -> bool) -> Vec<Blob> {
        let mut blobs: Vec<Blob> = vec![];
        let mut stream = self
            .container_client
            .as_mut()
            .unwrap()
            .list_blobs()
            .max_results(NonZeroU32::new(3000u32).unwrap())
            .into_stream();

        // Maybe execute the analysis inside of the loop so in that way will be able to spare time downloading innecessary data
        while let Some(value) = stream.next().await {
            value.unwrap().blobs.blobs().for_each(|b| {
                if filter(b) {
                    blobs.push(b.to_owned())
                }
            });
        }

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

    pub fn get_data_frame(data: Bytes, file_type: &String, field: &String) -> DataFrame {
        let reader = Cursor::new(data); // Create a Cursor pointing towards the Bytes that compound the Blob
        let field_schema = Field::new(field, DataType::Utf8);
        let schema = Schema::from(vec![field_schema].into_iter());
        if file_type == "csv" {
            return CsvReader::new(reader)
                .with_ignore_parser_errors(true)
                .with_dtypes(Some(&schema))
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

    pub fn filter_df_equal(df: &DataFrame, column: &str, value: &str) -> bool {
        let filter = df.column(column).unwrap().equal(value).unwrap();
        let result = df
            .column(column)
            .unwrap()
            .filter(&filter)
            .unwrap()
            .is_empty();
        if result {
            return false;
        }
        return true;
    }

    pub fn save_file(df: &mut DataFrame, file_name: &String, path: &String) {
        let file_name = format!("{}{}", path, file_name);
        let file = File::create(file_name).unwrap();
        CsvWriter::new(file).finish(df).unwrap();
    }
}
