use azure_storage::ConnectionString;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures::StreamExt;
// use polars::lazy::*;
use core::panic;
use polars::prelude::*;
use std::{
    fs::{create_dir_all, File},
    io::{Cursor, ErrorKind},
    num::NonZeroU32,
};

// ============ Todo ============
// Convert filter df into a filter lazy-df
// Generate wraper-error to have more exact errors while writing code
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

    pub async fn get_specific_blob(&self, blob_name: &str) -> Bytes {
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

    pub fn get_data_frame(data: Bytes, file_type: &str) -> DataFrame {
        let reader = Cursor::new(data); // Create a Cursor pointing towards the Bytes that compound the Blob
        if file_type == "csv" {
            return CsvReader::new(reader)
                .with_ignore_parser_errors(true)
                .finish()
                .unwrap();
        }
        ParquetReader::new(reader).finish().unwrap()
    }

    pub fn filter_blobs(data: Vec<Blob>, filter: impl FnMut(&Blob) -> bool) -> Vec<Blob> {
        let result: Vec<Blob> = data.into_iter().filter(filter).collect();
        return result;
    }

    pub fn filter_column(df: &DataFrame, column: &str, value: &str) -> bool {
        let selected_col = df.column(column).unwrap();
        let type_column = selected_col.dtype();

        if type_column.is_float() {
            let value: f64 = value.parse().unwrap();
            let filter = selected_col.equal(value).unwrap();
            let result = selected_col.filter(&filter).unwrap();
            return !result.is_empty();
        }
        if type_column.is_integer() {
            let value: i64 = value.parse().unwrap();
            let filter = selected_col.equal(value).unwrap();
            let result = selected_col.filter(&filter).unwrap();
            return !result.is_empty();
        }
        let filter = selected_col.equal(value).unwrap();
        let result = selected_col.filter(&filter).unwrap();
        return !result.is_empty();
    }

    pub fn save_file(df: &mut DataFrame, file_name: &str, path: &str, file_type: &str) {
        let mut checked_path: String = String::from(path);
        let mut checked_file_name: String = String::from(file_name);

        if file_name.contains("/") {
            let mut paths: Vec<String> = file_name.split("/").map(|s| String::from(s)).collect();
            checked_file_name = paths.pop().unwrap();
            checked_path = format!("{}/", paths.join("/"))
        }

        let file_name = format!("{checked_path}{checked_file_name}");
        let file = File::create(&file_name).unwrap_or_else(|err| {
            if err.kind() == ErrorKind::NotFound {
                create_dir_all(&checked_path).unwrap();
                return File::create(&file_name).unwrap();
            }
            panic!("{err}")
        });

        if file_type == "csv" {
            CsvWriter::new(file).finish(df).unwrap();
            return;
        }

        ParquetWriter::new(file).finish(df).unwrap();
    }
}
