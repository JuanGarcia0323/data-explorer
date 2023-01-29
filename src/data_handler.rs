use azure_core::Pageable;
use azure_storage::ConnectionString;
use azure_storage_blobs::{container::operations::ListBlobsResponse, prelude::*};
use bytes::Bytes;
use futures::{Future, StreamExt};
use std::num::NonZeroU32;

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

    fn get_stream(&mut self) -> Pageable<ListBlobsResponse, azure_core::Error> {
        self.container_client
            .as_mut()
            .unwrap()
            .list_blobs()
            .max_results(NonZeroU32::new(3000u32).unwrap())
            .into_stream()
    }

    pub async fn analyse_while_download<T: Future>(
        &mut self,
        filter: impl Fn(&Blob) -> bool,
        while_download: impl Fn(Vec<Blob>) -> T,
        thread_calling: usize,
    ) -> Vec<T::Output> {
        let mut stream = self.get_stream();
        let mut filtered_blobs: Vec<Blob> = vec![];
        let mut result: Vec<T::Output> = vec![];

        while let Some(value) = stream.next().await {
            let blobs: Vec<Blob> = value.unwrap().blobs.blobs().map(|b| b.to_owned()).collect();
            for b in blobs {
                if filter(&b) {
                    filtered_blobs.push(b)
                }
                if filtered_blobs.len() > thread_calling {
                    result.push(while_download(filtered_blobs.split_off(0)).await);
                }
            }
        }

        result.push(while_download(filtered_blobs).await);

        return result;
    }

    pub async fn get_blobs(&mut self, filter: impl Fn(&Blob) -> bool) -> Vec<Blob> {
        let mut stream = self.get_stream();
        let mut blobs: Vec<Blob> = vec![];

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
}
