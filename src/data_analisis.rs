use azure_storage_blobs::prelude::Blob;
use polars::prelude::DataFrame;
use tokio::task::JoinHandle;

use crate::{config::Config, data_handler::DataHandler};
use bytes::Bytes;

use core::panic;
use polars::prelude::*;
use std::{
    fs::{create_dir_all, File},
    io::{Cursor, ErrorKind},
};

pub async fn multi_thread_analisis(
    config: &Config,
    blobs: Vec<Blob>,
) -> Vec<JoinHandle<Vec<Option<DataFrame>>>> {
    println!("Muti_thread_analisis have been called");
    let thread_slicing = config.thread_slicing;
    let limit = blobs.len();

    let mut handles: Vec<JoinHandle<Vec<Option<DataFrame>>>> = vec![];
    let mut counter = 0;

    while counter < limit {
        if counter + thread_slicing > limit {
            let rest = limit - counter;
            let sliced_blobs = blobs[counter..counter + rest].to_owned();
            counter = counter + rest;
            let handle = thread_analisis(sliced_blobs);
            handles.push(handle);
            continue;
        }

        let sliced_blobs = blobs[counter..counter + thread_slicing].to_owned();
        let handle = thread_analisis(sliced_blobs);
        handles.push(handle);
        counter = counter + thread_slicing;
    }

    println!("{} threads were created", handles.len());

    return handles;
}

pub async fn get_filtered_data(config: &Config, handler: &mut DataHandler) -> Vec<Blob> {
    let blobs = handler
        .get_blobs(|b: &Blob| config.regx.is_match(&b.name))
        .await;
    return blobs;
}

pub async fn analyse_data(
    config: &Config,
    handler: &mut DataHandler,
    blobs: Vec<Blob>,
    return_first: bool,
) -> Vec<Option<DataFrame>> {
    let mut results: Vec<Option<DataFrame>> = vec![];
    for b in blobs {
        let data = handler.get_specific_blob(&b.name).await;
        let mut df = get_data_frame(data, &config.file_type);
        let mut len_founded: u8 = 0;

        for (i, c) in config.column_filter.iter().enumerate() {
            let result = filter_column(&df, c, &config.value[i]);
            if result {
                len_founded = len_founded + 1
            }
        }

        let founded = len_founded == config.value.len() as u8;

        if founded && return_first {
            results.push(Some(df));
            return results;
        }

        if founded {
            save_file(&mut df, &b.name, &config.path_save_files, &config.file_type);
            results.push(Some(df));
        }
    }
    println!("results from thread: {}", results.len());
    return results;
}

pub fn thread_analisis(sliced_blobs: Vec<Blob>) -> JoinHandle<Vec<Option<DataFrame>>> {
    tokio::spawn(async move {
        let config = Config::new();
        let mut handler = DataHandler::new(&config.container_name, &config.connection_string);
        analyse_data(&config, &mut handler, sliced_blobs, false).await
    })
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

    fn conversion_evaluation<T: NumericNative>(value: &str, selected_col: &Series) -> Series
    where
        T: std::str::FromStr,
        <T as std::str::FromStr>::Err: std::fmt::Debug,
    {
        let value: T = value.parse().unwrap();
        let filter = selected_col.equal(value).unwrap();
        let result = selected_col.filter(&filter).unwrap();
        return result;
    }

    if type_column.is_float() {
        let result = conversion_evaluation::<f64>(value, selected_col);
        return !result.is_empty();
    }
    if type_column.is_integer() {
        let result = conversion_evaluation::<i64>(value, selected_col);
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
