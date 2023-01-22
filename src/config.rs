use regex::Regex;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Error as IoError;
use toml;

#[derive(Serialize, Deserialize, Debug)]
struct ConfigToml {
    connection: Option<ConfigConnection>,
    search: Option<ConfigSearch>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigConnection {
    connection_string: Option<String>,
    container_name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigSearch {
    name_blob: Option<String>,
    value: Option<String>,
    column_filter: Option<String>,
    file_type: Option<String>,
    path_save_files: Option<String>,
}

#[derive(Debug)]
pub struct Config {
    pub connection_string: String,
    pub container_name: String,
    pub name_blob: String,
    pub value: String,
    pub column_filter: String,
    pub file_type: String,
    pub path_save_files: String,
    pub regx: Regex,
}

impl Config {
    pub fn new() -> Self {
        let config_filepaths: [&str; 2] = ["./config.toml", "./Config.toml"];

        let mut content: String = String::from("");

        for filepath in config_filepaths {
            let result: Result<String, IoError> = fs::read_to_string(filepath);

            if result.is_ok() {
                content = result.unwrap();
                break;
            }
        }

        let config_toml: ConfigToml = toml::from_str(&content).unwrap_or_else(|_| {
            println!("Failed to create ConfigToml Object out of config file.");
            ConfigToml {
                connection: None,
                search: None,
            }
        });

        let (connection_string, container_name): (String, String) = match config_toml.connection {
            Some(connection) => {
                let connection_string: String = connection.connection_string.unwrap_or_else(|| {
                    panic!("Missing field connection_string in table connection.");
                });

                let container_name: String = connection.container_name.unwrap_or_else(|| {
                    panic!("Missing field container_name in table connection.");
                });

                (connection_string, container_name)
            }
            None => {
                panic!("Missing table connection")
            }
        };

        let (name_blob, value, column_filter, file_type, path_save_files): (
            String,
            String,
            String,
            String,
            String,
        ) = match config_toml.search {
            Some(search) => {
                let name_blob: String = search.name_blob.unwrap_or_else(|| {
                    panic!("Missing field name_blob in table search.");
                });

                let value: String = search.value.unwrap_or_else(|| {
                    panic!("Missing field value in table search.");
                });

                let column_filter: String = search.column_filter.unwrap_or_else(|| {
                    panic!("Missing field column_filter in table search.");
                });

                let file_type: String = search.file_type.unwrap_or_else(|| {
                    panic!("Missing field file_type in table search.");
                });

                let path_save_files: String = search.path_save_files.unwrap_or_else(|| {
                    println!("Missing field path_save_files in table search.");
                    String::from("./")
                });

                (name_blob, value, column_filter, file_type, path_save_files)
            }
            None => {
                panic!("Missing search table")
            }
        };

        let file_type = file_type.to_lowercase();

        if file_type != "csv" && file_type != "parquet" {
            panic!("The only file types admited are: csv, parquet")
        }

        let regx = Regex::new(&format!("({})(.*)({})", name_blob, file_type)).unwrap();

        Config {
            connection_string,
            container_name,
            name_blob,
            column_filter,
            value,
            file_type,
            regx,
            path_save_files,
        }
    }
}