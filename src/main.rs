mod data_handler;
use data_handler::get_data;
#[tokio::main]
async fn main() {
    println!("Hello, world!");
    println!("DataFrame from main: {}", get_data().await.unwrap())
}
