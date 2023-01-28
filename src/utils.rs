use futures::Future;
use std::{io::stdin, time::Instant};

pub async fn measure_time<T>(message: Option<&str>, cb: impl Future<Output = T>) -> T {
    let start_time = Instant::now();
    let result = cb.await;
    let duration = start_time.elapsed().as_secs_f32();

    if message.is_some() {
        println!("{}: {duration}s", message.unwrap());
        return result;
    }

    println!("{duration}s");
    return result;
}

pub fn get_input(message: Option<&str>) -> String {
    if message.is_some() {
        println!("{}", message.unwrap())
    }
    let mut new_string = String::new();
    stdin().read_line(&mut new_string).unwrap();
    return String::from(new_string.trim());
}
