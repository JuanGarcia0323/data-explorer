use core::ops::Range;
use rand::{distributions::uniform::SampleRange, Rng};
use std::{
    fs::{File, OpenOptions},
    io::{ErrorKind, Write},
    time::Instant,
};

pub struct DevTools {
    pub title_testing: String,
    pub message: Vec<String>,
    pub random_number: u32,
    pub range: Range<u32>,
}
impl DevTools {
    pub fn write_message(&mut self, message: String) {
        let formated_message = format!("\n \n{}", message);
        self.message.push(formated_message)
    }

    pub fn save_messages(&self) {
        let file_name = format!("{}.txt", &self.title_testing);
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_name)
            .unwrap_or_else(|error| {
                if error.kind() == ErrorKind::NotFound {
                    File::create(&file_name).unwrap()
                } else {
                    panic!("{}", error)
                }
            });
        let title = format!("\n \n========= {} =========", &self.title_testing);
        file.write(title.as_bytes()).unwrap();
        for m in &self.message {
            file.write(m.as_bytes()).unwrap();
        }
    }

    pub fn auto_generate_number<T: SampleRange<u32>>(&mut self, range: T) -> u32 {
        let mut rng = rand::thread_rng();
        let random: u32 = rng.gen_range(range);
        self.random_number = random;
        random
    }

    pub fn measure_performance(&mut self, cb_test: impl Fn(&Vec<u32>, u32) -> u32, times: u32) {
        // Should take a function as parameter and mesure the execution time as well should test the function the amount of times specified
        let results: Vec<u32> = (self.range.clone()).collect();
        let mut total_duration: f32 = 0.0;
        println!("Starting Test");

        for i in 0..times {
            let random_number: u32 = self.auto_generate_number(self.range.clone());
            // Measuring time
            let start_time = Instant::now();
            let index_of_result = cb_test(&results, random_number);
            let duration = start_time.elapsed();

            let finding_message = format!("The algorithm was looking for: {}", random_number);
            let sub_title = format!("--------- Iteration number: {} ---------", i + 1);
            let index_result_message = format!("The index of the result: {}", index_of_result);
            let checking_result_message =
                format!("Checking result: {}", results[index_of_result as usize]);
            let time_message = format!("Time: {:?}", duration);
            total_duration = total_duration + duration.as_secs_f32();

            println!("{}", sub_title);
            println!("{}", index_result_message);
            println!("{}", finding_message);
            println!("{}", checking_result_message);
            println!("{}", time_message);

            self.write_message(sub_title);
            self.write_message(index_result_message);
            self.write_message(finding_message);
            self.write_message(checking_result_message);
            self.write_message(time_message);

            if index_of_result != random_number {
                panic!("The returned result is incorrect");
            }
        }

        total_duration = total_duration / (times + 1) as f32;
        let total_duration_message = format!(
            "========= Average duration was: {:?}s =========",
            total_duration
        );
        self.write_message(total_duration_message);
        self.save_messages()
    }
}
