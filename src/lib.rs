#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::fmt::{self, Display};
use std::fs::File;
use std::io::{stdout, BufReader, BufWriter, Read, Write};
use std::sync::Arc;
use rayon::{ThreadPoolBuilder, Scope};
use crossbeam::queue::SegQueue;
use hashbrown::HashMap;

#[derive(Debug)]
struct Data {
    sum: i32,
    count: u32,
    min: i32,
    max: i32,
}
impl Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{:.1}/{}",
            self.min as f64 / 10.0,
            self.sum as f64 / self.count as f64 / 10.0,
            self.max as f64 / 10.0,
        )
    }
}
impl Data {
    fn update(&mut self, value: i32) {
        self.sum += value;
        self.count += 1;
        if value < self.min {
            self.min = value;
        } else if value > self.max {
            self.max = value;
        }
    }
    fn union(&mut self, other: &Data) {
        // Assumes that the stations are the same
        self.sum += other.sum;
        self.count += other.count;
        self.min = if self.min < other.min { self.min } else { other.min };
        self.max = if self.max > other.max { self.max } else { other.max };
    }
}

/*
Line by Line Hashmap runtime: 272s - 100%
Line by Line BTreeMap runtime: 372s - Slower as Tree lookup is slower than HashMap
Line by Line HashMap + BTreeSet stations runtime: 469s - Slower as sorting at the end is faster
Line by Line FxHashMap runtime: 253s - Faster as it uses a faster hashing algorithm - 93%
Buffer with 100 lines FxHashMap runtime: 169s - Faster as it reduces reads to disk
Buffer with 50 lines FxHashMap runtime: 169s
Buffer with 25 lines FxHashMap runtime: 168s - 61%
Specify general edits to the Cargo.toml file runtime: 150s - 55% - Settings allow for beter optimisation
Switch to using mimalloc runtime: 85s - 31% - Improved memory allocation
Optimised string splitting runtime: 80s - 29% - Faster as it is a more specific split
Increased buffer size runtime: 76s - 28% - Reduces the number of reads to disk
Switched to AHashMap runtime: 76s - 28%
Switched count to be u32 runtime: 76s - 28%
Changed to line by line reading runtime: 69s - 25% - Faster as it reduces memory allocations
Attempted using a GPerf hash function dramatic runtime slowdown; reverted
Added multiple threads for data processing runtime: 40s - 15% - Faster as the work is spread across the CPU
Switched to using a rayon pool thread collecting results runtime: 40s - 15% - More efficient thread usage
Changed to using a SegQueue runtime: 40s - 15% - Reduces lock contention by using an atomic queue
Adjusted constants to be more reflective of the actual data rather than the worst case runtime: 39s - 14%
Read from the buffer in chunks of data runtime: 12s - 4% - Reduces the number of reads to disk
    ** This had a large impact on memory usage, peaking around 10GB rather than the previous 1GB

Profiler Optimisations:
Small changes to the threads data - 10.96s - 18.1% improvement
Changed to using a buffer that flushes to stdout - 10.82s - 1.3% improvement
Changed Hashmap implementation - 10.44s - 2.4% improvement
Changed to fast float parsing - 10.32s - 1.1% improvement
Switched to custom lines splitting - 8.50s - 17.7% improvement
    ** Uses the fact that the line delimiter will always be within the last 6 characters
    ** More optimal to search from the right side for the delimiter
Changed from f64 to i32 to store values using a custom parser - 8.04s - 5.4% improvement
    ** Still has some optimisation potential in the parser
*/

// Data Constants
const AVERAGE_STATION_LENGTH: usize = 10;
const MAX_STATION_LENGTH: usize = 100;

const LINE_DELIMITER: char = ';';
const MAX_LINE_LENGTH: usize = MAX_STATION_LENGTH + 7; // Line formatting: (name: 100);(-)dd.d\n
const AVERAGE_LINE_LENGTH: usize = AVERAGE_STATION_LENGTH + 6;
const MAX_UNIQUE_STATIONS: usize = 10_000;
const BATCH_SIZE: usize = 1_000_000;

fn split_line(line: &str) -> Option<(&str, &str)> {
    let delimiter = line.rfind(LINE_DELIMITER)?;
    Some((&line[..delimiter], &line[delimiter + 1..]))
}

fn parse_i32(value: &str) -> i32 {
    let characters = value.chars().rev();
    let mut result = 0;
    let mut place_value = 1;
    if value.as_bytes()[0] == b'-' {
        for character in characters.take(value.len() - 1) {
            if character == '.' { continue; }
            let digit = character.to_digit(10).unwrap() as i32;
            result -= digit * place_value;
            place_value *= 10;
        }
    } else {
        for character in characters {
            if character == '.' { continue; }
            let digit = character.to_digit(10).unwrap() as i32;
            result += digit * place_value;
            place_value *= 10;
        }
    }
    result
}

fn process_batch(mut batch: String) -> HashMap<String, Data> {
    // Batch has multiple lines contained within it
    let _ = batch.pop(); // Remove the last newline
    let lines = batch.split('\n');

    const LOCAL_CAPACITY: usize = if BATCH_SIZE > MAX_UNIQUE_STATIONS { MAX_UNIQUE_STATIONS } else { BATCH_SIZE };
    let mut local_map = HashMap::<String, Data>::with_capacity(LOCAL_CAPACITY);
    for line in lines {
        let (station, value_str) = match split_line(line) {
            Some((station, value_str)) => (station, value_str),
            None => unreachable!("Invalid line"),
        };
        let value = parse_i32(value_str);
        local_map.entry(station.to_string())
            .and_modify(|data| data.update(value))
            .or_insert_with(|| Data { sum: value, count: 1, min: value, max: value });
    }

    local_map
}

pub fn process_file(address: &str) {
    let max_threads: usize = num_cpus::get();
    let processing_threads = max_threads;

    let pool = ThreadPoolBuilder::new()
        .num_threads(processing_threads)
        .build()
        .unwrap();

    let results = Arc::new(SegQueue::new());
    let mut master_map = HashMap::<String, Data>::with_capacity(MAX_UNIQUE_STATIONS);
    let file = File::open(address).expect("File not found");
    let mut reader = BufReader::with_capacity((MAX_LINE_LENGTH + 1) * BATCH_SIZE, file);
    let mut batch = Vec::with_capacity(BATCH_SIZE * (MAX_LINE_LENGTH + 1));
    let mut remainder = Vec::with_capacity(MAX_LINE_LENGTH + 1);
    pool.scope(|s: &Scope| {
        loop {
            batch.clear();
            batch.extend_from_slice(&remainder);
            remainder.clear();
            let bytes_read = reader.by_ref().take((BATCH_SIZE * (AVERAGE_LINE_LENGTH + 1)) as u64).read_to_end(&mut batch).unwrap();
            if bytes_read == 0 { // EOF reached
                break;
            }
            if let Some(last_newline) = batch.iter().rposition(|&b| b == b'\n') {
                remainder = batch.split_off(last_newline + 1);
            }
            if !remainder.is_empty() && remainder[0] & 0b1100_0000 == 0b1000_0000 {
                let mut char_start = remainder.len();
                while char_start > 0 && remainder[char_start - 1] & 0b1100_0000 == 0b1000_0000 {
                    char_start -= 1;
                }
                let incomplete_char = remainder.split_off(char_start);
                batch.extend(incomplete_char);
            }
            let cloned_results = Arc::clone(&results);
            s.spawn(move |_| unsafe {
                let batch_str = String::from_utf8_unchecked(batch);
                let result = process_batch(batch_str);
                cloned_results.push(result);
            });
            batch = Vec::with_capacity(BATCH_SIZE * (MAX_LINE_LENGTH + 1));
        }
    });
    let results = Arc::try_unwrap(results).expect("Arc still has multiple owners");
    for local_map in results {
        for (station, data) in local_map {
            master_map.entry(station)
            .and_modify(|master_data| master_data.union(&data))
            .or_insert(data);
        }
    }
    let mut stations = master_map.keys().collect::<Vec<_>>();
    stations.sort_unstable();

    let writer_capacity: usize = stations.len() * (AVERAGE_STATION_LENGTH + 21);

    let num_stations = stations.len();
    let mut stations_iter = stations.into_iter();
    let mut stdout = BufWriter::with_capacity(writer_capacity, stdout());
    write!(stdout, "{{").unwrap();
    for station in stations_iter.by_ref().take(num_stations - 1) {
        write!(stdout, "{}={}, ", station, master_map[station]).unwrap();
    }
    if let Some(station) = stations_iter.next() {
        write!(stdout, "{}={}", station, master_map[station]).unwrap();
    }
    writeln!(stdout, "}}").unwrap();
    stdout.flush().unwrap();
}

#[cfg(test)]
mod tests {

    use super::parse_i32;

    #[test]
    fn test_parse_i32() {
        assert_eq!(parse_i32("-12.3"), -123);
        assert_eq!(parse_i32("12.3"), 123);
        assert_eq!(parse_i32("-1.3"), -13);
        assert_eq!(parse_i32("2.3"), 23);
        assert_eq!(parse_i32("-0.3"), -3);
        assert_eq!(parse_i32("0.3"), 3);
    }

}