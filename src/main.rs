#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::fmt::{self, Display};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Instant;
use ahash::AHashMap;
use rayon::{ThreadPoolBuilder, Scope};
use crossbeam::queue::SegQueue;

#[derive(Debug)]
struct Data {
    sum: f64,
    count: u32,
    min: f64,
    max: f64,
}
impl Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{:.1}/{}", self.min, self.sum / self.count as f64, self.max)
    }
}
impl Data {
    fn update(&mut self, value: f64) {
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
*/

const ADDRESS: &str = "../measurements.txt";
const LINE_DELIMITER: &str = ";";
const MAX_LINE_LENGTH: usize = 107; // Line formatting: (name: 100);(-)dd.d\n
const MAX_UNIQUE_STATIONS: usize = 10_000;
const BATCH_SIZE: usize = 1_000_000;

fn process_batch(mut batch: String) -> AHashMap<String, Data> {
    // Batch has multiple lines contained within it
    let _ = batch.pop(); // Remove the last newline
    let lines = batch.split('\n').collect::<Vec<_>>();

    const LOCAL_CAPACITY: usize = if BATCH_SIZE > MAX_UNIQUE_STATIONS { MAX_UNIQUE_STATIONS } else { BATCH_SIZE };
    let mut local_map = AHashMap::<String, Data>::with_capacity(LOCAL_CAPACITY);
    for line in lines {
        let (station, value_str) = match line.split_once(LINE_DELIMITER) {
            Some((station, value_str)) => (station, value_str),
            None => unreachable!("Invalid line"),
        };
        let value = match value_str.parse::<f64>() {
            Ok(value) => value,
            Err(_) => unreachable!("Invalid value"),
        };
        local_map.entry(station.to_string())
            .and_modify(|data| data.update(value))
            .or_insert_with(|| Data { sum: value, count: 1, min: value, max: value });
    }

    local_map
}

fn main() {
    let max_threads: usize = num_cpus::get();
    let processing_threads = max_threads;
    println!("Threads: {}", processing_threads);

    let start = Instant::now();

    let pool = ThreadPoolBuilder::new()
        .num_threads(processing_threads)
        .build()
        .unwrap();

    let results = Arc::new(SegQueue::new());
    let mut master_map = AHashMap::<String, Data>::with_capacity(MAX_UNIQUE_STATIONS);

    let file = File::open(ADDRESS).expect("File not found");
    let mut reader = BufReader::with_capacity(MAX_LINE_LENGTH * BATCH_SIZE, file);
    println!("Station: Min/Mean/Max");
    let start_read = Instant::now();
    let mut batch = String::with_capacity(BATCH_SIZE * (MAX_LINE_LENGTH + 1));
    pool.scope(|s: &Scope| {
        while let Ok(bytes_read) = reader.read_line(&mut batch) {
            if bytes_read == 0 { // Dispatch final thread
                let cloned_results = Arc::clone(&results);
                s.spawn(move |_| {
                    let result = process_batch(batch);
                    cloned_results.push(result);
                });
                break;
            } // EOF
            if batch.len() > BATCH_SIZE * (MAX_LINE_LENGTH + 1) {
                let cloned_results = Arc::clone(&results);
                s.spawn(move |_| {
                    let result = process_batch(batch);
                    cloned_results.push(result);
                });
                batch = String::with_capacity(BATCH_SIZE * (MAX_LINE_LENGTH + 1));
            }
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
    let end_read = Instant::now();
    println!("Sorting Stations");
    let mut stations = master_map.keys().collect::<Vec<_>>();
    stations.sort_unstable();
    for station in stations {
        println!("{}: {}", station, master_map[station]);
    }
    let end = Instant::now();
    println!("Reading: {:#?}", end_read.duration_since(start_read));
    println!("Total: {:#?}", end.duration_since(start));
}
