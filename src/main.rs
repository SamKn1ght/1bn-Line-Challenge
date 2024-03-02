#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::fmt::{self, Display};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use ahash::AHashMap;

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
}

// Line by Line Hashmap runtime: 272s - 100%
// Line by Line BTreeMap runtime: 372s - Slower as Tree lookup is slower than HashMap
// Line by Line HashMap + BTreeSet stations runtime: 469s - Slower as sorting at the end is faster
// Line by Line FxHashMap runtime: 253s - Faster as it uses a faster hashing algorithm - 93%
// Buffer with 100 lines FxHashMap runtime: 169s - Faster as it readuces read
// Buffer with 50 lines FxHashMap runtime: 169s
// Buffer with 25 lines FxHashMap runtime: 168s - 61%
// Specify general edits to the Cargo.toml file runtime: 150s - 55%
// Switch to using mimalloc runtime: 85s - 31%
// Optimised string splitting runtime: 80s - 29%
// Increased buffer size runtime: 76s - 28%
// Switched to AHashMap runtime: 76s - 28%
// Switched count to be u32 runtime: 76s - 28%
// Changed to line by line reading runtime: 69s - 25%

fn main() {
    const ADDRESS: &str = "../measurements.txt";
    const LINE_DELIMITER: &str = ";";
    const MAX_LINE_LENGTH: usize = 107; // Line formatting: (name: 100);(-)dd.d\n
    const MAX_UNIQUE_STATIONS: usize = 10_000;

    let start = Instant::now();

    let mut map = AHashMap::<String, Data>::with_capacity(MAX_UNIQUE_STATIONS);

    let file = File::open(ADDRESS).expect("File not found");
    let mut reader = BufReader::with_capacity(MAX_LINE_LENGTH * 1_000, file);
    println!("Station: Min/Mean/Max");
    let start_read = Instant::now();
    let mut line = String::with_capacity(MAX_LINE_LENGTH);
    while let Ok(bytes_read) = reader.read_line(&mut line) {
        if bytes_read == 0 { break; } // EOF
        line.truncate(bytes_read - 1); // Remove '\n'
        let (station, value_str) = match line.split_once(LINE_DELIMITER) {
            Some((station, value_str)) => (station, value_str),
            None => unreachable!("Invalid line"),
        };
        let value = match value_str.parse::<f64>() {
            Ok(value) => value,
            Err(_) => unreachable!("Invalid value"),
        };
        map.entry(station.to_string())
            .and_modify(|data| data.update(value))
            .or_insert_with(|| Data { sum: value, count: 1, min: value, max: value });
        line.clear();
    }
    let end_read = Instant::now();
    println!("Sorting Stations");
    let mut stations = map.keys().collect::<Vec<_>>();
    stations.sort_unstable();
    for station in stations {
        println!("{}: {}", station, map[station]);
    }
    let end = Instant::now();
    println!("Reading: {:#?}", end_read.duration_since(start_read));
    println!("Total: {:#?}", end.duration_since(start));
}
