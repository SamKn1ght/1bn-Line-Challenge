// use std::collections::HashMap;
use std::fmt::{self, Display};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use rustc_hash::FxHashMap;

struct Data {
    sum: f64,
    count: f64,
    min: f64,
    max: f64,
}
impl Display for Data {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{:.1}/{}", self.min, self.sum/self.count, self.max)
    }
}
impl Data {
    fn update(&mut self, value: f64) {
        self.sum += value;
        self.count += 1.0;
        if value < self.min {
            self.min = value;
        } else if value > self.max {
            self.max = value;
        }
    }
}

// Line by Line Hashmap runtime: 272s
// Line by Line BTreeMap runtime: 372s - Slower as Tree lookup is slower than HashMap
// Line by Line HashMap + BTreeSet stations runtime: 469s - Slower as sorting at the end is faster
// Line by Line FxHashMap runtime: 253s - Faster as it uses a faster hashing algorithm

fn main() {
    const ADDRESS: &str = "../measurements.txt";
    const LINE_DELIMITER: &str = ";";
    const MAX_LINE_LENGTH: usize = 106; // Line formatting: (name: 100);(-)dd.d
    const MAX_UNIQUE_STATIONS: usize = 10_000;

    let start = Instant::now();

    let mut map = FxHashMap::<String, Data>::with_capacity_and_hasher(MAX_UNIQUE_STATIONS, Default::default());

    let file = File::open(ADDRESS).expect("File not found");
    let reader = BufReader::with_capacity(MAX_LINE_LENGTH, file);
    println!("Station: Min/Mean/Max");
    let start_read = Instant::now();
    for line in reader.lines().map_while(Result::ok) {
        let mut line_data = line.split(LINE_DELIMITER);
        let station = line_data.next().expect("Invalid station").to_string();
        let value = line_data.next().expect("No value found").parse::<f64>().expect("Invalid value");
        map.entry(station)
            .and_modify(|data| data.update(value))
            .or_insert_with(|| Data { sum: value, count: 1.0, min: value, max: value });
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
