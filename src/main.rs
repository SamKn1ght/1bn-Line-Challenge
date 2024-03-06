use rust_billion_row_challenge::process_file;

fn main() {
    let address = std::env::var("MEASUREMENTS_FILE").expect("No file specified");
    process_file(&address);
}