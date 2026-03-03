use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "obs")]
#[command(about = "obs cli placeholder")]
struct Args {}

fn main() {
    let _ = Args::parse();
    println!("obs cli placeholder");
}
