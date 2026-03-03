use clap::{Parser, ValueEnum};
use std::time::Instant;

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Scenario {
    Parse,
    Resolve,
    Search,
}

#[derive(Parser, Debug)]
#[command(name = "obs-bench")]
#[command(about = "benchmark scaffold for obs workloads")]
struct Args {
    #[arg(long, value_enum, default_value_t = Scenario::Parse)]
    scenario: Scenario,
    #[arg(long, default_value_t = 100_000)]
    iterations: u64,
}

fn main() {
    let args = Args::parse();
    let start = Instant::now();

    for i in 0..args.iterations {
        std::hint::black_box(i.wrapping_mul(31).wrapping_add(7));
    }

    let elapsed = start.elapsed();
    println!(
        "scenario={:?} iterations={} elapsed_ms={}",
        args.scenario,
        args.iterations,
        elapsed.as_millis()
    );
}
