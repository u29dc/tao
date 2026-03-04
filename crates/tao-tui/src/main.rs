use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "tao-tui", version, about = "tao terminal ui placeholder shell")]
struct CliArgs {
    /// Print startup status and exit.
    #[arg(long, default_value_t = false)]
    status: bool,
}

fn main() {
    let args = CliArgs::parse();
    if args.status {
        println!("tao-tui status=placeholder");
        return;
    }

    println!("tao-tui placeholder shell");
    println!("tui routes are intentionally disabled for now");
}

#[cfg(test)]
mod tests {
    use clap::CommandFactory;

    use super::CliArgs;

    #[test]
    fn cli_help_mentions_placeholder_shell() {
        let mut command = CliArgs::command();
        let mut output = Vec::new();
        command
            .write_long_help(&mut output)
            .expect("render long help");
        let rendered = String::from_utf8(output).expect("utf8 help");
        assert!(rendered.contains("placeholder shell"));
    }
}
