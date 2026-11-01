//! The launcher on its own, for tests. The server's program answers
//! `sandbox` the same way.

fn main() {
    let mut args = std::env::args_os().skip(1);
    if args.next().is_none_or(|command| command != "sandbox") {
        eprintln!(
            "Usage: accordion-sandbox sandbox --memory <bytes> --cpu <seconds> [--read <path>]... -- <program> [<argument>]..."
        );
        std::process::exit(2);
    }
    std::process::exit(accordion_sandbox::launch(args));
}
