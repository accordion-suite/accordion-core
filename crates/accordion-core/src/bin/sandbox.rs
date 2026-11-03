//! The sandbox launcher on its own, for the thumbnail and text tests. On a
//! server, the server's own program is the launcher. It also reads the text
//! of Office files, as the server's program does, for the text tests.

fn main() {
    let mut args = std::env::args_os().skip(1);
    if args
        .next()
        .is_some_and(|command| command == accordion_core::texts::office::COMMAND)
    {
        std::process::exit(accordion_core::texts::office::main(
            args,
            accordion_core::texts::MAX_TEXT,
        ));
    }
    accordion_sandbox::main();
}
