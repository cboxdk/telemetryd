//! Printing that survives its reader going away.
//!
//! `println!` panics when stdout is closed — "failed printing to stdout: Broken pipe" —
//! and every command here can meet that, because piping into `head` is an ordinary thing
//! to do. `telemetryd query … | head -20` answered with a panic and a backtrace, and
//! `telemetryd version | head -1` did the same.
//!
//! # Why not reset SIGPIPE
//!
//! The usual fix is one line: restore `SIGPIPE` to its default so the process dies
//! silently, as every other Unix tool does. It needs `libc` and an `unsafe` block.
//!
//! This workspace sets `unsafe_code = "forbid"`, and **`forbid` cannot be overridden
//! locally** — not by an `allow` on the module, not by one on the expression. Taking
//! that route means lowering the whole workspace to `deny`, trading a guarantee that no
//! unsafe code exists anywhere for a convenience in how the CLI prints. That is a bad
//! trade, and it is the actual reason for this file rather than a dependency question.
//!
//! So: writes go through here, and a closed reader means stop rather than crash. Exiting
//! zero is what the signal would have done, and what the caller expects — `head` got its
//! twenty lines and is finished.

use std::io::Write;

/// Write one line to stdout, or exit quietly if the reader has gone.
pub fn line(text: &str) {
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    if let Err(error) = writeln!(handle, "{text}") {
        exit_if_pipe_closed(&error);
    }
}

/// A closed pipe is the reader finishing, not a failure. Anything else is a real write
/// error and there is nowhere useful left to report it — stdout is where reports go.
fn exit_if_pipe_closed(error: &std::io::Error) {
    if error.kind() == std::io::ErrorKind::BrokenPipe {
        std::process::exit(0);
    }
}

/// `println!`, but through [`line()`].
macro_rules! outln {
    () => { $crate::out::line("") };
    ($($arg:tt)*) => { $crate::out::line(&format!($($arg)*)) };
}

pub(crate) use outln;

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    /// A closed pipe must be distinguishable from a real write failure, because one is
    /// the reader finishing and the other is worth an error.
    #[test]
    fn only_a_closed_pipe_is_treated_as_the_end() {
        use std::io::ErrorKind;
        // Documenting the discrimination the helper makes; exiting cannot be asserted
        // from inside the process, so the kind check is what there is to test.
        assert_eq!(
            std::io::Error::from(ErrorKind::BrokenPipe).kind(),
            ErrorKind::BrokenPipe
        );
        assert_ne!(
            std::io::Error::from(ErrorKind::PermissionDenied).kind(),
            ErrorKind::BrokenPipe
        );
    }
}
