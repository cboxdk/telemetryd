//! Records the build target so `telemetryd version` can report which of the four
//! release artifacts is running — the first question in any "it works on my machine"
//! report about a static binary.

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    let target = std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned());
    println!("cargo:rustc-env=TELEMETRYD_TARGET={target}");
}
