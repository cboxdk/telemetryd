//! `telemetryd version`

pub fn run() {
    println!("telemetryd {}", telemetryd_core::VERSION);
    println!(
        "storage format v{}",
        telemetryd_core::STORAGE_FORMAT_VERSION
    );
    println!("milestone {}", telemetryd_server::MILESTONE);
    println!("target {}", env!("TELEMETRYD_TARGET"));
    println!("compatibility {}", telemetryd_core::COMPATIBILITY_DOC);
}
