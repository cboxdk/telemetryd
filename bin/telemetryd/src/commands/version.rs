//! `telemetryd version`

pub fn run() {
    crate::out::outln!("telemetryd {}", telemetryd_core::VERSION);
    crate::out::outln!(
        "storage format v{}",
        telemetryd_core::STORAGE_FORMAT_VERSION
    );
    crate::out::outln!("milestone {}", telemetryd_server::MILESTONE);
    crate::out::outln!("target {}", env!("TELEMETRYD_TARGET"));
    crate::out::outln!("compatibility {}", telemetryd_core::COMPATIBILITY_DOC);
}
