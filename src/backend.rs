use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

pub const MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND: u8 = 3;

pub struct Backend {
    pub addr: String,
    pub healthy: AtomicBool,
    pub consecutive_failures: AtomicU8,
    pub health_check_consecutive_successes: AtomicU8,
    pub health_check_consecutive_failures: AtomicU8,
    pub enabled: AtomicBool,
}

impl Clone for Backend {
    fn clone(&self) -> Self {
        Self {
            addr: self.addr.clone(),
            healthy: AtomicBool::new(self.healthy.load(Ordering::Relaxed)),
            consecutive_failures: AtomicU8::new(self.consecutive_failures.load(Ordering::Relaxed)),
            health_check_consecutive_successes: AtomicU8::new(
                self.health_check_consecutive_successes
                    .load(Ordering::Relaxed),
            ),
            health_check_consecutive_failures: AtomicU8::new(
                self.health_check_consecutive_failures
                    .load(Ordering::Relaxed),
            ),
            enabled: AtomicBool::new(self.enabled.load(Ordering::Relaxed)),
        }
    }
}

impl Backend {
    pub fn new<T: Into<String>>(addr: T) -> Backend {
        Backend {
            addr: addr.into(),
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU8::new(0),
            health_check_consecutive_successes: AtomicU8::new(0),
            health_check_consecutive_failures: AtomicU8::new(0),
            enabled: AtomicBool::new(true),
        }
    }

    pub fn is_enabled_and_healthy(self: &Backend) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::Relaxed)
            && self.healthy.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn enable(self: &Backend) {
        self.enabled
            .store(true, std::sync::atomic::Ordering::Relaxed)
    }

    pub fn disable(self: &Backend) {
        self.enabled
            .store(false, std::sync::atomic::Ordering::Relaxed)
    }
}
