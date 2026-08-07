use std::sync::atomic::{AtomicBool, AtomicU8};

pub const MAX_CONSECUTIVE_FAILURES_FOR_A_BACKEND: u8 = 3;

pub struct Backend {
    pub addr: String,
    pub healthy: AtomicBool,
    pub consecutive_failures: AtomicU8,
}

impl Backend {
    pub fn new<T: Into<String>>(addr: T) -> Backend {
        Backend {
            addr: addr.into(),
            healthy: AtomicBool::new(true),
            consecutive_failures: AtomicU8::new(0),
        }
    }
}
