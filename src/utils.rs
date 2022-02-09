use crate::base_node::NodeType;

pub(crate) fn convert_type_to_version(n_type: NodeType) -> usize {
    (n_type as usize) << 62
}

use core::cell::Cell;
use core::fmt;

const SPIN_LIMIT: u32 = 6;
const YIELD_LIMIT: u32 = 10;

/// Backoff implementation from the Crossbeam, added shuttle instrumentation
pub(crate) struct Backoff {
    step: Cell<u32>,
}

impl Backoff {
    #[inline]
    pub(crate) fn new() -> Self {
        Backoff { step: Cell::new(0) }
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn reset(&self) {
        self.step.set(0);
    }

    #[inline]
    pub(crate) fn spin(&self) {
        for _ in 0..1 << self.step.get().min(SPIN_LIMIT) {
            std::hint::spin_loop();
        }

        if self.step.get() <= SPIN_LIMIT {
            self.step.set(self.step.get() + 1);
        }

        #[cfg(shuttle)]
        shuttle::thread::yield_now();
    }

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn snooze(&self) {
        if self.step.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.step.get() {
                std::hint::spin_loop();
            }
        } else {
            #[cfg(shuttle)]
            shuttle::thread::yield_now();

            #[cfg(not(shuttle))]
            ::std::thread::yield_now();
        }

        if self.step.get() <= YIELD_LIMIT {
            self.step.set(self.step.get() + 1);
        }
    }

    #[inline]
    pub(crate) fn is_completed(&self) -> bool {
        self.step.get() > YIELD_LIMIT
    }
}

impl fmt::Debug for Backoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Backoff")
            .field("step", &self.step)
            .field("is_completed", &self.is_completed())
            .finish()
    }
}

impl Default for Backoff {
    fn default() -> Backoff {
        Backoff::new()
    }
}
