//! Deterministic clock.
use std::time::Instant;

/// Read-only logical time source.
pub trait Clock {
    /// Returns the current logical tick without advancing time.
    fn now(&self) -> u64;
}

/// Simulation clock with a fixed logical time step.
///
/// Time is single-threaded and driver-owned: only the simulator advances it.
#[derive(Debug, Default)]
pub struct SimClock {
    tick: u64,
}

impl SimClock {
    /// Creates a clock at logical tick zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Advances logical time by `ticks`.
    pub fn advance(&mut self, ticks: u64) {
        self.tick = match self.tick.checked_add(ticks) {
            Some(tick) => tick,
            None => panic!("simulation clock tick counter overflow"),
        };
    }
}

impl Clock for SimClock {
    fn now(&self) -> u64 {
        self.tick
    }
}

/// Production wall-clock adapter, outside the deterministic simulator.
///
/// A production driver measures elapsed monotonic time and translates it into
/// logical Raft ticks.
#[derive(Debug, Default)]
pub struct SystemClock;

impl SystemClock {
    /// Returns the current monotonic wall-clock instant.
    pub fn now_instant(&self) -> Instant {
        Instant::now()
    }
}

#[cfg(test)]
mod tests {
    use super::{Clock, SimClock};

    #[test]
    fn new_clock_starts_at_tick_zero() {
        let clock = SimClock::new();

        assert_eq!(clock.now(), 0);
    }

    #[test]
    fn advancing_zero_ticks_does_not_change_time() {
        let mut clock = SimClock::new();

        clock.advance(0);

        assert_eq!(clock.now(), 0);
    }

    #[test]
    fn advances_are_monotonic_and_additive() {
        let mut clock = SimClock::new();

        clock.advance(1);
        assert_eq!(clock.now(), 1);

        clock.advance(41);
        assert_eq!(clock.now(), 42);
    }
}
