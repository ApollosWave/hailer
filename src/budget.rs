//! Request budgets on one monotonic clock, so every deadline compares.

#[cfg(test)]
mod tests;

use std::sync::LazyLock;
use std::time::{Duration, Instant};

use deadline_machines::Deadline;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Monotonic time since the process started: what every deadline is on.
pub fn now() -> Duration {
    EPOCH.elapsed()
}

pub fn after(timeout: Duration) -> Deadline {
    Deadline::after(now(), timeout)
}

/// `deadline` brought forward by `reserve`, so work under it leaves `reserve`
/// to whatever follows.
pub fn leaving(deadline: Deadline, reserve: Duration) -> Deadline {
    deadline.expires_at().map_or(deadline, |at| Deadline::at(at.saturating_sub(reserve)))
}

/// The wait `deadline` still allows, at most `cap`; `None` below `floor`.
pub fn within(deadline: Deadline, floor: Duration, cap: Duration) -> Option<Duration> {
    let now = now();
    let wait = deadline.clamp_sleep(now, cap);
    (wait >= floor && !deadline.is_expired(now)).then_some(wait)
}
