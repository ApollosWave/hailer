use std::num::NonZeroU64;
use std::time::Duration;

use sqlx::{PgPool, Row};
use throttle_machines::{Gate, TokenBucket, TokenBucketPolicy};

use crate::config::RateLimitConfig;

/// Backed by a single shared `hailer.send_rate_limiter` row, serialized with
/// `FOR UPDATE`, so every replica draws down the same budget instead of each
/// enforcing its own independent, process-local one.
pub(super) struct Limiter {
    pool: PgPool,
}

impl Limiter {
    pub(super) async fn new(pool: PgPool, config: &RateLimitConfig) -> anyhow::Result<Self> {
        sqlx::query(
            "INSERT INTO hailer.send_rate_limiter (id, tokens, last_refill, capacity, refill_rate) \
             VALUES (1, $1, extract(epoch from clock_timestamp())::double precision, $1, $2) \
             ON CONFLICT (id) DO NOTHING",
        )
        .bind(f64::from(config.burst))
        .bind(config.recipients_per_second)
        .execute(&pool)
        .await?;
        Ok(Self { pool })
    }

    /// Reserves the entire envelope or consumes no tokens at all. State only
    /// commits on full admission, matching `TokenBucket::check`'s own
    /// behavior on denial (a refused call changes nothing).
    pub(super) async fn admit(&self, recipients: usize) -> anyhow::Result<bool> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            "SELECT tokens, last_refill, capacity, refill_rate, \
             extract(epoch from clock_timestamp())::double precision AS now \
             FROM hailer.send_rate_limiter WHERE id = 1 FOR UPDATE",
        )
        .fetch_one(&mut *tx)
        .await?;
        let now = Duration::try_from_secs_f64(row.try_get("now")?)?;
        let bucket = bucket(
            row.try_get("capacity")?,
            row.try_get("refill_rate")?,
            row.try_get("tokens")?,
            row.try_get("last_refill")?,
        )?;
        let admitted = bucket.and_then(|mut bucket| {
            (0..recipients).all(|_| bucket.check(now).is_ok()).then_some(bucket)
        });
        if let Some(bucket) = admitted {
            sqlx::query(
                "UPDATE hailer.send_rate_limiter SET tokens = $1, last_refill = $2 WHERE id = 1",
            )
            .bind(count(bucket.tokens()))
            .bind(bucket.refilled_at().as_secs_f64())
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(admitted.is_some())
    }

    /// Called by the AWS-discovery reconcile loop. Capacity tracks the
    /// discovered rate 1:1 (the burst allowance stops being an independent
    /// operator knob once AWS owns the number), never mutating `tokens`
    /// directly: a rate drop takes effect at the next refill, not instantly.
    pub(super) async fn set_rate(&self, recipients_per_second: f64) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE hailer.send_rate_limiter \
             SET refill_rate = $1, capacity = $1 WHERE id = 1",
        )
        .bind(recipients_per_second)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

/// The stored row as a bucket on the database's clock, in seconds since the
/// Unix epoch; `None` for a capacity below one token, which admits nothing.
/// A stored fraction of a token becomes the time already earned toward the
/// next one.
fn bucket(
    capacity: f64,
    refill_rate: f64,
    tokens: f64,
    last_refill: f64,
) -> anyhow::Result<Option<TokenBucket>> {
    let Some(capacity) = NonZeroU64::new(whole(capacity)) else {
        return Ok(None);
    };
    let refill_every = Duration::try_from_secs_f64(refill_rate.recip())?;
    let policy = TokenBucketPolicy::new(capacity, refill_every).ok_or_else(|| {
        anyhow::anyhow!("refill rate {refill_rate} is above one token a nanosecond")
    })?;
    let held = whole(tokens).min(capacity.get());
    let earned = Duration::try_from_secs_f64(tokens.max(0.0).fract() / refill_rate)?;
    let refilled_at = Duration::try_from_secs_f64(last_refill)?.saturating_sub(earned);
    Ok(TokenBucket::restored(policy, held, refilled_at))
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "a float cast saturates: negative and NaN are zero tokens"
)]
const fn whole(tokens: f64) -> u64 {
    tokens as u64
}

#[expect(
    clippy::cast_precision_loss,
    reason = "at most the capacity, which the row holds as a double"
)]
const fn count(tokens: u64) -> f64 {
    tokens as f64
}
