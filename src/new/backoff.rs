use rand::{Rng, RngCore};
use std::ops::ControlFlow;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub init_backoff: Duration,
    pub max_backoff: Duration,
    pub base: f64,
    pub deadline: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(500),
            base: 3.,
            deadline: None,
        }
    }
}

type SourceError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[allow(missing_copy_implementations)]
pub enum BackoffError {
    #[error("Retry exceeded deadline. Source: {source}")]
    DeadlineExceded {
        deadline: Duration,
        source: SourceError,
    },
}
pub type BackoffResult<T> = Result<T, BackoffError>;

/// Error (which should increase backoff) or throttle for a specific duration (as asked for by the broker).
#[derive(Debug)]
pub enum ErrorOrThrottle<E>
where
    E: Send,
{
    Error(E),
    Throttle(Duration),
}

pub struct Backoff {
    init_backoff: f64,
    next_backoff_secs: f64,
    max_backoff_secs: f64,
    base: f64,
    total: f64,
    deadline: Option<f64>,
    rng: Option<Box<dyn RngCore + Sync + Send>>,
}

impl std::fmt::Debug for Backoff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backoff")
            .field("init_backoff", &self.init_backoff)
            .field("next_backoff_secs", &self.next_backoff_secs)
            .field("max_backoff_secs", &self.max_backoff_secs)
            .field("base", &self.base)
            .finish()
    }
}

impl Backoff {
    pub fn new(config: &BackoffConfig) -> Self {
        Self::new_with_rng(config, None)
    }

    pub fn new_with_rng(
        config: &BackoffConfig,
        rng: Option<Box<dyn RngCore + Sync + Send>>,
    ) -> Self {
        let init_backoff = config.init_backoff.as_secs_f64();
        Self {
            init_backoff,
            next_backoff_secs: init_backoff,
            max_backoff_secs: config.max_backoff.as_secs_f64(),
            base: config.base,
            rng,
            total: 0.,
            deadline: config.deadline.map(|d| d.as_secs_f64()),
        }
    }

    pub async fn retry_with_backoff<F, F1, B, E>(
        &mut self,
        request_name: &str,
        do_stuff: F,
    ) -> BackoffResult<B>
    where
        F: (Fn() -> F1) + Send + Sync,
        F1: std::future::Future<Output = ControlFlow<B, ErrorOrThrottle<E>>> + Send,
        E: std::error::Error + Send + Sync + 'static,
    {
        loop {
            // split match statement from `tokio::time::sleep`, because otherwise rustc requires `B: Send`
            let fail = match do_stuff().await {
                ControlFlow::Break(r) => break Ok(r),
                ControlFlow::Continue(e) => e,
            };

            let sleep_time = match fail {
                ErrorOrThrottle::Error(e) => match self.next() {
                    Some(backoff) => {
                        info!(
                            e=%e,
                            request_name,
                            backoff_secs = backoff.as_secs(),
                            "request encountered non-fatal error - backing off",
                        );
                        backoff
                    }
                    None => {
                        break Err(BackoffError::DeadlineExceded {
                            deadline: Duration::from_secs_f64(self.deadline.unwrap()),
                            source: Box::new(e),
                        });
                    }
                },
                ErrorOrThrottle::Throttle(throttle) => {
                    info!(?throttle, request_name, "broker asked us to throttle",);
                    throttle
                }
            };

            tokio::time::sleep(sleep_time).await;
        }
    }
}

impl Iterator for Backoff {
    type Item = Duration;

    /// Returns the next backoff duration to wait for
    fn next(&mut self) -> Option<Duration> {
        let range = self.init_backoff..(self.next_backoff_secs * self.base);

        let rand_backoff = match self.rng.as_mut() {
            Some(rng) => rng.random_range(range),
            None => rand::rng().random_range(range),
        };

        let next_backoff = self.max_backoff_secs.min(rand_backoff);
        self.total += next_backoff;
        let backoff =
            Duration::from_secs_f64(std::mem::replace(&mut self.next_backoff_secs, next_backoff));

        if let Some(deadline) = self.deadline {
            if self.total >= deadline {
                return None;
            }
        }
        Some(backoff)
    }
}
