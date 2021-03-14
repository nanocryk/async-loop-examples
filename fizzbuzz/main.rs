use std::future::Future;

use futures::{channel::mpsc, join, select, SinkExt, StreamExt};
use tokio::time::{sleep_until, Duration, Instant};
use tracing::{instrument, Instrument};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[tokio::main]
#[instrument]
async fn main() {
    setup_tracing();

    tracing::trace!("Start setup ...");

    const MS_BETWEEN_PULSES: u64 = 200;
    const STEPS: u32 = 50;
    let (pulse1_fut, mut pulse1_rx) = pulse(Duration::from_millis(1 * MS_BETWEEN_PULSES));
    let (pulse3_fut, mut pulse3_rx) = pulse(Duration::from_millis(3 * MS_BETWEEN_PULSES));
    let (pulse5_fut, mut pulse5_rx) = pulse(Duration::from_millis(5 * MS_BETWEEN_PULSES));

    // We spawn the pulse3 and pulse5 a bit before pulse1 so we're sure their
    // pulse will arrive between to pulses of pulse1.
    let pulse3_handle = tokio::spawn(pulse3_fut);
    let pulse5_handle = tokio::spawn(pulse5_fut);
    // sleep(Duration::from_millis(MS_BETWEEN_PULSES / 2)).await;
    let pulse1_handle = tokio::spawn(pulse1_fut);

    tracing::trace!("Setup done, let's FizzBuzz !");

    // This is some mutable state. We don't need any locks.
    let (mut fizz, mut buzz) = (false, false);
    let mut step = 0;

    loop {
        select! {
            _ = pulse1_rx.next() => {
                step += 1;
                match (fizz, buzz) {
                    (false, false) => tracing::info!("{}", step),
                    (true, false) => tracing::info!("Fizz"),
                    (false, true) => tracing::info!("Buzz"),
                    (true, true) => tracing::info!("FizzBuzz"),
                }

                fizz = false;
                buzz = false;

                if step >= STEPS { break }
            },
            _ = pulse3_rx.next() => fizz = true,
            _ = pulse5_rx.next() => buzz = true,
        }
    }

    tracing::trace!("FizzBuzz accomplished, let's cleanup now !");

    // Lets stop everything cleanly (not necessary).
    // First we drop the Receivers.
    drop((pulse1_rx, pulse3_rx, pulse5_rx));

    // Then we wait for the 3 tokio handles to finish.
    let _ = join!(pulse1_handle, pulse3_handle, pulse5_handle);

    tracing::trace!("Bye bye !");
}

/// Create a "pulse" task that will emit a pulse every `delay`.
/// Returns the task to be awaited, and the receiver.
/// Dropping the Receiver will stop the task when the next pulse occurs.
/// Will start its timer when first awaited.
fn pulse(delay: Duration) -> (impl Future<Output = ()>, mpsc::Receiver<()>) {
    let (mut tx, rx) = mpsc::channel(128);

    let task = async move {
        let mut instant = Instant::now();
        loop {
            instant += delay;
            // We use `sleep_until` and not `sleep` to avoid time drifting,
            // since both sleep functions have ms granularity.
            tracing::trace!("Sleeping ...");
            sleep_until(instant).await;
            // We send the pulse. Failing means Receiver has been dropped,
            // so we stop the task that is now useless.
            tracing::trace!("Awake, sending pulse ...");
            if tx.send(()).await.is_err() {
                break;
            }
        }
        tracing::trace!("Receiver dropped, stopping task !");
    }
    .instrument(tracing::debug_span!("pulse", ?delay));

    (task, rx)
}

fn setup_tracing() {
    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // RUST_LOG env
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
