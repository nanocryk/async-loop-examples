use futures::{SinkExt, StreamExt, channel::{mpsc, oneshot}, join, select, stream::FuturesUnordered};
use tokio::{
    sync::watch,
    time::{interval, sleep, Duration},
};
use tokio_stream::wrappers::{IntervalStream, WatchStream};
use tracing::instrument;

#[derive(Debug)]
enum Request {
    Echo(oneshot::Sender<u32>, u32),
}

#[derive(Debug, Clone, Copy)]
struct Config {
    request_interval: Duration,
    work_duration: Duration,
}

#[tokio::main]
async fn main() {
    // Setup the tracing crate.
    setup_tracing();

    tracing::info!("Starting !");

    // A watch channel is created. It allows to share some value from a single
    // producer to multiple consumers which can await on its change. It
    // can be used to change some config dynamically or to ask for a shutdown by
    // dropping the Sender.
    let (config_tx, config_rx) = watch::channel(Config {
        request_interval: Duration::from_millis(50),
        work_duration: Duration::from_millis(100),
    });

    // Setup the request handler.
    //
    // A MPSC channel is created to allow communication with it.
    // MPSC = Multiple Producers, Single Consumer.
    // The Sender (tx) can be cloned to be provided to multiple producers of
    // messages. The Receiver (rx) however cannot be cloned and it moved into
    // the request_handler task.
    //
    // Provided interger is a buffer size to provide backpressure. The channel
    // will be able to queue up to (100 + senders count) messages. If the
    // buffer is full, sending a value will fail.
    let (handler_tx, handler_rx) = mpsc::channel(100);
    let handler_handle = tokio::spawn(request_handler(config_rx.clone(), handler_rx));

    // Start a requester.
    let requester_handle = tokio::spawn(echo_requester(config_rx, handler_tx));

    // Let the system run for some time.
    sleep(Duration::from_millis(500)).await;

    // Change config (ignore errors)
    tracing::info!("Update config !");
    let _ = config_tx.send(Config {
        request_interval: Duration::from_millis(100),
        work_duration: Duration::from_millis(50),
    });

    // Let the system run for some time.
    sleep(Duration::from_millis(500)).await;

    tracing::info!("The end !");
    std::mem::drop(config_tx);

    // config_tx then all handler_tx will drop, stopping both loops.
    // we wait for both loops to stop. join! macro awaits for all futures.
    let _ = join!(handler_handle, requester_handle);
}

#[instrument(skip(config_rx, handler_tx))]
async fn echo_requester(config_rx: watch::Receiver<Config>, mut handler_tx: mpsc::Sender<Request>) {
    // Fetch duration from config.
    let duration = config_rx.borrow().request_interval;
    // Turn the Receiver into a Stream.
    let mut config_rx = WatchStream::new(config_rx).fuse();
    // Create a timer for this duration.
    let timer = interval(duration);
    // Turn the timer into a Stream.
    let mut timer = IntervalStream::new(timer).fuse();
    // Since a request will take some time to be handled, we cannot .await
    // directly and instead have a pool of awaiting requests.
    let mut awaiting_responses = FuturesUnordered::new();

    // Mutable state without any locks.
    let mut msg = 0u32;

    loop {
        // The select! macro allows to await on multiple futures at the same
        // time, and to perform different branches based on which future
        // resolved. In these branches it's possible to mutate contextual values
        // without using any Mutex or channels.
        select! {
            // Timer tick.
            _ = timer.next() => {
                tracing::trace!("Time to send a request {}", msg);

                let (response_tx, response_rx) = oneshot::channel();

                if let Err(e) = handler_tx.send(Request::Echo(response_tx, msg)).await {
                    if e.is_full() {
                        tracing::warn!("Request channel is full");
                        continue;
                    }

                    if e.is_disconnected() {
                        tracing::error!("Request channel is disconnected");
                        break;
                    }
                }

                // Add the response future to the pool.
                awaiting_responses.push(response_rx);
                msg += 1;
            },
            // Awaiting responses
            res = awaiting_responses.next() => {
                match res {
                    None => {},
                    Some(Ok(res)) => tracing::info!("Get response for {}", res),
                    Some(Err(_)) => tracing::error!("A response channel was dropped"),
                }
            },
            // Config update
            status = config_rx.next() => {
                match status {
                    Some(config) => {
                        tracing::debug!("Config changed");
                        // TODO : Check if the value actually changed, and
                        // take account of the eleapsed time after previous
                        // timer tick.
                        let new_interval = interval(config.request_interval);
                        timer = IntervalStream::new(new_interval).fuse();
                    },
                    None =>{
                        tracing::debug!("config_tx dropped, let's shutdown");
                        break
                    }, // None = shutdown
                }
            }
        }
    }
}

#[instrument(skip(config_rx, handler_rx))]
async fn request_handler(
    config_rx: watch::Receiver<Config>,
    mut handler_rx: mpsc::Receiver<Request>,
) {
    loop {
        match handler_rx.next().await {
            None => {
                tracing::debug!("all handler_tx dropped, let's shutdown");
                break;
            }
            // This treatment will take time, but we want to handle more request :
            // We spawn the task.
            Some(Request::Echo(tx, msg)) => {
                // spawn returns an handle. it would allow to await later on the completion
                // of the future.
                let _ = tokio::spawn(echo_handler(config_rx.clone(), tx, msg));
            }
        }
    }
}

#[instrument(skip(config_rx, tx))]
async fn echo_handler(config_rx: watch::Receiver<Config>, tx: oneshot::Sender<u32>, msg: u32) {
    tracing::trace!("Starting complex stuff ...");

    let duration = config_rx.borrow().work_duration;
    sleep(duration).await;

    tracing::trace!("complex stuff finished !");

    match tx.send(msg) {
        Ok(()) => tracing::info!("Message was sent"),
        Err(_) => tracing::error!("Requester is dead"),
    }
}

fn setup_tracing() {
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    // a builder for `FmtSubscriber`.
    let subscriber = FmtSubscriber::builder()
        // all spans/events with a level higher than TRACE (e.g, debug, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::INFO)
        // completes the builder.
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}
