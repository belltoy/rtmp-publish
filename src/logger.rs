use slog::{self, o, Drain, Logger};

const DEFAULT_CHAN_SIZE: usize = 1024;

pub struct LogGuard {
    _async_guard: slog_async::AsyncGuard,
}

impl Drop for LogGuard {
    fn drop(&mut self) {
    }
}

pub fn init() -> (Logger, LogGuard) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();

    // TODO Maybe set a larger channel size to prevent logs drop
    // See https://github.com/slog-rs/async/issues/4
    let (drain, async_guard) = slog_async::Async::new(drain)
        .chan_size(DEFAULT_CHAN_SIZE)
        .overflow_strategy(slog_async::OverflowStrategy::Block)
        .thread_name("slog_async".to_owned())
        .build_with_guard();

    let root = Logger::root(drain.ignore_res(), o!());

    let guard = LogGuard {
        _async_guard: async_guard,
    };

    (root, guard)
}
