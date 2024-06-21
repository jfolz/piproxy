use log::{LevelFilter, Metadata, Record};
use crate::defaults::DEFAULT_LOG_LEVEL;

pub struct Logger {}

static LOGGER: Logger = Logger {};

pub fn install() -> Result<(), log::SetLoggerError> {
    let result = log::set_logger(&LOGGER);
    set_level(DEFAULT_LOG_LEVEL);
    result
}

pub fn set_level(level: LevelFilter) {
    log::set_max_level(level);
}

impl log::Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}
