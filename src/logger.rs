use std::io;
use log::{LevelFilter, Metadata, Record, SetLoggerError};
use systemd_journal_logger::JournalLog;

use crate::defaults::DEFAULT_LOG_LEVEL;

pub struct Logger {}

fn fromsetloggererror(err: SetLoggerError) -> io::Error {
    io::Error::new(io::ErrorKind::AlreadyExists, err.to_string())
}

pub fn install(journal: bool) -> io::Result<()> {
    set_level(DEFAULT_LOG_LEVEL);
    if journal {
        JournalLog::new()?.install().map_err(fromsetloggererror)
    } else {
        Logger::new().install().map_err(fromsetloggererror)
    }
}

pub fn set_level(level: LevelFilter) {
    log::set_max_level(level);
}

impl Logger {
    fn new() -> Logger {
        Self{}
    }

    fn install(self) -> Result<(), SetLoggerError> {
        log::set_boxed_logger(Box::new(self))
    }
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
