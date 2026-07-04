use crate::models::{AboutApp, PollType, Strategy};
use anyhow::{Context, Result};
use colored_json::to_colored_json_auto;
use csv::Writer;
use serde::{Deserialize, Serialize};
use std::fmt::{self};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::net::IpAddr;
use tracing::error;

// ============================================
// 2. СТРУКТУРЫ ДЛЯ ПАРСИНГА
// ============================================

#[derive(Debug, Serialize, Deserialize)]
pub struct PollPayload {
    pub strategy: Strategy,
    pub step: usize,
    pub username: String,
    pub test_type: PollType,
    pub target: IpAddr,
    pub start: String,
    pub end: String,
    pub success: bool,
    pub attempts: u8,
    pub latency_ms: f64,
    pub details: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfigRecord {
    Independent(IndependentConfig),
    Synchronized(SynchronizedConfig),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndependentConfig {
    pub pollers: Vec<PollerConfig>,
    pub num_pollers: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SynchronizedConfig {
    pub providers: Vec<ProviderConfig>,
    pub interval_ms: u64,
    pub num_providers: u8,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogRecord {
    PollResult {
        #[serde(rename = "sid")]
        session_id: String,
        timestamp: String,
        #[serde(flatten)]
        payload: PollPayload,
    },
    Config {
        #[serde(rename = "sid")]
        session_id: String,
        timestamp: String,
        strategy: Strategy,
        //#[serde(flatten)]
        details: serde_json::Value,
    },
    StartApplication {
        #[serde(rename = "sid")]
        session_id: String,
        timestamp: String,
        details: AboutApp,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProviderConfig {
    #[serde(rename = "type")]
    pub poll_type: PollType,
    pub username: String,
    pub target: IpAddr,
    pub timeout_ms: u64,
    pub extra: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PollerConfig {
    #[serde(flatten)]
    pub provider: ProviderConfig,
    pub retries: u8,
    pub retries_interval_ms: u64,
    pub interval_ms: u64,
}

// ============================================
// 3. CSV СТРУКТУРЫ
// ============================================
//

#[derive(Debug, Serialize, Deserialize)]
struct CsvRecordPollDto {
    session_id: String,
    timestamp: String,
    strategy: Strategy,
    step: usize,
    username: String,
    poll_type: PollType,
    target: IpAddr,
    start: String,
    end: String,
    success: bool,
    attempts: u8,
    latency_ms: f64,
    details: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CsvRecordConfigDto {
    session_id: String,
    timestamp: String,
    strategy: Strategy,
    step: String,
    username: String,
    whoami: String,
    target: String,
    start: String,
    end: String,
    success: String,
    attempts: String,
    latency_ms: String,
    details: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct CsvRecordStartAppDto {
    session_id: String,
    timestamp: String,
    strategy: String,
    step: String,
    username: String,
    whoami: String,
    target: String,
    start: String,
    end: String,
    success: String,
    attempts: String,
    latency_ms: String,
    details: String,
}

// ============================================
// 4. CSV WRITER
// ============================================

pub struct CsvWriter {
    writer: Writer<File>,
}

impl CsvWriter {
    pub fn new(path: &str) -> Result<Self> {
        let writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_path(path)?;

        Ok(Self { writer })
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.write_record(&[
            "session_id",
            "timestamp",
            "strategy",
            "step",
            "name",
            "type",
            "target",
            "start",
            "end",
            "success",
            "attempts",
            "latency_ms",
            "details",
        ])?;
        Ok(())
    }
}

// ============================================
// 6. LOG WRITER TRAIT
// ============================================

pub trait LogWriter {
    fn process_line(&mut self, record: LogRecord, lineno: usize) -> Result<()>;
    fn handle_error(&mut self, error: ParseError);
}

impl LogWriter for CsvWriter {
    fn process_line(&mut self, record: LogRecord, lineno: usize) -> Result<()> {
        match record {
            LogRecord::PollResult {
                session_id,
                timestamp,
                payload,
            } => {
                let record = CsvRecordPollDto {
                    session_id,
                    timestamp,
                    strategy: payload.strategy,
                    step: payload.step,
                    username: payload.username,
                    poll_type: payload.test_type,
                    target: payload.target,
                    start: payload.start,
                    end: payload.end,
                    success: payload.success,
                    attempts: payload.attempts,
                    latency_ms: payload.latency_ms,
                    details: payload.details.unwrap_or_default(),
                };
                self.writer
                    .serialize(record)
                    .map_err(|e| {
                        error!("Csv serialize error at line {}: {}", lineno, e);
                        e
                    })?;
            }
            LogRecord::Config {
                session_id,
                timestamp,
                strategy,
                details,
            } => {
                let pretty_details = serde_json::to_string_pretty(&details)
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "Невалидный JSON в details: {}\nRaw: {}",
                            e,
                            details
                        )
                    })?;

                let record = CsvRecordConfigDto {
                    session_id,
                    timestamp,
                    strategy,
                    step: "".to_string(),
                    username: "".to_string(),
                    whoami: "session-config".to_string(),
                    target: "".to_string(),
                    start: "".to_string(),
                    end: "".to_string(),
                    success: "".to_string(),
                    attempts: "".to_string(),
                    latency_ms: "".to_string(),
                    details: pretty_details,
                };
                self.writer
                    .serialize(record)
                    .map_err(|e| {
                        error!("Csv serialize error at line {}: {}", lineno, e);
                        e
                    })?;
            }
            LogRecord::StartApplication {
                session_id,
                timestamp,
                details,
            } => {
                let pretty_details = serde_json::to_string_pretty(&details)
                    .map_err(|e| {
                        anyhow::anyhow!("Невалидный JSON в details: {}", e,)
                    })?;
                let record = CsvRecordStartAppDto {
                    session_id,
                    timestamp,
                    strategy: "".to_string(),
                    step: "".to_string(),
                    username: "".to_string(),
                    whoami: "start-application".to_string(),
                    target: "".to_string(),
                    start: "".to_string(),
                    end: "".to_string(),
                    success: "".to_string(),
                    attempts: "".to_string(),
                    latency_ms: "".to_string(),
                    details: pretty_details,
                };
                self.writer
                    .serialize(record)
                    .map_err(|e| {
                        error!("Csv serialize error at line {}: {}", lineno, e);
                        e
                    })?;
            }
        }

        Ok(())
    }

    fn handle_error(&mut self, error: ParseError) {
        eprintln!("⚠️  {error}");
    }
}
// ============================================
// 7. CONSOLE WRITER
// ============================================

pub struct ConsoleWriter;

impl ConsoleWriter {
    pub fn new() -> Self {
        Self
    }
}

impl LogWriter for ConsoleWriter {
    fn process_line(&mut self, record: LogRecord, lineno: usize) -> Result<()> {
        match to_colored_json_auto(&record) {
            Ok(colored) => {
                let head = format!(
                    "{} [Line {lineno}]{}",
                    "-".repeat(50),
                    "-".repeat(50)
                );
                let foot = format!("{}", "-".repeat(head.len()));
                println!("{head}\n{colored}\n{foot}");
            }
            Err(e) => {
                eprintln!("❌ Ошибка форматирования строки {lineno}: {e}");
            }
        }
        Ok(())
    }

    fn handle_error(&mut self, error: ParseError) {
        eprintln!("❌ {error}");
    }
}

// ============================================
// 8. ОШИБКИ
// ============================================

#[derive(Debug)]
pub struct ErrorDetail {
    pub lineno: usize,
    pub error: String,
}

#[derive(Debug)]
pub enum ParseError {
    ReadError(ErrorDetail),
    WriteError(ErrorDetail),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadError(e) => {
                write!(f, "❌ Ошибка чтения строки {}: {}", e.lineno, e.error)
            }
            Self::WriteError(e) => {
                write!(f, "❌ Ошибка записи строки {}: {}", e.lineno, e.error)
            }
        }
    }
}

// ============================================
// 9. STATS
// ============================================

#[derive(Debug)]
pub struct Stats {
    pub lines_processed: usize,
    pub errors: Vec<ParseError>,
}

impl Stats {
    pub fn new() -> Self {
        Self {
            lines_processed: 0,
            errors: Vec::new(),
        }
    }

    pub fn push_error(&mut self, error: ParseError) {
        self.errors.push(error);
    }

    pub fn view_as_string(&self) -> String {
        let string = format!(
            "Всего прочитано строк: {}\nУспешно прочитано строк: {}\nКоличество ошибок: {}\nОшибки:{}",
            self.lines_processed,
            self.successed_lines(),
            self.errors.len(),
            self.fotrmatted_errors(),
        );

        string
    }

    pub fn successed_lines(&self) -> usize {
        self.lines_processed - self.errors.len()
    }

    pub fn fotrmatted_errors(&self) -> String {
        self.errors
            .iter()
            .enumerate()
            .map(|(i, e)| format!("\n{}: {e}", i + 1))
            .collect()
    }
}

// ============================================
// 10. LOG PARSER
// ============================================
#[derive(Debug)]
pub struct FileWriteDetails {
    pub stats: Stats,
    pub path: String,
}

pub struct LogParser<R: Read> {
    reader: BufReader<R>,
}

impl<R: Read> LogParser<R> {
    pub fn new(source: R) -> Self {
        Self {
            reader: BufReader::new(source),
        }
    }

    pub fn to_console(self) -> Result<Stats> {
        let writer = ConsoleWriter::new();
        let stats = self.parse(writer);
        stats
    }

    pub fn to_csv(self) -> Result<FileWriteDetails> {
        let path = "net-monitor.csv".to_string();
        let mut csv_writer = CsvWriter::new(&path)?;
        csv_writer.write_header()?;

        let stats = self.parse(csv_writer)?;

        Ok(FileWriteDetails { stats, path })
    }

    pub fn parse<W: LogWriter>(self, mut writer: W) -> Result<Stats> {
        let mut stats = Stats::new();
        let mut lineno = 0usize;

        for line in self.reader.lines() {
            let line = line.map_err(|e| {
                error!("Line {}: read error: {}", lineno, &e);
                e
            })?;
            lineno += 1;

            match serde_json::from_str::<LogRecord>(&line) {
                Ok(log) => {
                    if let Err(e) = writer.process_line(log, lineno) {
                        let err = ParseError::ReadError(ErrorDetail {
                            lineno,
                            error: e.to_string(),
                        });
                        stats.push_error(err);
                        error!("Write record {lineno}: {}", &e);
                        return Err(e);
                    }
                    //stats.success_lines += 1;
                }
                Err(e) => {
                    let err = ParseError::ReadError(ErrorDetail {
                        lineno,
                        error: e.to_string(),
                    });
                    stats.push_error(err);
                }
            }
            stats.lines_processed += 1;
        }

        Ok(stats)
    }
}
// ============================================
// 6. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
// ============================================
fn create_log_record(line: &str) -> Result<LogRecord> {
    serde_json::from_str(line).context("Невалидная строка")
}

fn format_message(message: &str, lineno: u64) -> String {
    format!("{}\n{:-<50} LINE {} {:-<50}\n", message, "", lineno, "")
}

/*
LogRecord (enum, тег type)
├── Config (вариант)
│   ├── session_id: String
│   ├── timestamp: String
│   └── config: ConfigRecord (enum, тег strategy)
│       ├── Independent (вариант)
│       │   ├── pollers: Vec<PollerConfig>
│       │   └── num_pollers: u8
│       └── Synchronized (вариант)
│           ├── providers: Vec<ProviderConfig>
│           ├── interval_ms: u64
│           └── num_providers: u8
│
└── PollResult (вариант)
    ├── session_id: String
    ├── timestamp: String
    └── payload: PollRecord (struct)
        ├── strategy: Strategy (enum)
        ├── step: u64
        ├── username: String
        ├── test_type: TestType (enum)
        ├── target: IpAddr
        └── ...
*/

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::net::IpAddr;
    use std::str::FromStr;

    // ============================================
    // 1. ТЕСТЫ ДЛЯ POLL_RESULT
    // ============================================

    #[test]
    fn test_parse_poll_result_ping_success() {
        let json = json!({
            "sid": "test123",
            "timestamp": "2026-06-23T10:00:00.000+03:00",
            "type": "poll_result",
            "strategy": "independent",
            "step": 1,
            "username": "fast",
            "test_type": "ping",
            "target": "8.8.8.8",
            "start": "10:00:00.000",
            "end": "10:00:00.005",
            "success": true,
            "attempts": 1,
            "latency_ms": 5.123,
            "details": "Попытка 1: Успех. RTT: 5ms"
        });

        let record: LogRecord = serde_json::from_value(json).unwrap();

        match record {
            LogRecord::PollResult {
                session_id,
                timestamp,
                payload,
            } => {
                assert_eq!(session_id, "test123");
                assert_eq!(timestamp, "2026-06-23T10:00:00.000+03:00");
                assert_eq!(payload.strategy, Strategy::Independent);
                assert_eq!(payload.step, 1);
                assert_eq!(payload.username, "fast");
                assert_eq!(payload.whoami, TestType::Ping);
                assert_eq!(
                    payload.target,
                    IpAddr::from_str("8.8.8.8").unwrap()
                );
                assert_eq!(payload.success, true);
                assert_eq!(payload.attempts, 1);
                assert_eq!(payload.latency_ms, 5.123);
                assert!(payload.details.contains("Успех"));
            }
            _ => panic!("Expected PollResult"),
        }
    }

    #[test]
    fn test_parse_poll_result_ping_failure() {
        let json = json!({
            "sid": "test123",
            "timestamp": "2026-06-23T10:00:00.000+03:00",
            "type": "poll_result",
            "strategy": "independent",
            "step": 1,
            "username": "fast",
            "test_type": "ping",
            "target": "10.179.180.190",
            "start": "10:00:00.000",
            "end": "10:00:00.200",
            "success": false,
            "attempts": 3,
            "latency_ms": 200.5,
            "details": "Попытка 1: Превышен таймаут ответа.: 200.764ms; Попытка 2: Превышен таймаут ответа.: 200.819ms"
        });

        let record: LogRecord = serde_json::from_value(json).unwrap();

        match record {
            LogRecord::PollResult { payload, .. } => {
                assert_eq!(payload.username, "fast");
                assert_eq!(payload.whoami, TestType::Ping);
                assert_eq!(
                    payload.target,
                    IpAddr::from_str("10.179.180.190").unwrap()
                );
                assert_eq!(payload.success, false);
                assert_eq!(payload.attempts, 3);
                assert!(
                    payload
                        .details
                        .contains("Превышен таймаут")
                );
            }
            _ => panic!("Expected PollResult"),
        }
    }

    #[test]
    fn test_parse_poll_result_snmp_failure() {
        let json = json!({
            "sid": "test123",
            "timestamp": "2026-06-23T10:00:00.000+03:00",
            "type": "poll_result",
            "strategy": "synchronized",
            "step": 5,
            "username": "main",
            "test_type": "snmp",
            "target": "10.179.180.190",
            "start": "10:00:00.000",
            "end": "10:00:01.001",
            "success": false,
            "attempts": 2,
            "latency_ms": 1001.5,
            "details": "Попытка 1: SNMP error: timeout after 1.001s; Попытка 2: SNMP error: timeout after 1.001s"
        });

        let record: LogRecord = serde_json::from_value(json).unwrap();

        match record {
            LogRecord::PollResult { payload, .. } => {
                assert_eq!(payload.username, "main");
                assert_eq!(payload.whoami, TestType::Snmp);
                assert_eq!(payload.strategy, Strategy::Synchronized);
                assert_eq!(payload.success, false);
                assert_eq!(payload.attempts, 2);
                assert!(payload.details.contains("SNMP error"));
            }
            _ => panic!("Expected PollResult"),
        }
    }

    #[test]
    fn test_parse_poll_result_synchronized() {
        let json = json!({
            "sid": "test123",
            "timestamp": "2026-06-23T10:00:00.000+03:00",
            "type": "poll_result",
            "strategy": "synchronized",
            "step": 10,
            "username": "primary",
            "test_type": "ping",
            "target": "1.1.1.1",
            "start": "10:00:00.000",
            "end": "10:00:00.010",
            "success": true,
            "attempts": 1,
            "latency_ms": 10.5,
            "details": "Попытка 1: Успех. RTT: 10ms"
        });

        let record: LogRecord = serde_json::from_value(json).unwrap();

        match record {
            LogRecord::PollResult { payload, .. } => {
                assert_eq!(payload.strategy, Strategy::Synchronized);
                assert_eq!(payload.step, 10);
                assert_eq!(payload.username, "primary");
                assert_eq!(
                    payload.target,
                    IpAddr::from_str("1.1.1.1").unwrap()
                );
                assert_eq!(payload.success, true);
            }
            _ => panic!("Expected PollResult"),
        }
    }

    // ============================================
    // 2. ТЕСТЫ ДЛЯ CONFIG
    // ============================================

    #[test]
    fn test_parse_config_independent() {
        let json = json!({
            "sid": "test456",
            "timestamp": "2026-06-23T09:00:00.000+03:00",
            "type": "config",
            "strategy": "independent",
            "pollers": [
                {
                    "type": "ping",
                    "username": "fast",
                    "target": "8.8.8.8",
                    "timeout_ms": 200,
                    "extra": null,
                    "retries": 3,
                    "retries_interval_ms": 200,
                    "interval_ms": 1000
                },
                {
                    "type": "snmp",
                    "username": "main",
                    "target": "10.179.180.190",
                    "timeout_ms": 350,
                    "extra": {
                        "oids": ["1.3.6.1.2.1.1.3.0"],
                        "port": 161
                    },
                    "retries": 2,
                    "retries_interval_ms": 300,
                    "interval_ms": 6000
                }
            ],
            "num_pollers": 2
        });

        let record: LogRecord = serde_json::from_value(json).unwrap();

        match record {
            LogRecord::Config {
                session_id,
                timestamp,
                config,
            } => {
                assert_eq!(session_id, "test456");
                assert_eq!(timestamp, "2026-06-23T09:00:00.000+03:00");

                match config {
                    ConfigRecord::Independent {
                        pollers,
                        num_pollers,
                    } => {
                        assert_eq!(num_pollers, 2);
                        assert_eq!(pollers.len(), 2);

                        // Проверяем первый поллер (ping)
                        assert_eq!(
                            pollers[0].provider.poll_type,
                            TestType::Ping
                        );
                        assert_eq!(pollers[0].provider.username, "fast");
                        assert_eq!(
                            pollers[0].provider.target,
                            IpAddr::from_str("8.8.8.8").unwrap()
                        );
                        assert_eq!(pollers[0].provider.timeout_ms, 200);
                        assert_eq!(pollers[0].provider.extra, None);
                        assert_eq!(pollers[0].retries, 3);
                        assert_eq!(pollers[0].interval_ms, 1000);

                        // Проверяем второй поллер (snmp)
                        assert_eq!(
                            pollers[1].provider.poll_type,
                            TestType::Snmp
                        );
                        assert_eq!(pollers[1].provider.username, "main");
                        assert_eq!(
                            pollers[1].provider.target,
                            IpAddr::from_str("10.179.180.190").unwrap()
                        );
                        assert_eq!(pollers[1].provider.timeout_ms, 350);
                        assert!(pollers[1].provider.extra.is_some());
                        assert_eq!(pollers[1].retries, 2);
                        assert_eq!(pollers[1].interval_ms, 6000);
                    }
                    _ => panic!("Expected Independent"),
                }
            }
            _ => panic!("Expected Config"),
        }
    }

    #[test]
    fn test_parse_config_synchronized() {
        let json = json!({
            "sid": "test789",
            "timestamp": "2026-06-23T09:00:00.000+03:00",
            "type": "config",
            "strategy": "synchronized",
            "providers": [
                {
                    "type": "ping",
                    "username": "primary",
                    "target": "8.8.8.8",
                    "timeout_ms": 200,
                    "extra": null
                },
                {
                    "type": "snmp",
                    "username": "main",
                    "target": "10.179.180.190",
                    "timeout_ms": 350,
                    "extra": {
                        "oids": ["1.3.6.1.2.1.1.3.0"],
                        "port": 161
                    }
                }
            ],
            "interval_ms": 4000,
            "num_providers": 2
        });

        let record: LogRecord = serde_json::from_value(json).unwrap();

        match record {
            LogRecord::Config {
                session_id,
                timestamp,
                config,
            } => {
                assert_eq!(session_id, "test789");
                assert_eq!(timestamp, "2026-06-23T09:00:00.000+03:00");

                match config {
                    ConfigRecord::Synchronized {
                        providers,
                        interval_ms,
                        num_providers,
                    } => {
                        assert_eq!(interval_ms, 4000);
                        assert_eq!(num_providers, 2);
                        assert_eq!(providers.len(), 2);

                        // Проверяем первый провайдер (ping)
                        assert_eq!(providers[0].poll_type, TestType::Ping);
                        assert_eq!(providers[0].username, "primary");
                        assert_eq!(
                            providers[0].target,
                            IpAddr::from_str("8.8.8.8").unwrap()
                        );
                        assert_eq!(providers[0].timeout_ms, 200);
                        assert_eq!(providers[0].extra, None);

                        // Проверяем второй провайдер (snmp)
                        assert_eq!(providers[1].poll_type, TestType::Snmp);
                        assert_eq!(providers[1].username, "main");
                        assert_eq!(
                            providers[1].target,
                            IpAddr::from_str("10.179.180.190").unwrap()
                        );
                        assert_eq!(providers[1].timeout_ms, 350);
                        assert!(providers[1].extra.is_some());
                    }
                    _ => panic!("Expected Synchronized"),
                }
            }
            _ => panic!("Expected Config"),
        }
    }

    // ============================================
    // 3. ТЕСТЫ С РЕАЛЬНЫМИ ДАННЫМИ ИЗ ЛОГА
    // ============================================

    #[test]
    fn test_parse_real_poll_result() {
        let json = r#"{"sid":"7f85e4320e69","timestamp":"2026-06-22T22:58:26.700+03:00","type":"poll_result","strategy":"independent","step":15,"username":"fast","test_type":"ping","target":"10.179.180.190","start":"22:58:25.694","end":"22:58:26.700","success":false,"attempts":3,"latency_ms":1006.1679290000001,"details":"Попытка 1: Превышен таймаут ответа.: 200.764392ms; Попытка 2: Превышен таймаут ответа.: 200.819549ms; Попытка 3: Превышен таймаут ответа.: 201.732801ms"}"#;

        let record: LogRecord = serde_json::from_str(json).unwrap();

        match record {
            LogRecord::PollResult {
                session_id,
                payload,
                ..
            } => {
                assert_eq!(session_id, "7f85e4320e69");
                assert_eq!(payload.username, "fast");
                assert_eq!(payload.whoami, TestType::Ping);
                assert_eq!(
                    payload.target,
                    IpAddr::from_str("10.179.180.190").unwrap()
                );
                assert_eq!(payload.success, false);
                assert_eq!(payload.attempts, 3);
            }
            _ => panic!("Expected PollResult"),
        }
    }

    #[test]
    fn test_parse_real_config_independent() {
        let json = r#"{"sid":"7f85e4320e69","timestamp":"2026-06-22T22:58:11.615+03:00","type":"config","strategy":"independent","pollers":[{"type":"ping","username":"fast","target":"10.179.180.190","timeout_ms":200,"extra":null,"retries":3,"retries_interval_ms":200,"interval_ms":1000},{"type":"ping","username":"slow","target":"10.179.180.190","timeout_ms":500,"extra":null,"retries":5,"retries_interval_ms":500,"interval_ms":5000},{"type":"snmp","username":"main","target":"10.179.180.190","timeout_ms":350,"extra":{"oids":["1.3.6.1.2.1.1.3.0","1.3.6.1.4.1.13267.3.2.4.1.0"],"port":161},"retries":2,"retries_interval_ms":300,"interval_ms":6000}],"num_pollers":3}"#;

        let record: LogRecord = serde_json::from_str(json).unwrap();

        match record {
            LogRecord::Config { config, .. } => match config {
                ConfigRecord::Independent {
                    pollers,
                    num_pollers,
                } => {
                    assert_eq!(num_pollers, 3);
                    assert_eq!(pollers.len(), 3);
                    assert_eq!(pollers[0].provider.username, "fast");
                    assert_eq!(pollers[1].provider.username, "slow");
                    assert_eq!(pollers[2].provider.username, "main");
                }
                _ => panic!("Expected Independent"),
            },
            _ => panic!("Expected Config"),
        }
    }

    // ============================================
    // 4. СТРЕСС-ТЕСТ: МНОГО ЗАПИСЕЙ
    // ============================================

    #[test]
    fn test_parse_multiple_records() {
        let json_data = vec![
            json!({
                "sid": "test1",
                "timestamp": "2026-06-23T10:00:00.000+03:00",
                "type": "config",
                "strategy": "independent",
                "pollers": [],
                "num_pollers": 0
            }),
            json!({
                "sid": "test1",
                "timestamp": "2026-06-23T10:00:01.000+03:00",
                "type": "poll_result",
                "strategy": "independent",
                "step": 1,
                "username": "fast",
                "test_type": "ping",
                "target": "8.8.8.8",
                "start": "10:00:01.000",
                "end": "10:00:01.005",
                "success": true,
                "attempts": 1,
                "latency_ms": 5.0,
                "details": "OK"
            }),
            json!({
                "sid": "test1",
                "timestamp": "2026-06-23T10:00:02.000+03:00",
                "type": "poll_result",
                "strategy": "independent",
                "step": 2,
                "username": "main",
                "test_type": "snmp",
                "target": "10.179.180.190",
                "start": "10:00:02.000",
                "end": "10:00:03.000",
                "success": false,
                "attempts": 2,
                "latency_ms": 1000.0,
                "details": "Timeout"
            }),
        ];

        let mut count = 0;
        for json in json_data {
            let record: LogRecord = serde_json::from_value(json).unwrap();
            match record {
                LogRecord::Config { .. } => count += 1,
                LogRecord::PollResult { .. } => count += 1,
            }
        }

        assert_eq!(count, 3);
    }

    // ============================================
    // 5. ТЕСТ НА ОШИБКУ (что не парсится)
    // ============================================

    #[test]
    #[should_panic(expected = "missing field")]
    fn test_parse_invalid_missing_field() {
        let json = json!({
            "sid": "test",
            "type": "poll_result",
            // нет поля "timestamp"
        });

        let _record: LogRecord = serde_json::from_value(json).unwrap();
    }

    #[test]
    #[should_panic(expected = "unknown variant")]
    fn test_parse_invalid_type() {
        let json = json!({
            "sid": "test",
            "timestamp": "2026-06-23T10:00:00.000+03:00",
            "type": "invalid_type",
        });

        let _record: LogRecord = serde_json::from_value(json).unwrap();
    }
}
