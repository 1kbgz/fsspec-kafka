use chrono::{DateTime, Datelike, Days, Months, NaiveDate, NaiveDateTime, TimeDelta, Weekday};
use fsspec_data::DataFormat;
use fsspec_rs::{FsError, FsResult};

/// The directory of ISO week buckets in a `kafka+time` topic.
pub const WEEKS_DIR: &str = "weeks";

/// How a filesystem arranges a topic's messages. Each view is its own protocol.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum KafkaView {
    /// `kafka+offset://topic/{partition}/{start-end}`, also `kafka://`.
    #[default]
    Offset,
    /// `kafka+time://topic/{YYYY}/{MM}/{DD}/{hh}/{mm}/{ss}/{mmm}` and `topic/weeks/{YYYY-Www}`.
    Time,
    /// `kafka+records://topic/{key...}`: the latest value of each key.
    Records,
    /// `kafka+history://topic/{key...}`: every message of each key.
    History,
}

impl KafkaView {
    pub fn protocols(self) -> &'static [&'static str] {
        match self {
            Self::Offset => &["kafka", "kafka+offset"],
            Self::Time => &["kafka+time"],
            Self::Records => &["kafka+records"],
            Self::History => &["kafka+history"],
        }
    }

    pub fn parse(value: &str) -> FsResult<Self> {
        match value {
            "offset" => Ok(Self::Offset),
            "time" => Ok(Self::Time),
            "records" => Ok(Self::Records),
            "history" => Ok(Self::History),
            _ => Err(FsError::InvalidArgument(format!(
                "view must be 'offset', 'time', 'records', or 'history', not {value:?}"
            ))),
        }
    }
}

/// A level of the `kafka+time` hierarchy, from years down to milliseconds.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub enum TimeLevel {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    #[default]
    Millisecond,
}

impl TimeLevel {
    pub const ALL: [TimeLevel; 7] = [
        Self::Year,
        Self::Month,
        Self::Day,
        Self::Hour,
        Self::Minute,
        Self::Second,
        Self::Millisecond,
    ];

    pub fn parse(value: &str) -> FsResult<Self> {
        Self::ALL
            .into_iter()
            .find(|level| level.as_str() == value)
            .ok_or_else(|| {
                FsError::InvalidArgument(format!(
                    "time_bucket must be one of year, month, day, hour, minute, second, \
                     millisecond, not {value:?}"
                ))
            })
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Year => "year",
            Self::Month => "month",
            Self::Day => "day",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Millisecond => "millisecond",
        }
    }

    /// Number of path segments down to and including this level.
    pub fn depth(self) -> usize {
        self as usize + 1
    }

    fn width(self) -> usize {
        match self {
            Self::Year => 4,
            Self::Millisecond => 3,
            _ => 2,
        }
    }
}

/// A span of time named by a `kafka+time` path. Calendar buckets hold one value per level
/// (year, month, day, hour, minute, second, millisecond), at least the year.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Bucket {
    Calendar(Vec<u32>),
    /// An ISO 8601 week: year and week number.
    Week(i32, u32),
}

impl Bucket {
    pub fn level(&self) -> Option<TimeLevel> {
        match self {
            Self::Calendar(parts) => Some(TimeLevel::ALL[parts.len() - 1]),
            Self::Week(..) => None,
        }
    }

    /// Start of the bucket in UTC milliseconds.
    pub fn start_ms(&self) -> i64 {
        self.start().and_utc().timestamp_millis()
    }

    /// End of the bucket (exclusive) in UTC milliseconds.
    pub fn end_ms(&self) -> i64 {
        let start = self.start();
        let end = match self {
            Self::Week(..) => start + Days::new(7),
            Self::Calendar(parts) => match TimeLevel::ALL[parts.len() - 1] {
                TimeLevel::Year => start + Months::new(12),
                TimeLevel::Month => start + Months::new(1),
                TimeLevel::Day => start + Days::new(1),
                TimeLevel::Hour => start + TimeDelta::hours(1),
                TimeLevel::Minute => start + TimeDelta::minutes(1),
                TimeLevel::Second => start + TimeDelta::seconds(1),
                TimeLevel::Millisecond => start + TimeDelta::milliseconds(1),
            },
        };
        end.and_utc().timestamp_millis()
    }

    fn start(&self) -> NaiveDateTime {
        match self {
            Self::Week(year, week) => NaiveDate::from_isoywd_opt(*year, *week, Weekday::Mon)
                .expect("parsed weeks are valid")
                .and_hms_opt(0, 0, 0)
                .expect("midnight is valid"),
            Self::Calendar(parts) => {
                let part =
                    |index: usize, default: u32| parts.get(index).copied().unwrap_or(default);
                NaiveDate::from_ymd_opt(part(0, 0) as i32, part(1, 1), part(2, 1))
                    .and_then(|date| {
                        date.and_hms_milli_opt(part(3, 0), part(4, 0), part(5, 0), part(6, 0))
                    })
                    .expect("parsed buckets are valid")
            }
        }
    }

    /// The bucket one level below this one that contains `timestamp_ms`.
    pub fn child_at(&self, timestamp_ms: i64) -> Option<Bucket> {
        let Self::Calendar(parts) = self else {
            return None;
        };
        let time = DateTime::from_timestamp_millis(timestamp_ms)?.naive_utc();
        let value = match parts.len() {
            1 => time.month(),
            2 => time.day(),
            3 => chrono::Timelike::hour(&time),
            4 => chrono::Timelike::minute(&time),
            5 => chrono::Timelike::second(&time),
            6 => time.and_utc().timestamp_subsec_millis(),
            _ => return None,
        };
        let mut parts = parts.clone();
        parts.push(value);
        Some(Self::Calendar(parts))
    }

    /// The year bucket containing `timestamp_ms`.
    pub fn year_at(timestamp_ms: i64) -> Option<Bucket> {
        let year = DateTime::from_timestamp_millis(timestamp_ms)?.year();
        u32::try_from(year)
            .ok()
            .filter(|year| *year <= 9999)
            .map(|year| Self::Calendar(vec![year]))
    }

    /// The ISO week containing `timestamp_ms`.
    pub fn week_at(timestamp_ms: i64) -> Option<Bucket> {
        let week = DateTime::from_timestamp_millis(timestamp_ms)?.iso_week();
        (0..=9999)
            .contains(&week.year())
            .then(|| Self::Week(week.year(), week.week()))
    }

    /// The name of the bucket's last path segment.
    pub fn name(&self) -> String {
        match self {
            Self::Week(year, week) => format!("{year:04}-W{week:02}"),
            Self::Calendar(parts) => {
                let level = TimeLevel::ALL[parts.len() - 1];
                format!("{:0width$}", parts[parts.len() - 1], width = level.width())
            }
        }
    }

    fn segments(&self) -> Vec<String> {
        match self {
            Self::Week(..) => vec![WEEKS_DIR.to_string(), self.name()],
            Self::Calendar(parts) => (1..=parts.len())
                .map(|len| Self::Calendar(parts[..len].to_vec()).name())
                .collect(),
        }
    }
}

/// An inclusive offset range. `end` is `None` for the head window, which ends at the
/// high watermark captured when it is read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Window {
    pub start: i64,
    pub end: Option<i64>,
}

impl Window {
    fn name(self, format: DataFormat) -> String {
        match self.end {
            Some(end) => format!("{}-{end}.{}", self.start, format.as_str()),
            None => format!("{}-.{}", self.start, format.as_str()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KafkaPath {
    Root,
    Topic(String),
    /// `topic.format`: every message of the topic. In `kafka+records`, the latest message
    /// of each key.
    TopicSnapshot {
        topic: String,
        format: DataFormat,
    },
    Partition(String, i32),
    Window {
        topic: String,
        partition: i32,
        window: Window,
        format: DataFormat,
    },
    /// `topic/partition.format?start=&limit=`
    PartitionRead {
        topic: String,
        partition: i32,
        format: DataFormat,
        start: Option<i64>,
        limit: Option<u64>,
    },
    /// `topic/weeks`
    Weeks(String),
    /// A time bucket as a directory, or its messages when `format` is set.
    Bucket {
        topic: String,
        bucket: Bucket,
        format: Option<DataFormat>,
    },
    /// `kafka+records://topic/{key...}`: the latest value of a key in a compacted topic. The
    /// key is the rest of the path, unchanged.
    Records {
        topic: String,
        key: String,
    },
    /// `kafka+history://topic/{key...}`: every message of a key.
    History {
        topic: String,
        key: String,
    },
}

impl KafkaPath {
    pub fn parse(path: &str, view: KafkaView) -> FsResult<Self> {
        let path = match path.split_once("://") {
            Some((scheme, rest)) if scheme.starts_with("kafka") => rest,
            _ => path,
        };
        let (path, query) = match path.split_once('?') {
            Some((path, query)) => (path, Some(query)),
            None => (path, None),
        };
        let segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        let parsed = match (view, segments.as_slice()) {
            (_, []) => Self::Root,
            (_, [name]) => match split_format(name) {
                Some((topic, format)) => Self::TopicSnapshot {
                    topic: topic_name(topic)?,
                    format,
                },
                None => Self::Topic(topic_name(name)?),
            },
            (KafkaView::Offset, [topic, rest @ ..]) => {
                return parse_offset(topic_name(topic)?, rest, query).ok_or_else(|| invalid(path))
            }
            (KafkaView::Time, [topic, rest @ ..]) => {
                parse_time(topic_name(topic)?, rest).ok_or_else(|| invalid(path))?
            }
            (KafkaView::Records, [topic, key @ ..]) => Self::Records {
                topic: topic_name(topic)?,
                key: key.join("/"),
            },
            (KafkaView::History, [topic, key @ ..]) => Self::History {
                topic: topic_name(topic)?,
                key: key.join("/"),
            },
        };
        if query.is_some() {
            return Err(FsError::InvalidArgument(format!(
                "query parameters are only accepted on partition reads: {path}"
            )));
        }
        Ok(parsed)
    }

    pub fn to_path(&self) -> String {
        match self {
            Self::Root => String::new(),
            Self::Topic(topic) => topic.clone(),
            Self::TopicSnapshot { topic, format } => format!("{topic}.{}", format.as_str()),
            Self::Partition(topic, partition) => format!("{topic}/{partition}"),
            Self::Window {
                topic,
                partition,
                window,
                format,
            } => format!("{topic}/{partition}/{}", window.name(*format)),
            Self::PartitionRead {
                topic,
                partition,
                format,
                start,
                limit,
            } => {
                let query = [
                    start.map(|start| format!("start={start}")),
                    limit.map(|limit| format!("limit={limit}")),
                ]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join("&");
                let path = format!("{topic}/{partition}.{}", format.as_str());
                if query.is_empty() {
                    path
                } else {
                    format!("{path}?{query}")
                }
            }
            Self::Weeks(topic) => format!("{topic}/{WEEKS_DIR}"),
            Self::Bucket {
                topic,
                bucket,
                format,
            } => {
                let path = format!("{topic}/{}", bucket.segments().join("/"));
                match format {
                    Some(format) => format!("{path}.{}", format.as_str()),
                    None => path,
                }
            }
            Self::Records { topic, key } | Self::History { topic, key } => {
                format!("{topic}/{key}")
            }
        }
    }
}

fn parse_offset(topic: String, rest: &[&str], query: Option<&str>) -> Option<KafkaPath> {
    let parsed = match rest {
        [name] => match split_format(name) {
            Some((partition, format)) => {
                let (start, limit) = parse_query(query.unwrap_or("")).ok()?;
                return Some(KafkaPath::PartitionRead {
                    topic,
                    partition: partition_id(partition)?,
                    format,
                    start,
                    limit,
                });
            }
            None => KafkaPath::Partition(topic, partition_id(name)?),
        },
        [partition, name] => {
            let (range, format) = split_format(name)?;
            KafkaPath::Window {
                topic,
                partition: partition_id(partition)?,
                window: parse_window(range)?,
                format,
            }
        }
        _ => return None,
    };
    query.is_none().then_some(parsed)
}

fn parse_time(topic: String, rest: &[&str]) -> Option<KafkaPath> {
    let (last, parents) = rest.split_last()?;
    let (name, format) = match split_format(last) {
        Some((name, format)) => (name, Some(format)),
        None => (*last, None),
    };
    let bucket = match (parents, name) {
        ([], WEEKS_DIR) if format.is_none() => return Some(KafkaPath::Weeks(topic)),
        ([WEEKS_DIR], week) => {
            let (year, week) = week.split_once("-W")?;
            let year = digits(year, 4)? as i32;
            let week = digits(week, 2)?;
            NaiveDate::from_isoywd_opt(year, week, Weekday::Mon)?;
            Bucket::Week(year, week)
        }
        _ => {
            let parts = parents
                .iter()
                .chain([&name])
                .zip(TimeLevel::ALL)
                .map(|(segment, level)| digits(segment, level.width()))
                .collect::<Option<Vec<_>>>()?;
            if parts.len() != parents.len() + 1 || !valid_calendar(&parts) {
                return None;
            }
            Bucket::Calendar(parts)
        }
    };
    Some(KafkaPath::Bucket {
        topic,
        bucket,
        format,
    })
}

fn valid_calendar(parts: &[u32]) -> bool {
    let part = |index: usize, default: u32| parts.get(index).copied().unwrap_or(default);
    NaiveDate::from_ymd_opt(part(0, 0) as i32, part(1, 1), part(2, 1))
        .and_then(|date| date.and_hms_milli_opt(part(3, 0), part(4, 0), part(5, 0), part(6, 0)))
        .is_some()
        // and_hms_milli_opt accepts a leap second as 60 seconds or 1000+ milliseconds.
        && part(5, 0) < 60
        && part(6, 0) < 1000
}

fn digits(value: &str, width: usize) -> Option<u32> {
    (value.len() == width && value.bytes().all(|b| b.is_ascii_digit()))
        .then(|| value.parse().ok())
        .flatten()
}

fn invalid(path: &str) -> FsError {
    FsError::InvalidArgument(format!("not a Kafka path: {path}"))
}

/// Kafka topic names are 1 to 249 characters from `[a-zA-Z0-9._-]`.
fn topic_name(name: &str) -> FsResult<String> {
    let valid = !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'));
    if valid {
        Ok(name.to_string())
    } else {
        Err(FsError::InvalidArgument(format!(
            "invalid topic name: {name:?}"
        )))
    }
}

fn partition_id(value: &str) -> Option<i32> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn offset(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

fn split_format(name: &str) -> Option<(&str, DataFormat)> {
    let (stem, extension) = name.rsplit_once('.')?;
    let format = match extension {
        "arrow" | "ipc" => DataFormat::Arrow,
        "parquet" | "pq" => DataFormat::Parquet,
        "csv" => DataFormat::Csv,
        "jsonl" | "ndjson" => DataFormat::JsonLines,
        _ => return None,
    };
    Some((stem, format))
}

fn parse_window(range: &str) -> Option<Window> {
    let (start, end) = range.split_once('-')?;
    let start = offset(start)?;
    let end = if end.is_empty() {
        None
    } else {
        Some(offset(end)?).filter(|end| *end >= start)
    };
    if end.is_none() && !range.ends_with('-') {
        return None;
    }
    Some(Window { start, end })
}

fn parse_query(query: &str) -> FsResult<(Option<i64>, Option<u64>)> {
    let mut start = None;
    let mut limit = None;
    for pair in query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let bad = || FsError::InvalidArgument(format!("invalid query parameter: {pair}"));
        match key {
            "start" => start = Some(offset(value).ok_or_else(bad)?),
            "limit" => limit = Some(value.parse::<u64>().map_err(|_| bad())?),
            _ => return Err(bad()),
        }
    }
    Ok((start, limit))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(view: KafkaView, path: &str, expected: KafkaPath) {
        let parsed = KafkaPath::parse(path, view).unwrap();
        assert_eq!(parsed, expected);
        assert_eq!(KafkaPath::parse(&parsed.to_path(), view).unwrap(), expected);
    }

    fn offset_path(path: &str, expected: KafkaPath) {
        round_trip(KafkaView::Offset, path, expected);
    }

    #[test]
    fn parses_directories() {
        offset_path("", KafkaPath::Root);
        offset_path("kafka://", KafkaPath::Root);
        offset_path("orders", KafkaPath::Topic("orders".into()));
        offset_path("kafka://orders/", KafkaPath::Topic("orders".into()));
        offset_path("kafka+offset://orders/", KafkaPath::Topic("orders".into()));
        offset_path("orders/3", KafkaPath::Partition("orders".into(), 3));
        for view in [KafkaView::Time, KafkaView::Records, KafkaView::History] {
            round_trip(
                view,
                "kafka+time://orders",
                KafkaPath::Topic("orders".into()),
            );
        }
    }

    #[test]
    fn parses_windows() {
        offset_path(
            "orders/0/10000-19999.parquet",
            KafkaPath::Window {
                topic: "orders".into(),
                partition: 0,
                window: Window {
                    start: 10000,
                    end: Some(19999),
                },
                format: DataFormat::Parquet,
            },
        );
        offset_path(
            "orders/0/20000-.arrow",
            KafkaPath::Window {
                topic: "orders".into(),
                partition: 0,
                window: Window {
                    start: 20000,
                    end: None,
                },
                format: DataFormat::Arrow,
            },
        );
    }

    #[test]
    fn parses_reads_and_snapshots() {
        offset_path(
            "orders/1.jsonl?start=5000&limit=100",
            KafkaPath::PartitionRead {
                topic: "orders".into(),
                partition: 1,
                format: DataFormat::JsonLines,
                start: Some(5000),
                limit: Some(100),
            },
        );
        offset_path(
            "orders/1.csv",
            KafkaPath::PartitionRead {
                topic: "orders".into(),
                partition: 1,
                format: DataFormat::Csv,
                start: None,
                limit: None,
            },
        );
        offset_path(
            "orders.v1.parquet",
            KafkaPath::TopicSnapshot {
                topic: "orders.v1".into(),
                format: DataFormat::Parquet,
            },
        );
        offset_path("orders.v1", KafkaPath::Topic("orders.v1".into()));
        round_trip(
            KafkaView::Records,
            "state.arrow",
            KafkaPath::TopicSnapshot {
                topic: "state".into(),
                format: DataFormat::Arrow,
            },
        );
    }

    #[test]
    fn parses_time_buckets() {
        let bucket = |parts: &[u32], format| KafkaPath::Bucket {
            topic: "orders".into(),
            bucket: Bucket::Calendar(parts.to_vec()),
            format,
        };
        let time = |path, expected| round_trip(KafkaView::Time, path, expected);
        time("orders/2026", bucket(&[2026], None));
        time("orders/2026/11/17", bucket(&[2026, 11, 17], None));
        time(
            "kafka+time://orders/2026/11.parquet",
            bucket(&[2026, 11], Some(DataFormat::Parquet)),
        );
        time(
            "orders/2026/11/17/05/30/12/250.arrow",
            bucket(&[2026, 11, 17, 5, 30, 12, 250], Some(DataFormat::Arrow)),
        );
        time("orders/weeks", KafkaPath::Weeks("orders".into()));
        time(
            "orders/weeks/2026-W47.csv",
            KafkaPath::Bucket {
                topic: "orders".into(),
                bucket: Bucket::Week(2026, 47),
                format: Some(DataFormat::Csv),
            },
        );
    }

    #[test]
    fn buckets_span_their_level() {
        let span = |bucket: Bucket| (bucket.start_ms(), bucket.end_ms());
        let ms = |text: &str| {
            DateTime::parse_from_rfc3339(text)
                .unwrap()
                .timestamp_millis()
        };
        assert_eq!(
            span(Bucket::Calendar(vec![2026])),
            (ms("2026-01-01T00:00:00Z"), ms("2027-01-01T00:00:00Z"))
        );
        assert_eq!(
            span(Bucket::Calendar(vec![2026, 12])),
            (ms("2026-12-01T00:00:00Z"), ms("2027-01-01T00:00:00Z"))
        );
        assert_eq!(
            span(Bucket::Calendar(vec![2026, 11, 17, 5, 30, 12, 250])),
            (
                ms("2026-11-17T05:30:12.250Z"),
                ms("2026-11-17T05:30:12.251Z")
            )
        );
        assert_eq!(
            span(Bucket::Week(2026, 1)),
            (ms("2025-12-29T00:00:00Z"), ms("2026-01-05T00:00:00Z"))
        );
        let at = ms("2026-11-17T05:30:12.250Z");
        assert_eq!(Bucket::year_at(at), Some(Bucket::Calendar(vec![2026])));
        assert_eq!(
            Bucket::Calendar(vec![2026, 11, 17, 5, 30, 12]).child_at(at),
            Some(Bucket::Calendar(vec![2026, 11, 17, 5, 30, 12, 250]))
        );
        assert_eq!(Bucket::week_at(at), Some(Bucket::Week(2026, 47)));
    }

    #[test]
    fn parses_keys() {
        let records = |key: &str| KafkaPath::Records {
            topic: "state".into(),
            key: key.into(),
        };
        let keys = |path, expected| round_trip(KafkaView::Records, path, expected);
        keys("kafka+records://state/ns/a%2Fb", records("ns/a%2Fb"));
        keys("state/0/10-19.arrow", records("0/10-19.arrow"));
        assert_eq!(
            KafkaPath::parse("state//ns/key/", KafkaView::Records).unwrap(),
            records("ns/key")
        );
        round_trip(
            KafkaView::History,
            "kafka+history://state/ns/a.arrow",
            KafkaPath::History {
                topic: "state".into(),
                key: "ns/a.arrow".into(),
            },
        );
    }

    #[test]
    fn accepts_extension_aliases() {
        assert_eq!(
            KafkaPath::parse("orders.ndjson", KafkaView::Offset).unwrap(),
            KafkaPath::TopicSnapshot {
                topic: "orders".into(),
                format: DataFormat::JsonLines,
            }
        );
    }

    #[test]
    fn rejects_invalid_paths() {
        let offset = [
            "bad topic",
            "orders/-1",
            "orders/x",
            "orders/_meta",
            "orders/_meta/config.json",
            "orders/0/10-5.arrow",
            "orders/0/10.arrow",
            "orders/0/a-b.arrow",
            "orders/0/10-20.txt",
            "orders/0/10-20.arrow/extra",
            "orders/0/@2026-11-01T00:00:00Z.arrow",
            "orders?start=1",
            "orders/0.arrow?offset=1",
            "orders/0.arrow?limit=-1",
            "orders/0/0-9.arrow?limit=1",
        ];
        let time = [
            "orders/26",
            "orders/2026/13",
            "orders/2026/2/01",
            "orders/2026/02/30",
            "orders/2026/11/17/24",
            "orders/2026/11/17/05/30/60",
            "orders/2026/11/17/05/30/12/1000",
            "orders/2026/11/17/05/30/12/250/1",
            "orders/2026.txt",
            "orders/weeks/2026-W54",
            "orders/weeks/2026-47",
            "orders/weeks.arrow",
            "orders/2026?start=1",
        ];
        for (view, paths) in [
            (KafkaView::Offset, &offset[..]),
            (KafkaView::Time, &time[..]),
            (KafkaView::Records, &["state/key?start=1"][..]),
        ] {
            for path in paths {
                assert!(
                    matches!(
                        KafkaPath::parse(path, view),
                        Err(FsError::InvalidArgument(_))
                    ),
                    "{view:?} {path}"
                );
            }
        }
    }
}
