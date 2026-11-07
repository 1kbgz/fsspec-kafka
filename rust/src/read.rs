use std::cmp::min;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BinaryBuilder, Int32Builder, Int64Builder, MapBuilder,
    RecordBatch, StringBuilder, StructArray, TimestampMillisecondBuilder,
};
use arrow::compute::{concat_batches, interleave};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use fsspec_data::{CodecWriter, DataFormat, DEFAULT_REGISTRY};
use fsspec_rs::{FileInfo, FsError, FsFile, FsResult};

use crate::backend::{Message, MessageStream};

/// How message values become columns.
#[derive(Clone, Debug, Default, PartialEq)]
pub enum ValueFormat {
    /// `value: binary`.
    #[default]
    Binary,
    /// JSON objects decoded with `schema`, nested in a `value` struct column or, with
    /// `flatten`, placed beside the envelope columns. Decoded fields are nullable because
    /// tombstones have no value.
    Json { schema: SchemaRef, flatten: bool },
    /// JSON objects in Confluent wire format, where a zero magic byte and a big-endian
    /// schema ID precede each value. Reads accept any schema ID. Appends write `schema_id`,
    /// so appending needs one.
    ConfluentJson {
        schema: SchemaRef,
        flatten: bool,
        schema_id: Option<u32>,
    },
}

impl ValueFormat {
    /// The schema and `flatten` setting of JSON values, or `None` for binary values.
    pub(crate) fn json(&self) -> Option<(&SchemaRef, bool)> {
        match self {
            Self::Binary => None,
            Self::Json { schema, flatten }
            | Self::ConfluentJson {
                schema, flatten, ..
            } => Some((schema, *flatten)),
        }
    }
}

/// Arrow schema of message files. CSV cannot encode maps, so CSV files leave out `headers`.
pub fn message_schema(format: DataFormat, value: &ValueFormat) -> SchemaRef {
    let mut fields = vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("key", DataType::Binary, true),
    ];
    if format != DataFormat::Csv {
        fields.push(Field::new(
            "headers",
            headers_builder().finish().data_type().clone(),
            false,
        ));
    }
    match value.json() {
        None => fields.push(Field::new("value", DataType::Binary, true)),
        Some((schema, false)) => fields.push(Field::new(
            "value",
            DataType::Struct(nullable_fields(schema)),
            true,
        )),
        Some((schema, true)) => {
            fields.extend(nullable_fields(schema).iter().map(|f| f.as_ref().clone()))
        }
    }
    Arc::new(Schema::new(fields))
}

fn nullable_fields(schema: &Schema) -> Fields {
    schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect()
}

fn headers_builder() -> MapBuilder<StringBuilder, BinaryBuilder> {
    MapBuilder::new(None, StringBuilder::new(), BinaryBuilder::new())
}

/// Groups messages into record batches of at most `batch_size` rows.
pub struct MessageBatches {
    topic: String,
    messages: MessageStream,
    value: ValueFormat,
    schema: SchemaRef,
    batch_size: usize,
}

impl MessageBatches {
    pub fn new(
        topic: &str,
        messages: MessageStream,
        format: DataFormat,
        value: ValueFormat,
        batch_size: usize,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            messages,
            schema: message_schema(format, &value),
            value,
            batch_size,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn batch(&self, messages: &[Message]) -> FsResult<RecordBatch> {
        message_batch(&self.topic, &self.schema, &self.value, messages)
    }
}

/// `messages` as one record batch with `schema`, the `message_schema` for `value`.
pub(crate) fn message_batch(
    topic_name: &str,
    schema: &SchemaRef,
    value: &ValueFormat,
    messages: &[Message],
) -> FsResult<RecordBatch> {
    let mut topic = StringBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();
    let mut timestamp = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut key = BinaryBuilder::new();
    let mut headers = headers_builder();
    for message in messages {
        topic.append_value(topic_name);
        partition.append_value(message.partition);
        offset.append_value(message.offset);
        timestamp.append_option(message.timestamp_ms);
        key.append_option(message.key.as_deref());
        for (name, header) in &message.headers {
            headers.keys().append_value(name);
            headers.values().append_option(header.as_deref());
        }
        headers.append(true).map_err(arrow_error)?;
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(topic.finish()),
        Arc::new(partition.finish()),
        Arc::new(offset.finish()),
        Arc::new(timestamp.finish()),
        Arc::new(key.finish()),
    ];
    if schema.column_with_name("headers").is_some() {
        columns.push(Arc::new(headers.finish()));
    }
    let mut values = messages
        .iter()
        .map(|message| message.value.as_deref())
        .collect::<Vec<_>>();
    if let ValueFormat::ConfluentJson { .. } = value {
        for value in values.iter_mut().flatten() {
            *value = confluent_payload(value)?;
        }
    }
    match value.json() {
        None => columns.push(Arc::new(BinaryArray::from(values))),
        Some((schema, flatten)) => {
            let decoded = decode_json(schema, &values)?;
            if flatten {
                columns.extend_from_slice(decoded.columns());
            } else {
                columns.push(Arc::new(decoded));
            }
        }
    }
    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_error)
}

/// The JSON after a Confluent wire format prefix.
fn confluent_payload(value: &[u8]) -> FsResult<&[u8]> {
    match value {
        [0, _, _, _, _, payload @ ..] => Ok(payload),
        _ => Err(FsError::Other(
            "value is not in Confluent wire format".to_string(),
        )),
    }
}

/// Decodes one struct row per value, with null rows for tombstones. Values are decoded
/// together as JSON lines, except values containing a raw newline, which are decoded one at
/// a time.
fn decode_json(schema: &Schema, values: &[Option<&[u8]>]) -> FsResult<StructArray> {
    let fields = nullable_fields(schema);
    let decode = |data: &[u8], rows: usize| -> FsResult<StructArray> {
        let schema = Arc::new(Schema::new(fields.clone()));
        let decoded = DEFAULT_REGISTRY
            .get(DataFormat::JsonLines)
            .and_then(|codec| codec.decode(data, Some(schema.clone())))
            .map_err(|error| FsError::Other(format!("invalid JSON value: {error}")))?;
        let batch = concat_batches(&schema, &decoded.batches).map_err(arrow_error)?;
        if batch.num_rows() != rows {
            return Err(FsError::Other(format!(
                "{rows} JSON values decoded to {} rows; each value must be one JSON object",
                batch.num_rows()
            )));
        }
        Ok(batch.into())
    };
    // Source 0 is a null row, source 1 the joined values, then one per separate value.
    let mut sources = vec![StructArray::new_null(fields.clone(), 1)];
    let mut joined = Vec::new();
    let mut joined_rows = 0;
    let mut indices = Vec::with_capacity(values.len());
    let mut separate = Vec::new();
    for value in values {
        match value {
            None => indices.push((0, 0)),
            Some(value) if value.contains(&b'\n') => {
                separate.push(decode(value, 1)?);
                indices.push((1 + separate.len(), 0));
            }
            Some(value) => {
                joined.extend_from_slice(value);
                joined.push(b'\n');
                indices.push((1, joined_rows));
                joined_rows += 1;
            }
        }
    }
    sources.push(decode(&joined, joined_rows)?);
    sources.extend(separate);
    let arrays = sources.iter().map(|s| s as &dyn Array).collect::<Vec<_>>();
    Ok(interleave(&arrays, &indices)
        .map_err(arrow_error)?
        .as_struct()
        .clone())
}

impl Iterator for MessageBatches {
    type Item = FsResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut messages = Vec::new();
        while messages.len() < self.batch_size {
            match self.messages.next() {
                Some(Ok(message)) => messages.push(message),
                Some(Err(error)) => return Some(Err(error)),
                None => break,
            }
        }
        (!messages.is_empty()).then(|| self.batch(&messages))
    }
}

#[derive(Clone, Default)]
struct SharedBuffer {
    inner: Arc<Mutex<Vec<u8>>>,
}

impl SharedBuffer {
    fn take(&self) -> io::Result<Vec<u8>> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("stream buffer lock is poisoned"))?;
        Ok(std::mem::take(&mut *guard))
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut guard = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("stream buffer lock is poisoned"))?;
        guard.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// A read-only file that encodes message batches only as far as reads and seeks require.
pub struct EncodedFile {
    batches: MessageBatches,
    writer: Option<Box<dyn CodecWriter>>,
    sink: SharedBuffer,
    emitted: Vec<u8>,
    position: u64,
    info: FileInfo,
    eof: bool,
}

impl EncodedFile {
    pub fn new(batches: MessageBatches, format: DataFormat, info: FileInfo) -> FsResult<Self> {
        let sink = SharedBuffer::default();
        let writer = DEFAULT_REGISTRY
            .get(format)
            .and_then(|codec| codec.start_owned_writer(batches.schema(), Box::new(sink.clone())))
            .map_err(|error| FsError::Other(error.to_string()))?;
        let mut file = Self {
            batches,
            writer: Some(writer),
            sink,
            emitted: Vec::new(),
            position: 0,
            info,
            eof: false,
        };
        file.drain_sink()?;
        Ok(file)
    }

    fn drain_sink(&mut self) -> io::Result<()> {
        self.emitted.extend_from_slice(&self.sink.take()?);
        Ok(())
    }

    fn fill_until(&mut self, target_len: Option<usize>) -> io::Result<()> {
        while !self.eof && target_len.is_none_or(|target| self.emitted.len() < target) {
            match self.batches.next() {
                Some(Ok(batch)) => {
                    self.writer
                        .as_mut()
                        .expect("writer is present until eof")
                        .write_batch(&batch)
                        .map_err(io::Error::other)?;
                    self.drain_sink()?;
                }
                Some(Err(error)) => return Err(io::Error::other(error)),
                None => {
                    if let Some(writer) = self.writer.take() {
                        writer.finish().map_err(io::Error::other)?;
                    }
                    self.eof = true;
                    self.drain_sink()?;
                }
            }
        }
        Ok(())
    }
}

impl Read for EncodedFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let start = usize::try_from(self.position).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "file position is too large")
        })?;
        self.fill_until(Some(start.saturating_add(buf.len())))?;
        if start >= self.emitted.len() {
            return Ok(0);
        }
        let count = min(buf.len(), self.emitted.len() - start);
        buf[..count].copy_from_slice(&self.emitted[start..start + count]);
        self.position += count as u64;
        Ok(count)
    }
}

impl Write for EncodedFile {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "file is read-only",
        ))
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for EncodedFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = match pos {
            SeekFrom::Start(position) => Some(position),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
            SeekFrom::End(offset) => {
                self.fill_until(None)?;
                (self.emitted.len() as u64).checked_add_signed(offset)
            }
        }
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid seek position"))?;
        if let Ok(target) = usize::try_from(position) {
            self.fill_until(Some(target))?;
        }
        self.position = position;
        Ok(position)
    }
}

impl FsFile for EncodedFile {
    fn info(&self) -> FsResult<FileInfo> {
        let mut info = self.info.clone();
        if self.eof {
            info.size = self.emitted.len() as u64;
            info.extra.insert("size_known".into(), "true".into());
        }
        Ok(info)
    }

    fn size(&self) -> FsResult<Option<u64>> {
        Ok(self.eof.then_some(self.emitted.len() as u64))
    }
}

pub(crate) fn arrow_error(error: arrow::error::ArrowError) -> FsError {
    FsError::Other(error.to_string())
}

#[cfg(test)]
mod tests {
    use arrow::array::{AsArray, Int64Array, StringArray};
    use arrow::datatypes::Int64Type;

    use super::*;

    fn message(offset: i64, value: Option<&str>) -> Message {
        Message {
            partition: 0,
            offset,
            timestamp_ms: None,
            key: None,
            headers: Vec::new(),
            value: value.map(|value| value.as_bytes().to_vec()),
        }
    }

    fn json(flatten: bool) -> ValueFormat {
        ValueFormat::Json {
            schema: Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            flatten,
        }
    }

    fn batch(value: &ValueFormat, values: &[Option<&str>]) -> FsResult<RecordBatch> {
        let messages = values
            .iter()
            .enumerate()
            .map(|(offset, value)| message(offset as i64, *value))
            .collect::<Vec<_>>();
        let schema = message_schema(DataFormat::Arrow, value);
        message_batch("orders", &schema, value, &messages)
    }

    #[test]
    fn json_values_keep_message_order() {
        let values = [
            Some(r#"{"id": 1, "name": "a"}"#),
            None,
            Some("{\n  \"id\": 2,\n  \"name\": \"b\"\n}\n"),
            Some(r#"{"id": 3, "name": "c", "extra": true}"#),
        ];
        let nested = batch(&json(false), &values).unwrap();
        let value = nested.column_by_name("value").unwrap().as_struct();
        assert_eq!(value.null_count(), 1);
        assert!(value.is_null(1));
        assert_eq!(
            value
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(1), None, Some(2), Some(3)])
        );

        let flat = batch(&json(true), &values).unwrap();
        assert_eq!(
            flat.schema(),
            message_schema(DataFormat::Arrow, &json(true))
        );
        assert!(flat.schema().field_with_name("id").unwrap().is_nullable());
        assert_eq!(
            flat.column_by_name("name").unwrap().as_string::<i32>(),
            &StringArray::from(vec![Some("a"), None, Some("b"), Some("c")])
        );
    }

    #[test]
    fn confluent_values_drop_the_wire_format_prefix() {
        let ValueFormat::Json { schema, flatten } = json(true) else {
            unreachable!()
        };
        let confluent = ValueFormat::ConfluentJson {
            schema,
            flatten,
            schema_id: None,
        };
        let decoded = batch(&confluent, &[Some("\0\0\0\0\x07{\"id\": 1}"), None]).unwrap();
        assert_eq!(
            decoded
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>(),
            &Int64Array::from(vec![Some(1), None])
        );
        for value in [r#"{"id": 1}"#, "\0\0\0"] {
            assert!(batch(&confluent, &[Some(value)]).is_err(), "{value:?}");
        }
    }

    #[test]
    fn values_that_are_not_one_json_object_fail() {
        for value in [r#"{"id": 1}{"id": 2}"#, "", "not json", "{\"id\": \n"] {
            assert!(
                batch(&json(false), &[Some(r#"{"id": 0}"#), Some(value)]).is_err(),
                "{value:?}"
            );
        }
    }
}
