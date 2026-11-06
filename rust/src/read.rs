use std::cmp::min;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryBuilder, Int32Builder, Int64Builder, MapBuilder, RecordBatch,
    StringBuilder, TimestampMillisecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use fsspec_data::{CodecWriter, DataFormat, DEFAULT_REGISTRY};
use fsspec_rs::{FileInfo, FsError, FsFile, FsResult};

use crate::backend::{Message, MessageStream};

/// Arrow schema of message files. CSV cannot encode maps, so CSV files leave out `headers`.
pub fn message_schema(format: DataFormat) -> SchemaRef {
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
    fields.push(Field::new("value", DataType::Binary, true));
    Arc::new(Schema::new(fields))
}

fn headers_builder() -> MapBuilder<StringBuilder, BinaryBuilder> {
    MapBuilder::new(None, StringBuilder::new(), BinaryBuilder::new())
}

/// Groups messages into record batches of at most `batch_size` rows.
pub struct MessageBatches {
    topic: String,
    messages: MessageStream,
    schema: SchemaRef,
    batch_size: usize,
}

impl MessageBatches {
    pub fn new(
        topic: &str,
        messages: MessageStream,
        format: DataFormat,
        batch_size: usize,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            messages,
            schema: message_schema(format),
            batch_size,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn batch(&self, messages: &[Message]) -> FsResult<RecordBatch> {
        message_batch(&self.topic, &self.schema, messages)
    }
}

/// `messages` as one record batch with `schema`, a `message_schema`.
pub(crate) fn message_batch(
    topic_name: &str,
    schema: &SchemaRef,
    messages: &[Message],
) -> FsResult<RecordBatch> {
    let mut topic = StringBuilder::new();
    let mut partition = Int32Builder::new();
    let mut offset = Int64Builder::new();
    let mut timestamp = TimestampMillisecondBuilder::new().with_timezone("UTC");
    let mut key = BinaryBuilder::new();
    let mut headers = headers_builder();
    let mut value = BinaryBuilder::new();
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
        value.append_option(message.value.as_deref());
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
    columns.push(Arc::new(value.finish()));
    RecordBatch::try_new(schema.clone(), columns).map_err(arrow_error)
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

fn arrow_error(error: arrow::error::ArrowError) -> FsError {
    FsError::Other(error.to_string())
}
