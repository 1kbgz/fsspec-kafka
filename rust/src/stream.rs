use std::collections::BTreeMap;
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fsspec_data::DataFormat;
use fsspec_rs::FsResult;

use crate::backend::LiveMessages;
use crate::read::{message_batch, message_schema, ValueFormat};

/// Where `KafkaFs::stream` starts reading each partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamStart {
    /// The oldest retained message.
    Beginning,
    /// The next message produced.
    End,
    /// The first message at or after this time in milliseconds since the epoch.
    Time(i64),
    /// The next offset to read in each partition, such as `MessageTail::positions` from an
    /// earlier stream. Partitions left out start at the beginning.
    Offsets(BTreeMap<i32, i64>),
}

/// Messages from followed partitions as Arrow record batches.
pub struct MessageTail {
    topic: String,
    messages: Box<dyn LiveMessages>,
    value: ValueFormat,
    schema: SchemaRef,
    batch_size: usize,
    positions: BTreeMap<i32, i64>,
}

impl MessageTail {
    pub(crate) fn new(
        topic: String,
        messages: Box<dyn LiveMessages>,
        value: ValueFormat,
        batch_size: usize,
        positions: BTreeMap<i32, i64>,
    ) -> Self {
        Self {
            topic,
            messages,
            schema: message_schema(DataFormat::Arrow, &value),
            value,
            batch_size,
            positions,
        }
    }

    /// The schema of every batch, the same as `.arrow` files.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The next offset to read in each partition. Passing these as `StreamStart::Offsets`
    /// resumes after the last message returned.
    pub fn positions(&self) -> &BTreeMap<i32, i64> {
        &self.positions
    }

    /// Waits up to `timeout` for a message, then returns it with any others already
    /// received, up to the batch size. `None` means no message arrived in time.
    pub fn poll(&mut self, timeout: Duration) -> FsResult<Option<RecordBatch>> {
        let Some(first) = self.messages.poll(timeout)? else {
            return Ok(None);
        };
        let mut messages = vec![first];
        while messages.len() < self.batch_size {
            match self.messages.poll(Duration::ZERO)? {
                Some(message) => messages.push(message),
                None => break,
            }
        }
        for message in &messages {
            self.positions.insert(message.partition, message.offset + 1);
        }
        message_batch(&self.topic, &self.schema, &self.value, &messages).map(Some)
    }
}
