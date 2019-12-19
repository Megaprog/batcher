use std::io;
use std::fmt::Display;
use std::marker::PhantomData;
use miniz_oxide::deflate::{compress_to_vec, CompressionLevel};
use std::ops::Deref;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct BinaryBatch {
    pub batch_id: i64,
    pub bytes: Vec<u8>
}

pub trait BatchFactory<R>: Clone + Send + 'static {
    fn create_batch(&self, records: R, batch_id: i64) -> io::Result<BinaryBatch>;
}

impl<R: 'static> BatchFactory<R> for fn(R, i64) -> io::Result<BinaryBatch> {
    fn create_batch(&self, records: R, batch_id: i64) -> io::Result<BinaryBatch> {
        self(records, batch_id)
    }
}

pub trait BatchStorage<B: Deref<Target=BinaryBatch>>: Clone + Send + 'static {
    fn store<R>(&mut self, records: R, batch_factory: impl BatchFactory<R>) -> io::Result<()>;
    fn get(&self) -> io::Result<B>;
    fn remove(&mut self) -> io::Result<()>;
    fn is_persistent(&self) -> bool;
    fn is_empty(&self) -> bool;
    fn shutdown(self);
}

#[derive(Clone, Debug)]
pub struct GzippedDisplayBatchFactory<T> {
    server_id: String,
    phantom: PhantomData<T>
}

impl<T> GzippedDisplayBatchFactory<T> {
    pub fn new(server_id: String) -> Self {
        GzippedDisplayBatchFactory {
            server_id,
            phantom: PhantomData
        }
    }
}

impl<R: Display + Clone + Send + 'static> BatchFactory<R> for GzippedDisplayBatchFactory<R> {
    fn create_batch(&self, records: R, batch_id: i64) -> io::Result<BinaryBatch> {
        Ok(BinaryBatch {
            batch_id,
            bytes: compress_to_vec(
                &format!(r#"{{"serverId":{},"batchId":{},"batch":{}}}"#, self.server_id, batch_id, records).into_bytes(),
                CompressionLevel::BestCompression as u8)})
    }
}

pub fn time_from_epoch_millis() -> i64 {
    time_from_epoch().as_millis() as i64
}

pub fn time_from_epoch() -> Duration {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("Backward time")
}


#[cfg(test)]
mod test {
    use miniz_oxide::inflate::decompress_to_vec;
    use crate::batch_storage::{GzippedDisplayBatchFactory, BatchFactory};

    #[test]
    fn test_gzipped_batch_factory() {
        let batch_factory = GzippedDisplayBatchFactory::new("server_1".to_string());
        let batch = batch_factory.create_batch(r#"["action1", "action2"]"#, 1).unwrap();
        let decompressed = String::from_utf8(decompress_to_vec(&batch.bytes).unwrap()).unwrap();
        assert_eq!(r#"{"serverId":server_1,"batchId":1,"batch":["action1", "action2"]}"#, decompressed)
    }
}
