use std::collections::VecDeque;
use std::sync::Arc;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
use crate::waiter::{Lock, Waiter};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::{io, fs, fmt};
use log::*;
use std::io::{Error, ErrorKind, BufRead, Write, Seek, SeekFrom, Read};
use std::fmt::{Display, Formatter, Debug};

macro_rules! batch_file {
    () => ( "batchFile" )
}
macro_rules! file_id_pattern {
    () => ( "{:011}" )
}

static BATCH_FILE: &str = batch_file!();
static BATCH_FILE_NAME_PREFIX: &str = concat!(batch_file!(), "-");
static NEXT_BATCH_ID_FILE_NAME: &str = "nextBatchId";

const DEFAULT_MAX_BYTES_IN_FILES: u64 = std::u64::MAX;

pub struct FileStorageSharedState {
    pub(crate) file_ids: VecDeque<(i64, u64)>,
    occupied_bytes: u64,
    next_batch_id: i64,
    batch_id_file: File,
    stopped: bool,
}

#[derive(Clone)]
pub struct FileStorage {
    path: PathBuf,
    pub max_bytes: u64,
    pub(crate) shared_state: Arc<Lock<FileStorageSharedState>>
}

impl Debug for FileStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileStorage")
            .field("path", &self.path)
            .finish()
    }
}

impl FileStorage {
    pub fn init(path: impl Into<PathBuf>) -> io::Result<FileStorage> {
        FileStorage::init_with_max_bytes(path, DEFAULT_MAX_BYTES_IN_FILES)
    }

    pub fn init_with_max_bytes(path: impl Into<PathBuf>, max_bytes: u64) -> io::Result<FileStorage> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        if !path.is_dir() {
            return Err(Error::new(ErrorKind::NotFound, format!("The path {:?} is not a directory", path)))
        }

        let mut ids_and_sizes = fs::read_dir(&path)?
            .filter(|dir_entry_res| dir_entry_res.as_ref()
                .map(|dir_entry| dir_entry.path().is_file()).unwrap_or(true))
            .map(|dir_entry_res| dir_entry_res
                .map(|dir_entry| (dir_entry.file_name(), dir_entry.path()))
                .map(|file_name| (file_name.0.to_string_lossy().into_owned(), file_name.1)))
            .filter(|file_name_res| file_name_res.as_ref()
                .map(|file_name| file_name.0.starts_with(BATCH_FILE_NAME_PREFIX)).unwrap_or(true))
            .map(|file_name_res| file_name_res
                .and_then(|file_name| fs::metadata(&file_name.1).map(|meta| (file_name.0, meta.len()))))
            .map(|result| result
                .and_then(|name_meta| FileStorage::batch_id(&name_meta.0).map(|id| (id, name_meta.1))))
            .collect::<Result<Vec<_>, io::Error>>()?;

        let batch_id_path = path.join(NEXT_BATCH_ID_FILE_NAME);
        let mut batch_id_file = OpenOptions::new().read(true).write(true).create(true)
            .open(&batch_id_path)?;

        let next_batch_id = if batch_id_file.metadata()?.len() == 0 {
            ids_and_sizes.last().map(|id_size| id_size.0 + 1).unwrap_or(1)
        } else {
            let mut buffer = String::new();
            batch_id_file.read_to_string(&mut buffer)?;
            buffer.parse::<i64>()
                .map_err(|e| Error::new(ErrorKind::InvalidData,
                                        format!("Cannot parse last batch id value {} from the file {:?}: {}",
                                                &buffer, batch_id_path, e)))?
        };

        debug!("Init next batch id with {}", next_batch_id);
        debug!("Initialized {} batches.", ids_and_sizes.len());

        ids_and_sizes.sort();
        let occupied_bytes = ids_and_sizes.iter().map(|id_size| id_size.1).sum();

        Ok(FileStorage {
            path,
            max_bytes,
            shared_state: Arc::new(Lock::new(FileStorageSharedState {
                file_ids: ids_and_sizes.into(),
                occupied_bytes,
                next_batch_id,
                batch_id_file,
                stopped: false,
            })),
        })
    }

    fn batch_id(batch_file_name: &str) -> io::Result<i64> {
        batch_file_name[BATCH_FILE_NAME_PREFIX.len()..].parse::<i64>()
            .map_err(|e|
                Error::new(ErrorKind::InvalidData, format!("Cannot parse file name {} got {}", batch_file_name, e)))
    }

    fn batch_file_path(&self, file_id: i64) -> PathBuf {
        self.path.join(BATCH_FILE_NAME_PREFIX.to_owned() + &format!(file_id_pattern!(), file_id))
    }

    fn increment_next_batch_id(waiter: &mut Waiter<FileStorageSharedState>) -> io::Result<()> {
        waiter.next_batch_id += 1;
        waiter.batch_id_file.seek(SeekFrom::Start(0))?;
        let batch_id_str = format!(file_id_pattern!(), waiter.next_batch_id);
        waiter.batch_id_file.write_all(batch_id_str.as_bytes())
    }

    fn bytes_from_file(path: &Path) -> io::Result<Option<Vec<u8>>> {
        if !path.exists() {
            return Ok(None)
        }

        let mut buffer = Vec::new();
        File::open(path)?.read_to_end(&mut buffer)?;
        if buffer.len() > 0 { Ok(Some(buffer)) } else { Ok(None) }
    }
}

impl BatchStorage<BinaryBatch> for FileStorage {
    fn store<R>(&self, records: R, batch_factory: &impl BatchFactory<R>) -> io::Result<()> {
        let mut waiter = self.shared_state.lock();
        if waiter.stopped {
            return Err(Error::new(ErrorKind::Interrupted, format!("The storage {:?} has been shut down", self)))
        }

        let next_batch_id = waiter.next_batch_id;
        let batch = batch_factory.create_batch(records, next_batch_id)?;
        let number_bytes = batch.bytes.len() as u64;

        if waiter.occupied_bytes + number_bytes > self.max_bytes {
            return Err(Error::new(ErrorKind::Other,
                                  format!("The storage {:?} capacity exceeded: needed {} available {}",
                                          self, number_bytes, self.max_bytes - waiter.occupied_bytes)))
        }

        let batch_file_path = self.batch_file_path(next_batch_id);
        OpenOptions::new().write(true).create_new(true).open(&batch_file_path)?.write_all(&batch.bytes)?;

        waiter.occupied_bytes += number_bytes;
        waiter.file_ids.push_back((next_batch_id, number_bytes));

        FileStorage::increment_next_batch_id(&mut waiter)?;

        waiter.notify_one();

        Ok(())
    }

    fn get(&self) -> io::Result<BinaryBatch> {
        let mut waiter = self.shared_state.lock();

        loop {
            let batch_id_opt = waiter.file_ids.front();
            if batch_id_opt.is_none() {
                waiter.wait()?;
                continue;
            }
            let &batch_id_size = batch_id_opt.unwrap();

            let batch_path = self.batch_file_path(batch_id_size.0);
            let bytes_opt = FileStorage::bytes_from_file(&batch_path)?;
            if let Some(bytes) = bytes_opt {
                return Ok(BinaryBatch { batch_id: batch_id_size.0, bytes });
            }

            warn!("Batch file {:?} is missing or empty", batch_path);

            waiter.file_ids.pop_front();
        }
    }

    fn remove(&self) -> Result<(), Error> {
        let mut waiter = self.shared_state.lock();
        let batch_id_opt = waiter.file_ids.front();
        if let Some(&batch_id_size) = batch_id_opt {
            let batch_path = self.batch_file_path(batch_id_size.0);
            fs::remove_file(batch_path).and_then(|_| {
                waiter.file_ids.pop_front();
                waiter.occupied_bytes -= batch_id_size.1;
                Ok(())
            })
        } else {
            Ok(())
        }
    }

    fn is_persistent(&self) -> bool {
        true
    }

    fn is_empty(&self) -> bool {
        self.shared_state.lock().file_ids.is_empty()
    }

    fn shutdown(self) {
        let mut waiter = self.shared_state.lock();
        waiter.stopped = true;
        waiter.interrupt();
    }
}

#[cfg(test)]
mod test_file_storage {
    use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
    use crate::memory_storage::MemoryStorage;
    use std::{io, thread};
    use std::time::Duration;
    use crate::file_storage::{FileStorage, NEXT_BATCH_ID_FILE_NAME, BATCH_FILE_NAME_PREFIX};
    use tempfile::tempdir;
    use crate::memory_storage::test::BATCH_FACTORY;
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::Path;
    use std::sync::{Once, Arc};
    use env_logger::{Builder, Env};
    use std::sync::atomic::{AtomicBool, Ordering};

    static INIT: Once = Once::new();

    fn init_log() {
        INIT.call_once(|| {
            Builder::from_env(Env::default().default_filter_or("trace")).is_test(true)
                .format_timestamp_millis()
                .init();
        });
    }

    #[test]
    fn zero_next_batch_id() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path());
        assert!(file_storage.is_ok());
        assert_eq!(1, file_storage.unwrap().shared_state.lock().next_batch_id);
    }

    #[test]
    fn none_next_batch_id() {
        let dir = tempdir().unwrap();
        let mut next_batch_id_file = open_next_batch_id_file(&dir,NEXT_BATCH_ID_FILE_NAME);
        next_batch_id_file.write_all(format!(file_id_pattern!(), 2).as_bytes()).unwrap();
        let file_storage = FileStorage::init(dir.path());
        assert!(file_storage.is_ok());
        assert_eq!(2, file_storage.unwrap().shared_state.lock().next_batch_id);
    }

    #[test]
    fn wrong_next_batch_id_file() {
        let dir = tempdir().unwrap();
        let mut next_batch_id_file = open_next_batch_id_file(&dir,NEXT_BATCH_ID_FILE_NAME);
        next_batch_id_file.write_all("abc".as_bytes()).unwrap();
        let file_storage = FileStorage::init(dir.path());
        assert!(file_storage.is_err());
    }

    #[test]
    fn next_batch_id_by_batches() {
        let dir = tempdir().unwrap();
        open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 1));
        open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 2));
        let file_storage = FileStorage::init(dir.path());
        assert!(file_storage.is_ok());
        assert_eq!(3, file_storage.unwrap().shared_state.lock().next_batch_id);
    }

    #[test]
    fn scenario() {
        init_log();
        let dir = tempdir().unwrap();
        let mut f1 = open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 1));
        f1.write_all(&BATCH_FACTORY.create_batch(String::new(), 1).unwrap().bytes).unwrap();
        let mut f2 = open_next_batch_id_file(&dir, format!(concat!(batch_file!(), "-", file_id_pattern!()), 2));
        f2.write_all(&BATCH_FACTORY.create_batch(String::new(), 1).unwrap().bytes).unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();

        let batch = file_storage.get().unwrap();
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.store(String::new(), &BATCH_FACTORY).unwrap();
        let batch = file_storage.get().unwrap();
        //the same batch
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);
        //again
        let batch = file_storage.get().unwrap();
        assert_eq!(1, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.remove();
        let batch = file_storage.get().unwrap();
        assert_eq!(2, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.remove();
        let batch = file_storage.get().unwrap();
        assert_eq!(3, batch.batch_id);
        assert_eq!(vec![1, 2], batch.bytes);

        file_storage.remove();
        assert!(file_storage.shared_state.lock().file_ids.is_empty());
    }

    fn open_next_batch_id_file(dir: impl AsRef<Path>, path: impl AsRef<Path>) -> File {
        OpenOptions::new().write(true).create(true)
            .open(dir.as_ref().join(path)).unwrap()
    }

    #[test]
    fn capacity_exceeded() {
        let dir = tempdir();
        let file_storage = FileStorage::init_with_max_bytes(dir.as_ref().unwrap().path(), 1).unwrap();
        assert!(file_storage.store(String::new(), &BATCH_FACTORY).is_err());
    }

    #[test]
    fn blocks_forever() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();

        let working = Arc::new(AtomicBool::new(true));

        let cloned_storage = file_storage.clone();
        let cloned_working = working.clone();
        let join_handle = thread::spawn(move || {
            cloned_storage.get().unwrap();
            cloned_working.store(false, Ordering::Relaxed);
        });

        thread::sleep(Duration::from_millis(10));

        assert!(working.load(Ordering::Relaxed));
    }

    #[test]
    fn producer_first() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();
        crate::memory_storage::test::producer_first(file_storage);
    }

    #[test]
    fn consumer_first() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();
        crate::memory_storage::test::consumer_first(file_storage);
    }

    #[test]
    fn shutdown() {
        let dir = tempdir().unwrap();
        let file_storage = FileStorage::init(dir.path()).unwrap();
        crate::memory_storage::test::shutdown(file_storage);
    }
}
