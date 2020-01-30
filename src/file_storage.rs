use std::collections::VecDeque;
use std::sync::Arc;
use crate::batch_storage::{BinaryBatch, BatchStorage, BatchFactory};
use crate::waiter::{Lock, Waiter};
use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::{io, fs, fmt};
use log::*;
use std::io::{Error, ErrorKind, BufRead, Write, Seek, SeekFrom};
use std::fmt::{Display, Formatter, Debug};

macro_rules! batch_file {
    () => ( "batchFile" )
}
macro_rules! file_id_pattern {
    () => ( "{:011}" )
}

static BATCH_FILE: &str = batch_file!();
static BATCH_FILE_NAME_PREFIX: &str = concat!(batch_file!(), "-");
static LAST_BATCH_ID_FILE_NAME: &str = "nextBatchId";

const DEFAULT_MAX_BYTES_IN_FILES: u64 = std::u64::MAX;

pub struct FileStorageSharedState {
    pub(crate) file_ids: VecDeque<i64>,
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
                .map(|dir_entry| dir_entry.path().is_file()).unwrap_or(true) )
            .map(|dir_entry_res| dir_entry_res
                .map(|dir_entry| dir_entry.file_name())
                .map(|file_name| file_name.to_string_lossy().into_owned()))
            .filter(|file_name_res| file_name_res.as_ref()
                .map(|file_name| file_name.starts_with(BATCH_FILE_NAME_PREFIX)).unwrap_or(true))
            .map(|file_name_res| file_name_res
                .and_then(|file_name| fs::metadata(&file_name).map(|meta| (file_name, meta.len()))))
            .map(|result| result
                .and_then(|name_meta| FileStorage::batch_id(&name_meta.0).map(|id| (id, name_meta.1))))
            .collect::<Result<Vec<_>, io::Error>>()?;

        let batch_id_file = OpenOptions::new().read(true).write(true).create(true)
            .open(path.join(LAST_BATCH_ID_FILE_NAME))?;

        let next_batch_id = {
            let mut lines = io::BufReader::new(batch_id_file).lines();
            lines.next().map(|result| result.and_then(|s| s.parse::<i64>()
                .map_err(|e| Error::new(ErrorKind::InvalidData,
                                        format!("Can't parse last batch id value '{}' from file {}", s, e)))))
                .unwrap_or(Ok(ids_and_sizes.last().map(|id_size| id_size.0).unwrap_or(0)))?
        };

        debug!("Init next batch id with {}", next_batch_id);
        debug!("Initialized {} batches.", ids_and_sizes.len());

        ids_and_sizes.sort();
        let (file_ids, sizes) = ids_and_sizes.into_iter().unzip::<_, _, VecDeque<_>, Vec<_>>();

        Ok(FileStorage {
            path,
            max_bytes,
            shared_state: Arc::new(Lock::new(FileStorageSharedState {
                file_ids,
                occupied_bytes: sizes.into_iter().sum(),
                next_batch_id,
                batch_id_file,
                stopped: false,
            })),
        })
    }

    fn batch_id(batch_file_name: &str) -> io::Result<i64> {
        batch_file_name[..BATCH_FILE_NAME_PREFIX.len()].parse::<i64>()
            .map_err(|e|
                Error::new(ErrorKind::InvalidData, format!("Can't parse file name '{}' got {}", batch_file_name, e)))
    }

    fn is_capacity_exceeded(&self, batch: &BinaryBatch, waiter: &Waiter<FileStorageSharedState>) -> bool {
        waiter.occupied_bytes + batch.bytes.len() as u64 > self.max_bytes
    }

    fn batch_file_path(&self, file_id: i64) -> PathBuf {
        self.path.join(BATCH_FILE_NAME_PREFIX.to_owned() + &format!(file_id_pattern!(), file_id))
    }

    fn increment_next_batch_id(&self, waiter: &mut Waiter<FileStorageSharedState>) -> io::Result<()> {
        waiter.next_batch_id += 1;
        waiter.batch_id_file.seek(SeekFrom::Start(0))?;
        let batch_id_str = format!("{}", waiter.next_batch_id);
        waiter.batch_id_file.write_all(batch_id_str.as_bytes())
    }
}

impl BatchStorage<Box<BinaryBatch>> for FileStorage {
    fn store<R>(&self, records: R, batch_factory: &impl BatchFactory<R>) -> Result<(), Error> {
        let mut waiter = self.shared_state.lock();
        if waiter.stopped {
            return Err(Error::new(ErrorKind::Interrupted, format!("The storage {:?} has been shut down", self)))
        }

        let next_batch_id = waiter.next_batch_id;
        let batch = batch_factory.create_batch(records, next_batch_id)?;

        while self.is_capacity_exceeded(&batch, &waiter) {
            waiter.wait()?;
        }

        let batch_file_path = self.batch_file_path(next_batch_id);
        OpenOptions::new().write(true).create_new(true).open(&batch_file_path)?.write_all(&batch.bytes)?;

        waiter.file_ids.push_back(next_batch_id);

        waiter.notify_one();

        Ok(())
    }

    fn get(&self) -> Result<Box<BinaryBatch>, Error> {
        unimplemented!()
    }

    fn remove(&self) -> Result<(), Error> {
        unimplemented!()
    }

    fn is_persistent(&self) -> bool {
        unimplemented!()
    }

    fn is_empty(&self) -> bool {
        unimplemented!()
    }

    fn shutdown(self) {
        unimplemented!()
    }
}
