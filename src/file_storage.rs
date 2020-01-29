use std::collections::VecDeque;
use std::sync::Arc;
use crate::batch_storage::BinaryBatch;
use crate::waiter::Lock;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::{io, fs};
use log::*;
use std::ops::Deref;
use std::io::{Error, ErrorKind, BufRead};
use std::str::FromStr;
use std::ffi::OsStr;

macro_rules! batch_file {
    () => ( "batchFile" )
}

static BATCH_FILE: &str = batch_file!();
static BATCH_FILE_NAME_PREFIX: &str = concat!(batch_file!(), "-");
static DIGITAL_FORMAT: &str = "{:011}";
static LAST_BATCH_ID_FILE_NAME: &str = "lastBatchId";

const DEFAULT_MAX_BYTES_IN_FILES: u128 = std::u128::MAX;

pub struct FileStorageSharedState {
    pub(crate) file_ids: VecDeque<i64>,
    occupied_bytes: u128,
    last_batch_id: i64,
    stopped: bool,
}

#[derive(Clone)]
pub struct FileStorage {
    path: PathBuf,
    batch_id_file: PathBuf,
    pub max_bytes: u128,
    pub(crate) shared_state: Arc<Lock<FileStorageSharedState>>
}

impl FileStorage {
    pub fn init(path: impl Into<PathBuf>) -> io::Result<FileStorage> {
        FileStorage::init_with_max_bytes(path, DEFAULT_MAX_BYTES_IN_FILES)
    }

    pub fn init_with_max_bytes(path: impl Into<PathBuf>, max_bytes: u128) -> io::Result<FileStorage> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        if !path.is_dir() {
            return Err(Error::new(ErrorKind::NotFound, format!("The path {:?} is not a directory", path)))
        }

        let mut file_ids = fs::read_dir(&path)?
            .filter(|dir_entry_res| dir_entry_res.as_ref()
                .map(|dir_entry| dir_entry.path().is_file()).unwrap_or(true) )
            .map(|dir_entry_res| dir_entry_res
                .map(|dir_entry| dir_entry.file_name())
                .map(|file_name| file_name.to_string_lossy().into_owned()))
            .filter(|file_name_res| file_name_res.as_ref()
                .map(|file_name| file_name.starts_with(BATCH_FILE_NAME_PREFIX)).unwrap_or(true))
            .map(|res| res.and_then(|file_name| FileStorage::batch_id(&file_name)))
            .collect::<Result<Vec<_>, io::Error>>()?;

        file_ids.sort();

        let batch_id_file = path.join(LAST_BATCH_ID_FILE_NAME);

        let last_batch_id = {
            let file = fs::OpenOptions::new().read(true).create(true).open(&batch_id_file)?;
            let mut lines = io::BufReader::new(file).lines();
            lines.next().map(|result| result.and_then(|s| s.parse::<i64>()
                .map_err(|e| Error::new(ErrorKind::InvalidData,
                                        format!("Can't parse last batch id value '{}' from file {}", s, e)))))
                .unwrap_or(Ok(0))?
        };

        debug!("Init lastBatchId with {}", last_batch_id);
        debug!("Initialized {} batches.", file_ids.len());

        Ok(FileStorage {
            path,
            batch_id_file,
            max_bytes,
            shared_state: Arc::new(Lock::new(FileStorageSharedState {
                file_ids: file_ids.into(),
                occupied_bytes: 0,
                last_batch_id,
                stopped: false,
            })),
        })
    }

    fn batch_id(batch_file_name: &str) -> io::Result<i64> {
        batch_file_name[..BATCH_FILE_NAME_PREFIX.len()].parse::<i64>()
            .map_err(|e|
                Error::new(ErrorKind::InvalidData, format!("Can't parse file name '{}' got {}", batch_file_name, e)))
    }
}

