use std::collections::VecDeque;
use std::sync::Arc;
use crate::batch_storage::BinaryBatch;
use crate::waiter::Lock;
use std::path::{Path, PathBuf};
use std::fs::File;
use std::{io, fs};
use log::*;
use std::ops::{Deref, Try};
use std::io::{Error, ErrorKind};

macro_rules! batch_file {
    () => ( "batchFile" )
}

static BATCH_FILE: &str = batch_file!();
static BATCH_FILE_NAME_PREFIX: &str = concat!(batch_file!(), "-");
static BATCH_FILE_GLOB_PATTERN: &str = concat!(batch_file!(), "*");
static DIGITAL_FORMAT: &str = "{:011}";
static LAST_BATCH_ID_FILE_NAME: &str = "lastBatchId";

const DEFAULT_MAX_BYTES_IN_FILES: usize = std::usize::MAX;

pub struct FileStorageSharedState {
    pub(crate) file_ids: VecDeque<i64>,
    occupied_bytes: usize,
    last_batch_id: i64,
    stopped: bool,
}

#[derive(Clone)]
pub struct FileStorage {
    path: PathBuf,
    batch_id_file: PathBuf,
    pub max_bytes: usize,
    pub(crate) shared_state: Arc<Lock<FileStorageSharedState>>
}

impl FileStorage {
    pub fn init(path: impl Into<PathBuf>) -> io::Result<FileStorage> {
        FileStorage::init_with_max_bytes(path, DEFAULT_MAX_BYTES_IN_FILES)
    }

    pub fn init_with_max_bytes(path: impl Into<PathBuf>, max_bytes: usize) -> io::Result<FileStorage> {
        let path = path.into();
        fs::create_dir_all(path)?;
        if !path.is_dir() {
            return Err(Error::new(ErrorKind::NotFound, format!("The path {:?} is not a directory", path)))
        }

//        batchIdFile = path.toAbsolutePath().resolve(LAST_BATCH_ID_FILE_NAME);
//
//        final List<Long> ids = new ArrayList<>();
//        try (final DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path, BATCH_FILE_GLOB_PATTERN)) {
//            for (Path file : directoryStream) {
//                ids.add(batchId(file.getFileName().toString()));
//            }
//        }
//        Collections.sort(ids);
//
//        final long batchIdFileLength = Files.exists(batchIdFile) ? Files.size(batchIdFile) : 0;
//
//        if (batchIdFileLength > 0) {
//            final List<String> lines = Files.readAllLines(batchIdFile, StandardCharsets.UTF_8);
//            lastBatchId = Integer.parseInt(lines.get(0));
//        } else if (ids.size() > 0) {
//            lastBatchId = ids.get(ids.size() - 1);
//        } else {
//            lastBatchId = 0;
//        }
//
//        log.debug("Init lastBatchId with {}", lastBatchId);
//
//        fileIds = new ArrayDeque<>(ids);
//        log.debug("Initialized {} batches.", ids.size());

        Ok(FileStorage {
            path: path.into(),
            batch_id_file: PathBuf::new(),
            max_bytes,
            shared_state: Arc::new(Lock::new(FileStorageSharedState {
                file_ids: VecDeque::new(),
                occupied_bytes: 0,
                last_batch_id: 0,
                stopped: false,
            })),
        })
    }
}

