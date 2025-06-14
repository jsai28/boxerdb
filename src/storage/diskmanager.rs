use crate::storage::configs::{StorageConfig};
use crate::storage::node::{Node};
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::path::Path;

#[derive(Debug)]
pub enum EncodeResult {
    Encoded,
    NeedSplit,
}

/// All functions related to reading and writing from disk
pub struct DiskManager {
    pub file: File,
    pub config: StorageConfig,
}

impl DiskManager {
    /// Open or create a file, load metadata
    pub fn new(path: &str, config: StorageConfig) -> Result<Self> {
        // create the directory if it doesn't exist
        let dir = Path::new(path).parent().unwrap();
        std::fs::create_dir_all(dir)?;

        // read file if it exists, else create it
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let is_new_file = metadata.len() == 0;

        let mut disk_manager = Self { file, config };

        if is_new_file {
            // write root_offset to metadata block
            disk_manager.write_metadata(disk_manager.config.first_page_offset)?;

            let root = Node {
                keys: vec![],
                values: vec![],
                children: vec![],
            };

            // write empty root to root_offset
            disk_manager.append_node_to_disk(disk_manager.config.first_page_offset, &root);
        }

        Ok(disk_manager)
    }

    /// read page 0 which is the metadata page
    /// first 8 bytes are the root offset, rest is currently unused
    /// returns the root offset
    pub fn read_metadata(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.file.seek(SeekFrom::Start(self.config.metadata_offset))?;
        match self.file.read_exact(&mut buf) {
            Ok(_) => Ok(u64::from_le_bytes(buf)),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// write a new root_offset to metadata page
    /// root_offset is where the root page is located on disk
    pub fn write_metadata(&mut self, root_offset: u64) -> Result<()> {
        let mut block = vec![0u8; self.config.page_size as usize];
        block[..8].copy_from_slice(&root_offset.to_le_bytes());
        self.file.seek(SeekFrom::Start(self.config.metadata_offset))?;
        self.file.write_all(&block)?;
        self.file.sync_all()?;
        Ok(())
    }

    /// Load a node from disk into memory, given the page offset
    pub fn load_node_from_disk(&mut self, offset: u64) -> Result<Node> {
        let mut buf = vec![0u8; self.config.page_size as usize];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        Ok(Node::decode_node(buf))
    }

    /// Write the node from memory to disk
    pub fn append_node_to_disk(&mut self, offset: u64, node: &Node) -> EncodeResult {
        match Node::encode_node(node, self.config.clone()) {
            Some(encoded) => {
                self.file.seek(SeekFrom::Start(offset)).unwrap();
                self.file.write_all(&encoded).unwrap();
                self.file.sync_all().unwrap();
                EncodeResult::Encoded
            }
            None => {
                EncodeResult::NeedSplit
            }
        }
    }

    /// Get a new offset
    pub fn get_new_offset(&mut self) -> Result<u64> {
        self.file.seek(SeekFrom::End(0))
    }
}
