use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, Result, ErrorKind};
use crate::storage::constants::BTREE_PAGE_SIZE;
use crate::storage::node::Node;

pub struct Pager {
    file: File,
    root_offset: u64,
}

/// Pager is the bridge between disk and memory
impl Pager {
    /// Open or create a file for the pager, load metadata
    pub fn open(path: &str) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Read metadata (root offset)
        let root_offset = Self::read_metadata(&mut file).unwrap_or(0);

        Ok(Self { file, root_offset })
    }

    /// read page 0 which is the metadata page
    /// returns the root offset
    fn read_metadata(file: &mut File) -> Result<u64> {
        let mut buf = [0u8; 8];
        file.seek(SeekFrom::Start(0))?;
        match file.read_exact(&mut buf) {
            Ok(_) => Ok(u64::from_le_bytes(buf)),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(0),
            Err(e) => Err(e),
        }
    }

    /// write a new root_offset to metadata page
    fn write_metadata(file: &mut File, root_offset: u64) -> Result<()> {
        let mut block = [0u8; BTREE_PAGE_SIZE as usize];
        block[..8].copy_from_slice(&root_offset.to_le_bytes());
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&block)?;
        file.sync_all()?;
        Ok(())
    }

    /// 
    pub fn load_node_from_disk(file: &mut File, offset: u64) -> Result<Node> {
        let mut buf = vec![0u8; BTREE_PAGE_SIZE as usize];
        file.seek(SeekFrom::Start(offset))?;
        file.read_exact(&mut buf)?;
        Ok(Node::decode_node(buf))
    }

    pub fn append_node_to_disk(file: &mut File, offset: u64, node: &Node) -> Result<()> {
        let encoded = Node::encode_node(node);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&encoded)?;
        file.sync_all()?;
        Ok(())
    }
}