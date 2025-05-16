use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, Result, ErrorKind};
use std::path::{Path};
use crate::storage::constants::{BTREE_PAGE_SIZE, METADATA_OFFSET, FIRST_PAGE_OFFSET};
use crate::storage::node::Node;

/// All functions related to reading and writing from disk


/// Open or create a file, load metadata
pub fn create_db_file(path: &str) -> Result<File> {
    // create the directory if it doesn't exist
    let dir = Path::new(path).parent().unwrap();
    std::fs::create_dir_all(dir)?;

    // read file if it exists, else create it
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    let metadata = file.metadata()?;
    let is_new_file = metadata.len() == 0;

    if is_new_file {
        // write root_offset to metadata block
        write_metadata(&mut file, FIRST_PAGE_OFFSET)?;

        let root = Node {
            keys: vec![],
            values: vec![],
            children: vec![],
        };

        // write empty root to root_offset
        append_node_to_disk(&mut file, FIRST_PAGE_OFFSET, &root)?;
    }

    Ok(file)
}

/// read page 0 which is the metadata page
/// first 8 bytes are the root offset, rest is currently unused
/// returns the root offset
pub fn read_metadata(file: &mut File) -> Result<u64> {
    let mut buf = [0u8; 8];
    file.seek(SeekFrom::Start(METADATA_OFFSET))?;
    match file.read_exact(&mut buf) {
        Ok(_) => Ok(u64::from_le_bytes(buf)),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(0),
        Err(e) => Err(e),
    }
}

/// write a new root_offset to metadata page
/// root_offset is where the root page is located on disk
pub fn write_metadata(file: &mut File, root_offset: u64) -> Result<()> {
    let mut block = [0u8; BTREE_PAGE_SIZE as usize];
    block[..8].copy_from_slice(&root_offset.to_le_bytes());
    file.seek(SeekFrom::Start(METADATA_OFFSET))?;
    file.write_all(&block)?;
    file.sync_all()?;
    Ok(())
}

/// Load a node from disk into memory, given the page offset
pub fn load_node_from_disk(file: &mut File, offset: u64) -> Result<Node> {
    let mut buf = vec![0u8; BTREE_PAGE_SIZE as usize];
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;
    Ok(Node::decode_node(buf))
}

/// Write the node from memory to disk
pub fn append_node_to_disk(file: &mut File, offset: u64, node: &Node) -> Result<()> {
    let encoded = Node::encode_node(node);
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    Ok(())
}

/// Get a new offset
pub fn get_new_offset(file: &mut File) -> Result<u64> {
    file.seek(SeekFrom::End(0))
}