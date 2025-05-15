use crate::storage::constants::{
    BTREE_PAGE_SIZE,
};
use crate::storage::node::{Node};
use std::fs::File;
use std::io::{Read, Seek, Write};
use crate::storage::pager::{append_node_to_disk, load_node_to_disk, read_metadata, write_metadata};

pub struct BTree {
    pub root: Node,
    pub root_offset: u64,
    pub file: File,
}

impl BTree {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let dir = std::path::Path::new(path).parent().unwrap();
        std::fs::create_dir_all(dir)?;

        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let metadata = file.metadata()?;
        if metadata.len() == 0 {
            // file is empty
            let root = Node {
                keys: vec![],
                values: vec![],
                children: vec![],
            };

            let root_offset = BTREE_PAGE_SIZE as u64; // page 1
            write_metadata(&mut file, root_offset)?;
            append_node_to_disk(&mut file, root_offset, &root)?;

            Ok(Self {
                root,
                root_offset,
                file,
            })
        } else {
            // decode root node
            let root_offset = read_metadata(&mut file)?;
            let root = load_node_to_disk(&mut file, root_offset)?;

            Ok(Self {
                root,
                root_offset,
                file,
            })
        }
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let file = &mut self.file;
        let root = &mut self.root;
        let root_offset = self.root_offset;
        Self::_insert(root, &key, &value, root_offset, file);
    }

    fn _insert(node: &mut Node, key: &Vec<u8>, value: &Vec<u8>, offset: u64, file: &mut File) {
        if node.children.is_empty() {
            // leaf node
            match node.keys.binary_search(&key) {
                Ok(pos) => {
                    // Key exists — update value
                    node.values[pos] = value.clone();
                }
                Err(pos) => {
                    // Key doesn't exist — insert
                    node.keys.insert(pos, key.clone());
                    node.values.insert(pos, value.clone());
                }
            }
            append_node_to_disk(file, offset, node).unwrap();
        } else {
            // internal node
            let pos = match node.keys.binary_search(&key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            };

            let offset = node.children[pos];
            let mut child_node = load_node_to_disk(file, offset).unwrap();
            Self::_insert(&mut child_node, key, value, offset, file);
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::NamedTempFile;

    fn get_temp_btree() -> BTree {
        let tmp = NamedTempFile::new().unwrap();
        BTree::new(tmp.path().to_str().unwrap()).unwrap()
    }

    #[test]
    fn test_insert_single() {
        let mut btree = get_temp_btree();
        btree.insert(b"key1".to_vec(), b"value1".to_vec());

        let root = load_node_to_disk(&mut btree.file, 4096).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"key1");
        assert_eq!(root.values[0], b"value1");
    }

    #[test]
    fn test_insert_multiple_sorted_order() {
        let mut btree = get_temp_btree();
        btree.insert(b"b".to_vec(), b"2".to_vec());
        btree.insert(b"a".to_vec(), b"1".to_vec());
        btree.insert(b"c".to_vec(), b"3".to_vec());

        let root = load_node_to_disk(&mut btree.file, 4096).unwrap();
        assert_eq!(root.keys.len(), 3);
        assert_eq!(root.keys[0], b"a");
        assert_eq!(root.values[0], b"1");
        assert_eq!(root.keys[1], b"b");
        assert_eq!(root.values[1], b"2");
        assert_eq!(root.keys[2], b"c");
        assert_eq!(root.values[2], b"3");
    }

    #[test]
    fn test_persisted_btree_read_after_write() {
        // Setup
        let tmpfile = NamedTempFile::new().unwrap();
        let path = tmpfile.path().to_str().unwrap().to_string();

        // Insert
        {
            let mut btree = BTree::new(&path).unwrap();
            btree.insert(b"alpha".to_vec(), b"1".to_vec());
            btree.insert(b"beta".to_vec(), b"2".to_vec());
            btree.insert(b"gamma".to_vec(), b"3".to_vec());
        }

        // Reload
        {
            let mut btree = BTree::new(&path).unwrap();
            let root = load_node_to_disk(&mut btree.file, 4096).unwrap();
            assert_eq!(
                root.keys,
                vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]
            );
            assert_eq!(
                root.values,
                vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]
            );
        }
    }
}
