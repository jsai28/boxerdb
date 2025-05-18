use crate::storage::node::Node;
use crate::storage::diskmanager::{
    append_node_to_disk, create_db_file, get_new_offset, load_node_from_disk, read_metadata,
    write_metadata,
};
use crate::storage::configs::{StorageConfig};
use std::fs::File;
use std::io::{Read, Seek, Write};

pub struct BTree {
    pub root: Node,
    pub root_offset: u64,
    pub file: File,
    pub storage_config: StorageConfig,
}

impl BTree {
    pub fn new(path: &str, storage_config: Option<StorageConfig>) -> std::io::Result<Self> {
        let storage_config = storage_config.unwrap_or_default();
        let mut file = create_db_file(path)?;
        // load in root node
        let root_offset = read_metadata(&mut file)?;
        let root = load_node_from_disk(&mut file, root_offset)?;

        Ok(Self {
            root,
            root_offset,
            file,
            storage_config,
        })
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let file = &mut self.file;
        let root = &mut self.root;

        let new_offset = Self::_insert(root, &key, &value, file);
        self.root_offset = new_offset;
        write_metadata(file, self.root_offset).unwrap();

        self.root = load_node_from_disk(file, self.root_offset).unwrap();
    }

    fn _insert(node: &mut Node, key: &Vec<u8>, value: &Vec<u8>, file: &mut File) -> u64 {
        if node.children.is_empty() {
            // leaf node
            let mut update_node = node.clone();
            match update_node.keys.binary_search(&key) {
                Ok(pos) => {
                    // Key exists — update value
                    update_node.values[pos] = value.clone();
                }
                Err(pos) => {
                    // Key doesn't exist — insert
                    update_node.keys.insert(pos, key.clone());
                    update_node.values.insert(pos, value.clone());
                }
            }

            let new_offset = get_new_offset(file).unwrap();
            append_node_to_disk(file, new_offset, &update_node).unwrap();

            new_offset
        } else {
            // internal node
            let pos = match node.keys.binary_search(&key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            };

            let offset = node.children[pos];
            let mut child_node = load_node_from_disk(file, offset).unwrap();

            let child_offset = Self::_insert(&mut child_node, key, value, file);
            let mut update_node = node.clone();
            update_node.children[pos] = child_offset;

            let new_offset = get_new_offset(file).unwrap();
            append_node_to_disk(file, new_offset, &update_node).unwrap();

            new_offset
        }
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

        let root = load_node_from_disk(&mut btree.file, btree.root_offset).unwrap();
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

        let root = load_node_from_disk(&mut btree.file, btree.root_offset).unwrap();
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
            let root = load_node_from_disk(&mut btree.file, btree.root_offset).unwrap();
            assert_eq!(root.keys.len(), 3);
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
