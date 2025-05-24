use crate::storage::node::Node;
use crate::storage::diskmanager::{DiskManager, EncodeResult};
use crate::storage::configs::{StorageConfig};

pub struct BTree {
    pub root: Node,
    pub root_offset: u64,
    pub storage_config: StorageConfig,
    pub disk_manager: DiskManager,
}

impl BTree {
    pub fn new(path: &str, storage_config: Option<StorageConfig>) -> std::io::Result<Self> {
        let storage_config = storage_config.unwrap_or_default();
        let mut disk_manager = DiskManager::new(path, storage_config.clone())?;
        // load in root node
        let root_offset = disk_manager.read_metadata()?;
        let root = disk_manager.load_node_from_disk(root_offset)?;

        Ok(Self {
            root,
            root_offset,
            storage_config,
            disk_manager
        })
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let new_offset = self._insert(&mut self.root.clone(), &key, &value);
        self.root_offset = new_offset;
        self.disk_manager.write_metadata(self.root_offset).unwrap();

        self.root = self.disk_manager.load_node_from_disk(self.root_offset).unwrap();
    }

    fn _insert(&mut self, node: &mut Node, key: &Vec<u8>, value: &Vec<u8>) -> u64 {
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

            let new_offset = self.disk_manager.get_new_offset().unwrap();
            match self.disk_manager.append_node_to_disk(new_offset, &update_node) {
                EncodeResult::Encoded => {
                    // write ok
                    new_offset
                }
                EncodeResult::NeedSplit => {
                    // find middle node, promote to parent
                    // everything left of middle, put in left node
                    // everything right of middle, put in right node
                    let mid = update_node.keys.len() / 2;
                    let left_node = Node {
                        keys: update_node.keys[..mid].to_vec(),
                        children: vec![],
                        values: update_node.values[..mid].to_vec()
                    };
                    let right_node = Node {
                        keys: update_node.keys[mid..].to_vec(),
                        children: vec![],
                        values: update_node.values[mid..].to_vec()
                    };

                    self.disk_manager.append_node_to_disk(new_offset, &left_node);

                    let right_offset = self.disk_manager.get_new_offset().unwrap();
                    self.disk_manager.append_node_to_disk(right_offset, &right_node);

                    // promote middle node
                    let middle_key = right_node.keys[0].clone();
                    let internal_node = Node {
                        keys: vec![middle_key],
                        values: vec![],
                        children: vec![new_offset, right_offset],
                    };

                    let middle_offset = self.disk_manager.get_new_offset().unwrap();
                    self.disk_manager.append_node_to_disk(middle_offset, &internal_node);
                    middle_offset
                }
            }
        } else {
            // internal node
            let pos = match node.keys.binary_search(&key) {
                Ok(pos) => pos + 1,
                Err(pos) => pos,
            };

            let offset = node.children[pos];
            let mut child_node = self.disk_manager.load_node_from_disk(offset).unwrap();

            let child_offset = self._insert(&mut child_node, key, value);
            let mut update_node = node.clone();
            update_node.children[pos] = child_offset;

            let new_offset = self.disk_manager.get_new_offset().unwrap();
            self.disk_manager.append_node_to_disk(new_offset, &update_node);

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
        BTree::new(tmp.path().to_str().unwrap(), None).unwrap()
    }

    fn get_temp_btree_new_configs() -> BTree {
        let tmp = NamedTempFile::new().unwrap();

        let storage_config = StorageConfig {
            // bad configs, can lead to splits that dont make sense
            // only for testing purposes
            page_size: 30,
            max_key_size: 10,
            max_val_size: 10,
            metadata_offset: 0,
            first_page_offset: 30,
        };

        BTree::new(tmp.path().to_str().unwrap(), Some(storage_config)).unwrap()
    }

    #[test]
    fn test_insert_single() {
        let mut btree = get_temp_btree();
        btree.insert(b"key1".to_vec(), b"value1".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
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

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
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
            let mut btree = BTree::new(&path, None).unwrap();
            btree.insert(b"alpha".to_vec(), b"1".to_vec());
            btree.insert(b"beta".to_vec(), b"2".to_vec());
            btree.insert(b"gamma".to_vec(), b"3".to_vec());
        }

        // Reload
        {
            let mut btree = BTree::new(&path, None).unwrap();
            let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
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

    #[test]
    fn test_node_split_path_promotes_correct_key() {
        let mut btree = get_temp_btree_new_configs();

        // Insert keys in order that will trigger a split
        btree.insert(b"alpha".to_vec(), b"1".to_vec());
        btree.insert(b"beta".to_vec(), b"1".to_vec());
        btree.insert(b"charlie".to_vec(), b"1".to_vec());

        // Now we check internal structure
        let root_offset = btree.root_offset;
        let root_node = btree.disk_manager.load_node_from_disk(root_offset).unwrap();

        // Root should be internal node with one promoted key: "beta"
        assert_eq!(root_node.keys.len(), 1);
        assert_eq!(root_node.keys[0], b"beta".to_vec());
        assert_eq!(root_node.children.len(), 2);

        // Read children from disk
        let left_child = btree.disk_manager.load_node_from_disk(root_node.children[0]).unwrap();
        let right_child = btree.disk_manager.load_node_from_disk(root_node.children[1]).unwrap();

        // Check right leaf contains "charlie"
        assert_eq!(right_child.keys, vec![b"beta".to_vec(), b"charlie".to_vec()]);
        assert_eq!(right_child.values, vec![b"1".to_vec(), b"1".to_vec()]);

        // Check left leaf contains "alpha"
        assert_eq!(left_child.keys, vec![b"alpha".to_vec()]);
        assert_eq!(left_child.values, vec![b"1".to_vec()]);
    }

    #[test]
    fn test_node_key_greater_than_max() {
        let result = std::panic::catch_unwind(move || {
            let mut btree = get_temp_btree_new_configs();
            btree.insert(b"12345678910".to_vec(), b"1".to_vec());
        });

        assert!(result.is_err(), "Expected panic due to key_len > max_key_size");
    }

    #[test]
    fn test_node_value_greater_than_max() {
        let result = std::panic::catch_unwind(move || {
            let mut btree = get_temp_btree_new_configs();
            btree.insert(b"1".to_vec(), b"012345678901234567890123456789".to_vec());
        });

        assert!(result.is_err(), "Expected panic due to val_len > max_val_size");
    }
}
