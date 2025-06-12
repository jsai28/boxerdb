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
        let mut root_clone = self.root.clone();
        let result = self.insert_recursive(&mut root_clone, &key, &value);

        match result.splits {
            None => {
                // need to change this so it doesnt make the returned offset equal to roof offset
                // check where key goes in roots children if it has any
                // overwrite child pointer with result.new_offset
                self.root_offset = result.new_offset;
                self.disk_manager.write_metadata(self.root_offset).unwrap();
                self.root = self.disk_manager.load_node_from_disk(self.root_offset).unwrap();
            }
            Some(splits) => {
                // create new root case
                let promoted_key = splits.promoted_key;
                let new_root = Node {
                    keys: vec![promoted_key],
                    children: vec![splits.left_offset, splits.right_offset],
                    values: vec![],
                };

                let new_root_offset = self.disk_manager.get_new_offset().unwrap();
                self.root_offset = new_root_offset;
                self.disk_manager.append_node_to_disk(self.root_offset, &new_root);
                self.disk_manager.write_metadata(self.root_offset).unwrap();
                self.root = self.disk_manager.load_node_from_disk(self.root_offset).unwrap();
            }
        }
    }

    fn insert_recursive(&mut self, node: &mut Node, key: &Vec<u8>, value: &Vec<u8>) -> InsertResult {
        if node.children.len() == 0 {
            // leaf node
            self.insert_into_leaf(node, &key, &value)
        } else {
            // internal node
            let pos = node.keys.binary_search(&key).unwrap_or_else(|pos| pos);
            let child_offset = node.children[pos];
            let mut child_node = self.disk_manager.load_node_from_disk(child_offset).unwrap();
            let result = self.insert_recursive(&mut child_node, key, value);

            match result.splits {
                None => {
                    let new_child_offset = result.new_offset;
                    node.children[pos] = new_child_offset;

                    let new_internal_offset = self.disk_manager.get_new_offset().unwrap();
                    self.disk_manager.append_node_to_disk(new_internal_offset, &node);

                    InsertResult {
                        new_offset: new_internal_offset,
                        splits: None
                    }
                }
                Some(splits) => {
                    // create new internal node
                    // set child pointer to this new internal node
                    // return
                    let promoted_key = splits.promoted_key;
                    let left_child_offset = splits.left_offset;
                    let right_child_offset = splits.right_offset;

                    let new_internal_node = Node {
                        keys: vec![promoted_key],
                        children: vec![left_child_offset, right_child_offset],
                        values: vec![]
                    };

                    let new_offset = self.disk_manager.get_new_offset().unwrap();
                    self.disk_manager.append_node_to_disk(new_offset, &new_internal_node);
                    node.children[pos] = new_offset;

                    let new_internal_offset = self.disk_manager.get_new_offset().unwrap();
                    self.disk_manager.append_node_to_disk(new_internal_offset, &node);

                    InsertResult {
                        new_offset: new_internal_offset,
                        splits: None
                    }
                }
            }
        }
    }

    fn insert_into_leaf(&mut self, node: &mut Node, key: &Vec<u8>, value: &Vec<u8>) -> InsertResult {
        let pos = node.keys.binary_search(&key).unwrap_or_else(|pos| pos);
        node.keys.insert(pos, key.clone());
        node.values.insert(pos, value.clone());

        let new_offset = self.disk_manager.get_new_offset().unwrap();
        match self.disk_manager.append_node_to_disk(new_offset, node) {
            EncodeResult::Encoded => {
                InsertResult {
                    new_offset,
                    splits: None
                }
            }
            EncodeResult::NeedSplit => {
                let mid = node.keys.len() / 2;

                // for leaf nodes, include mid
                let left_node = Node {
                    keys: node.keys[..mid].to_vec(),
                    values: node.values[..mid].to_vec(),
                    children: vec![]
                };
                self.disk_manager.append_node_to_disk(new_offset, &left_node);

                let right_node = Node {
                    keys: node.keys[mid..].to_vec(),
                    values: node.values[mid..].to_vec(),
                    children: vec![]
                };
                let right_offset = self.disk_manager.get_new_offset().unwrap();
                self.disk_manager.append_node_to_disk(right_offset, &right_node);

                let promoted_key = node.keys[mid].clone();

                let split = InsertSplit {
                    promoted_key,
                    left_offset: new_offset,
                    right_offset
                };

                InsertResult {
                    new_offset: 0,
                    splits: Some(split)
                }
            }
        }
    }
}

struct InsertResult {
    new_offset: u64,
    splits: Option<InsertSplit>
}

struct InsertSplit {
    promoted_key: Vec<u8>,
    left_offset: u64,
    right_offset: u64,
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
            page_size: 32,
            max_key_size: 16,
            max_val_size: 16,
            metadata_offset: 0,
            first_page_offset: 32,
        };

        BTree::new(tmp.path().to_str().unwrap(), Some(storage_config)).unwrap()
    }

    #[test]
    fn test_insert_single_val_into_root() {
        let mut btree = get_temp_btree();
        btree.insert(b"key1".to_vec(), b"value1".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"key1");
        assert_eq!(root.values[0], b"value1");
    }

    #[test]
    fn test_insert_multiple_val_into_root() {
        let mut btree = get_temp_btree_new_configs();
        btree.insert(b"key1".to_vec(), b"value1".to_vec());
        btree.insert(b"key2".to_vec(), b"val".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 2);
        assert_eq!(root.keys[0], b"key1");
        assert_eq!(root.keys[1], b"key2");
        assert_eq!(root.values[0], b"value1");
        assert_eq!(root.values[1], b"val");
    }

    #[test]
    fn test_root_node_split_sorted() {
        let mut btree = get_temp_btree_new_configs();
        btree.insert(b"alpha".to_vec(), b"1".to_vec());
        btree.insert(b"beta".to_vec(), b"1".to_vec());
        btree.insert(b"charlie".to_vec(), b"1".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"beta");

        assert_eq!(root.children.len(), 2);
        let left_offset = root.children[0];
        let right_offset = root.children[1];
        let left_node = btree.disk_manager.load_node_from_disk(left_offset).unwrap();
        let right_node = btree.disk_manager.load_node_from_disk(right_offset).unwrap();

        assert_eq!(left_node.keys.len(), 1);
        assert_eq!(left_node.keys, vec![b"alpha".to_vec()]);
        assert_eq!(left_node.values.len(), 1);
        assert_eq!(left_node.values, vec![b"1".to_vec()]);

        assert_eq!(right_node.keys.len(), 2);
        assert_eq!(right_node.keys, vec![b"beta".to_vec(), b"charlie".to_vec()]);
        assert_eq!(right_node.values.len(), 2);
        assert_eq!(right_node.values, vec![b"1".to_vec(), b"1".to_vec()]);
    }

    #[test]
    fn test_root_node_split_unsorted() {
        let mut btree = get_temp_btree_new_configs();
        btree.insert(b"charlie".to_vec(), b"1".to_vec());
        btree.insert(b"alpha".to_vec(), b"1".to_vec());
        btree.insert(b"beta".to_vec(), b"1".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"beta");

        assert_eq!(root.children.len(), 2);
        let left_offset = root.children[0];
        let right_offset = root.children[1];
        let left_node = btree.disk_manager.load_node_from_disk(left_offset).unwrap();
        let right_node = btree.disk_manager.load_node_from_disk(right_offset).unwrap();

        assert_eq!(left_node.keys.len(), 1);
        assert_eq!(left_node.keys, vec![b"alpha".to_vec()]);
        assert_eq!(left_node.values.len(), 1);
        assert_eq!(left_node.values, vec![b"1".to_vec()]);

        assert_eq!(right_node.keys.len(), 2);
        assert_eq!(right_node.keys, vec![b"beta".to_vec(), b"charlie".to_vec()]);
        assert_eq!(right_node.values.len(), 2);
        assert_eq!(right_node.values, vec![b"1".to_vec(), b"1".to_vec()]);
    }

    #[test]
    fn test_insert_into_leaf_node_with_internal_node() {
        let mut btree = get_temp_btree_new_configs();
        btree.insert(b"charlie".to_vec(), b"1".to_vec());
        btree.insert(b"alpha".to_vec(), b"1".to_vec());
        btree.insert(b"beta".to_vec(), b"1".to_vec());
        btree.insert(b"a".to_vec(), b"1".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"beta");

        assert_eq!(root.children.len(), 2);
        let left_offset = root.children[0];
        let right_offset = root.children[1];
        let left_node = btree.disk_manager.load_node_from_disk(left_offset).unwrap();
        let right_node = btree.disk_manager.load_node_from_disk(right_offset).unwrap();

        assert_eq!(left_node.keys.len(), 2);
        assert_eq!(left_node.keys, vec![b"a".to_vec(), b"alpha".to_vec()]);
        assert_eq!(left_node.values.len(), 2);
        assert_eq!(left_node.values, vec![b"1".to_vec(), b"1".to_vec()]);

        assert_eq!(right_node.keys.len(), 2);
        assert_eq!(right_node.keys, vec![b"beta".to_vec(), b"charlie".to_vec()]);
        assert_eq!(right_node.values.len(), 2);
        assert_eq!(right_node.values, vec![b"1".to_vec(), b"1".to_vec()]);
    }

    #[test]
    fn test_insert_into_leaf_node_with_internal_node_2() {
        let mut btree = get_temp_btree_new_configs();
        btree.insert(b"charlie".to_vec(), b"1".to_vec());
        btree.insert(b"alpha".to_vec(), b"1".to_vec());
        btree.insert(b"beta".to_vec(), b"1".to_vec());
        btree.insert(b"abcd".to_vec(), b"1".to_vec());
        btree.insert(b"abcdefg".to_vec(), b"1".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"beta");

        assert_eq!(root.children.len(), 2);
        let left_offset = root.children[0];
        let right_offset = root.children[1];
        let left_node = btree.disk_manager.load_node_from_disk(left_offset).unwrap();
        let right_node = btree.disk_manager.load_node_from_disk(right_offset).unwrap();

        assert_eq!(left_node.keys.len(), 1);
        assert_eq!(left_node.keys, vec![b"abcdefg".to_vec()]);
        assert_eq!(left_node.children.len(), 2);

        assert_eq!(right_node.keys.len(), 2);
        assert_eq!(right_node.keys, vec![b"beta".to_vec(), b"charlie".to_vec()]);
        assert_eq!(right_node.values.len(), 2);
        assert_eq!(right_node.values, vec![b"1".to_vec(), b"1".to_vec()]);
    }
}