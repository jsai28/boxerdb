use crate::storage::node::{Node};
use crate::storage::diskmanager::{DiskManager, AppendResult};
use crate::storage::configs::{StorageConfig};

pub struct BTree {
    pub root: Node,
    pub root_offset: u64,
    pub storage_config: StorageConfig,
    pub disk_manager: DiskManager,
}

struct InsertResult {
    new_offset: Option<u64>,
    splits: Option<InsertSplit>
}

struct InsertSplit {
    promoted_key: Vec<u8>,
    left_offset: u64,
    right_offset: u64,
}

struct DeleteResult {
    new_offset: Option<u64>,
    merges: Option<DeleteMerge>
}

struct DeleteMerge {
    left_offset: u64,
    right_offset: u64,
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
        // modify clone of root so insert is durable
        let mut root_clone = self.root.clone();
        let result = self.insert_recursive(&mut root_clone, &key, &value);

        match result.splits {
            None => {
                // commit transaction by changing root pointer to new root offset
                self.root_offset = result.new_offset.unwrap();
                self.disk_manager.write_metadata(self.root_offset).unwrap();
                self.root = self.disk_manager.load_node_from_disk(self.root_offset).unwrap();
            }
            Some(splits) => {
                // create new root case
                // increases height of tree
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
                    node.children[pos] = new_child_offset.unwrap();

                    let new_internal_offset = self.disk_manager.get_new_offset().unwrap();
                    self.disk_manager.append_node_to_disk(new_internal_offset, &node);

                    InsertResult {
                        new_offset: Some(new_internal_offset),
                        splits: None
                    }
                }
                Some(splits) => {
                    // The node has overflowed its page size
                    // Split into two nodes and promote the middle key to the current node
                    let promoted_key = splits.promoted_key;
                    let left_child_offset = splits.left_offset;
                    let right_child_offset = splits.right_offset;

                    node.keys.insert(pos, promoted_key);

                    node.children.remove(pos);
                    node.children.insert(pos, left_child_offset);
                    node.children.insert(pos+1, right_child_offset);

                    let new_offset = self.disk_manager.get_new_offset().unwrap();
                    match self.disk_manager.append_node_to_disk(new_offset, &node) {
                        AppendResult::Encoded => {
                            InsertResult {
                                new_offset: Some(new_offset),
                                splits: None
                            }
                        }
                        AppendResult::NeedSplit => {
                            self.propagate_internal_split(node, new_offset)
                        }
                    }
                }
            }
        }
    }

    fn insert_into_leaf(&mut self, node: &mut Node, key: &Vec<u8>, value: &Vec<u8>) -> InsertResult {
        match node.keys.binary_search(&key) {
            Ok(pos) => {
                // key already exists, update value
                node.values[pos] = value.clone();
            }
            Err(pos) => {
                node.keys.insert(pos, key.clone());
                node.values.insert(pos, value.clone());
            }
        }

        let new_offset = self.disk_manager.get_new_offset().unwrap();
        match self.disk_manager.append_node_to_disk(new_offset, node) {
            AppendResult::Encoded => {
                InsertResult {
                    new_offset: Some(new_offset),
                    splits: None
                }
            }
            AppendResult::NeedSplit => {
                self.propagate_leaf_split(node, new_offset)
            }
        }
    }

    fn propagate_internal_split(&mut self, node: &mut Node, new_offset: u64) -> InsertResult {
        // adding the promoted key to the current node resulted in another split
        // the promoted key should be present in either left or right sub nodes' keys
        // as this would be redundant like this sentence
        let mid = node.keys.len() / 2;
        // for internal nodes, dont include mid
        let left_node = Node {
            keys: node.keys[..mid].to_vec(),
            values: vec![],
            children: node.children[..=mid].to_vec(),
        };
        self.disk_manager.append_node_to_disk(new_offset, &left_node);

        let right_node = Node {
            keys: node.keys[mid+1..].to_vec(),
            values: vec![],
            children: node.children[mid+1..].to_vec(),
        };

        let right_offset = self.disk_manager.get_new_offset().unwrap();
        self.disk_manager.append_node_to_disk(right_offset, &right_node);

        let promoted_key = node.keys[mid].clone();
        let splits = InsertSplit {
            promoted_key,
            left_offset: new_offset,
            right_offset
        };

        InsertResult {
            new_offset: None,
            splits: Some(splits)
        }
    }

    fn propagate_leaf_split(&mut self, node: &mut Node, left_offset: u64) -> InsertResult {
        let mid = node.keys.len() / 2;

        // for leaf nodes, include mid in keys
        let left_node = Node {
            keys: node.keys[..mid].to_vec(),
            values: node.values[..mid].to_vec(),
            children: vec![]
        };
        self.disk_manager.append_node_to_disk(left_offset, &left_node);

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
            left_offset,
            right_offset
        };

        InsertResult {
            new_offset: None,
            splits: Some(split)
        }
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        // modify clone of root so delete is durable
        let mut root_clone = self.root.clone();
        let result = self.delete_recursive(&mut root_clone, &key);

        self.root_offset = result.new_offset.unwrap();
        self.disk_manager.write_metadata(self.root_offset).unwrap();
        self.root = self.disk_manager.load_node_from_disk(self.root_offset).unwrap();
    }

    fn delete_recursive(&mut self, node: &mut Node, key: &Vec<u8>) -> DeleteResult {
        if node.children.len() == 0 {
            // leaf node
            match node.keys.binary_search(&key) {
                Ok(pos) => {
                    node.keys.remove(pos);
                    node.values.remove(pos);
                }
                Err(_) => {
                    // doesn't exist in node
                    // do nothing
                    DeleteResult {
                        new_offset: None,
                        merges: None
                    };
                }
            }
            // check if encoded meets the minimum size
            let needs_merge = self.disk_manager.check_node_needs_merge(node);
            if !needs_merge {
                let new_offset = self.disk_manager.get_new_offset().unwrap();
                match self.disk_manager.append_node_to_disk(new_offset, &node) {
                    AppendResult::Encoded => {
                        // delete successful
                        DeleteResult {
                            new_offset: Some(new_offset),
                            merge: false
                        }
                    }
                    _ => panic!("Delete failed!")
                }
            } else {
                panic!("needs merge!");
            }
        } else {
            let pos = node.keys.binary_search(&key).unwrap_or_else(|pos| pos);
            let child_offset = node.children[pos];
            let mut child_node = self.disk_manager.load_node_from_disk(child_offset).unwrap();
            self.delete_recursive(&mut child_node, &key)
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::NamedTempFile;

    fn get_temp_btree() -> BTree {
        let tmp = tempfile::NamedTempFile::new().expect("Failed to create temp file");

        let path_str = tmp.path().to_str().expect("Temp path is not UTF-8");
        println!("Temp file path: {}", path_str);

        let btree = BTree::new(path_str, None).expect("Failed to create BTree");
        btree
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
            min_node_size: 8
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
    fn test_duplicate_key_inserts() {
        let mut btree = get_temp_btree();
        btree.insert(b"a".to_vec(), b"1".to_vec());
        btree.insert(b"a".to_vec(), b"2".to_vec());

        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 1);
        assert_eq!(root.keys[0], b"a".to_vec());
        assert_eq!(root.values[0], b"2".to_vec());
    }

    #[test]
    fn test_simple_delete_key() {
        let mut btree = get_temp_btree_new_configs();
        btree.insert(b"a".to_vec(), b"1".to_vec());
        btree.insert(b"b".to_vec(), b"1".to_vec());
        btree.insert(b"c".to_vec(), b"1".to_vec());

        btree.delete(b"b".to_vec());
        let root = btree.disk_manager.load_node_from_disk(btree.root_offset).unwrap();
        assert_eq!(root.keys.len(), 2);
        assert_eq!(root.keys[0], b"a");
        assert_eq!(root.keys[1], b"c");
    }
}