use std::io::{Read, Seek, Write};

const BTREE_PAGE_SIZE: u16 = 4096;
const BTREE_MAX_KEY_SIZE: u16 = 1000;
const BTREE_MAX_VAL_SIZE: u16 = 3000;
const BNODE_INTERNAL: u8 = 0;
const BNODE_LEAF: u8 = 1;

#[derive(Clone, PartialEq)]
struct Node {
    keys: Vec<Vec<u8>>,
    children: Vec<Node>,
    value: Vec<Vec<u8>>,
}

pub struct BTree {
    root: Node,
}

impl BTree {
    pub fn new(path: &str) -> std::io::Result<Self> {
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
                value: vec![],
                children: vec![],
            };

            let encoded_root = encode_node(&root);
            file.write_all(&encoded_root)?;
            file.sync_all()?;
            Ok(Self { root })
        } else {
            // file has root node
            todo!("decode root node into memory")
        }
    }
}

// Encode the keys, values, and children of a node + metadata
// Node = node_type (u8) + num_of_keys (u16) + pointers (u64) + offsets (u16) + KV pairs (4000 bytes) + unused space
// KV pairs = key_len (u16) + val_len (u16) + key bytes + val bytes
fn encode_node(node: &Node) -> Vec<u8> {
    let mut buf = vec![0u8; BTREE_PAGE_SIZE as usize];
    let mut node_type = BNODE_LEAF;
    if !node.children.is_empty() {
        node_type = BNODE_INTERNAL;
    };
    buf[0] = node_type;

    let num_keys = node.keys.len() as u16;
    buf[1..3].copy_from_slice(&num_keys.to_le_bytes());

    let mut cursor = 3;
    let offsets_start = cursor;
    cursor += (num_keys as usize) * 2;
    for i in 0..num_keys as usize {
        let key = &node.keys[i];
        let val = &node.value[i];
        let key_len = key.len() as u16;
        let val_len = val.len() as u16;

        assert!(key_len <= BTREE_MAX_KEY_SIZE);
        assert!(val_len <= BTREE_MAX_VAL_SIZE);

        let offset = cursor as u16;
        let offset_pos = offsets_start + i * 2;
        buf[offset_pos..offset_pos + 2].copy_from_slice(&offset.to_le_bytes());

        buf[cursor..cursor+2].copy_from_slice(&key_len.to_le_bytes());
        buf[cursor+2..cursor+4].copy_from_slice(&val_len.to_le_bytes());
        cursor += 4;

        // Write key bytes
        buf[cursor..cursor + key_len as usize].copy_from_slice(key);
        cursor += key_len as usize;

        // Write value bytes
        buf[cursor..cursor + val_len as usize].copy_from_slice(val);
        cursor += val_len as usize;
    }

    buf
}

fn decode_node(node: Vec<u8>) -> Node {
    todo!("decode the bytes into a node for memory")
}
