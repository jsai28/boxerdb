const BTREE_PAGE_SIZE: u16 = 4096;
const BTREE_MAX_KEY_SIZE: u16 = 1000;
const BTREE_MAX_VAL_SIZE: u16 = 3000;
const BNODE_NODE: u8 = 1;
const BNODE_LEAF: u8 = 2;

// InternalNode represents a non-leaf node in a B+ tree.
// It holds keys used for routing and pointers (child page IDs) to other nodes.
// Keys are used for routing; each key separates ranges of child nodes.
// Example: keys = ["dog", "mango"] routes to children like:
//          [< "dog", "dog"–"mango", > "mango"]
// Children are page IDs (u32 offsets) pointing to other nodes (internal or leaf).
// children.len() == keys.len() + 1
#[derive(Clone, PartialEq)]
struct InternalNode {
    keys: Vec<String>,
    children: Vec<u32>,
}

// LeafNode represents the bottom-level node in a B+ tree that stores actual key-value pairs.
// These nodes are linked and contain data directly.
// Keys stored in sorted order, these are actual user-provided keys.
// Values corresponding to each key, stored as raw bytes (Vec<u8>).
// Each value could be anything — integers, strings, or serialized objects.
#[derive(Clone, PartialEq)]
struct LeafNode {
    keys: Vec<String>,
    value: Vec<Vec<u8>>,
}

enum Node {
    Internal(InternalNode),
    Leaf(LeafNode),
}

struct BTree {
    root: Node,
}

impl BTree {
    fn new() -> Self {
        Self {
            root: Node::Leaf(LeafNode {
                keys: Vec::new(),
                value: Vec::new()
            }),
        }
    }

    fn insert(&mut self, key: String, value: Vec<u8>) {
        self._insert(&mut self.root, key, value);
    }

    fn _insert(&mut self, node: &mut Node, key: String, value: Vec<u8>) {
        match node {
            Node::Internal(mut internal_node) => {
                let pos = internal_node.keys.binary_search(&key).unwrap_or_else(|e| e);
                let child_page_id = internal_node[pos];
                todo!("recursively move down internal nodes")
            },
            Node::Leaf(mut leaf_node) => {
                let pos = leaf_node.keys.binary_search(&key).unwrap_or_else(|e| e);
                leaf_node.keys.insert(pos, key);
                leaf_node.value.insert(pos, value);
            }
        }
    }
}
