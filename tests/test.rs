use boxerdb::storage::btree::{encode_node, decode_node, load_node, Node, BTree};
use tempfile::NamedTempFile;

fn create_sample_node() -> Node {
    let keys = vec![
        b"key1".to_vec(),
        b"key2".to_vec(),
        b"key3".to_vec(),
    ];
    let values = vec![
        b"value1".to_vec(),
        b"value2".to_vec(),
        b"value3".to_vec(),
    ];
    let children = vec![];
    Node {
        keys,
        values,
        children,
    }
}

fn get_temp_btree() -> BTree {
    let tmp = NamedTempFile::new().unwrap();
    BTree::new(tmp.path().to_str().unwrap()).unwrap()
}

#[test]
fn test_encode_decode_roundtrip_1() {
    let node = Node {
        keys: vec![b"key1".to_vec()],
        values: vec![b"value1".to_vec()],
        children: vec![],
    };

    let encoded = encode_node(&node);
    let decoded = decode_node(encoded);

    assert_eq!(node.keys, decoded.keys);
    assert_eq!(node.values, decoded.values);
    assert_eq!(node.children, decoded.children);
}

#[test]
fn test_encode_decode_roundtrip_2() {
    let node = create_sample_node();

    let encoded = encode_node(&node);
    let decoded = decode_node(encoded);

    assert_eq!(node.keys, decoded.keys);
    assert_eq!(node.values, decoded.values);
    assert_eq!(node.children, decoded.children);
}

#[test]
fn test_encode_decode_internal_node() {
    let node = Node {
        keys: vec![
            b"key1".to_vec(),
            b"key2".to_vec(),
        ],
        values: vec![],
        children: vec![10, 20, 30],
    };

    let encoded = encode_node(&node);
    let decoded = decode_node(encoded);

    assert_eq!(node.keys, decoded.keys);
    assert_eq!(node.values, decoded.values);
    assert_eq!(node.children, decoded.children);
}

#[test]
fn test_insert_single() {
    let mut btree = get_temp_btree();
    btree.insert(b"key1".to_vec(), b"value1".to_vec());

    let root = load_node(&mut btree.file, 0).unwrap();
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

    let root = load_node(&mut btree.file, 0).unwrap();
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
        let root = load_node(&mut btree.file, 0).unwrap();
        assert_eq!(root.keys, vec![b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]);
        assert_eq!(root.values, vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    }
}



