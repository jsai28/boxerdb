use boxerdb::storage::btree::{encode_node, decode_node, Node};

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


