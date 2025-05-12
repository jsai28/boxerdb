mod storage;
use storage::btree::BTree;

fn main() {
    let _ = BTree::new("./data/store.db");
    println!("success.")
}
