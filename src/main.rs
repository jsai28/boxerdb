mod storage;
use storage::btree::BTree;

fn main() {
    let mut btree = BTree::new("./data/store.db");
    println!("success.")
}
