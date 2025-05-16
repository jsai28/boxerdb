pub(crate) const BTREE_PAGE_SIZE: u16 = 4096;
pub(crate) const BTREE_MAX_KEY_SIZE: u16 = 1000;
pub(crate) const BTREE_MAX_VAL_SIZE: u16 = 3000;
pub(crate) const BNODE_INTERNAL: u8 = 0;
pub(crate) const BNODE_LEAF: u8 = 1;

pub(crate) const METADATA_OFFSET: u64 = 0;
pub(crate) const FIRST_PAGE_OFFSET: u64 = 4096;
