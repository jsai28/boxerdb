pub(crate) const BNODE_INTERNAL: u8 = 0;
pub(crate) const BNODE_LEAF: u8 = 1;

/// Users should be able to change these configs
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub page_size: u16,
    pub max_key_size: u16,
    pub max_val_size: u16,
    pub metadata_offset: u64,
    pub first_page_offset: u64,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            page_size: 4096,
            max_key_size: 1000,
            max_val_size: 3000,
            metadata_offset: 0,
            first_page_offset: 4096
        }
    }
}
