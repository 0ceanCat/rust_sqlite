use crate::storage_engine::enums::NodeType::{Internal, Leaf};

pub(crate) enum NodeType {
    Leaf = 0,
    Internal = 1,
}

impl From<u8> for NodeType {
    fn from(value: u8) -> Self {
        match value {
            0 => Leaf,
            1 => Internal,
            _ => {
                panic!("Db file contains unknown NodeType. Corrupt file.")
            }
        }
    }
}
