pub struct BtreeLeafNodeBodyLayout {
    pub leaf_node_key_size: usize,
    pub leaf_node_key_offset: usize,
    pub leaf_node_value_size: usize,
    pub leaf_node_value_offset: usize,
    pub leaf_node_cell_size: usize,
    pub leaf_node_space_for_cells: usize,
    pub leaf_node_max_cells: usize,
    pub leaf_node_right_split_count: usize,
    pub leaf_node_left_split_count: usize,
}

impl BtreeLeafNodeBodyLayout {
    pub(crate) fn new(key_size: usize, row_size: usize) -> BtreeLeafNodeBodyLayout {
        let leaf_node_key_size: usize = key_size;
        let leaf_node_key_offset: usize = 0;
        let leaf_node_value_size: usize = row_size;
        let leaf_node_value_offset: usize = leaf_node_key_offset + leaf_node_key_size;
        let leaf_node_cell_size: usize = leaf_node_key_size + leaf_node_value_size;
        let leaf_node_space_for_cells: usize = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
        let leaf_node_max_cells: usize = leaf_node_space_for_cells / leaf_node_cell_size;
        let leaf_node_right_split_count: usize = (leaf_node_max_cells + 1) / 2;
        let leaf_node_left_split_count: usize = (leaf_node_max_cells + 1) - leaf_node_right_split_count;

        BtreeLeafNodeBodyLayout {
            leaf_node_key_size: leaf_node_key_size,
            leaf_node_key_offset: leaf_node_key_offset,
            leaf_node_value_size: leaf_node_value_size,
            leaf_node_value_offset: leaf_node_value_offset,
            leaf_node_cell_size: leaf_node_cell_size,
            leaf_node_space_for_cells: leaf_node_space_for_cells,
            leaf_node_max_cells: leaf_node_max_cells,
            leaf_node_right_split_count: leaf_node_right_split_count,
            leaf_node_left_split_count: leaf_node_left_split_count,
        }
    }
}

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

/*
 * Btree Table Metadata
 */
pub const INDEXED_FIELD_TYPE_PRIMARY: usize = FIELD_TYPE_PRIMARY_SIZE;
pub const INDEXED_FIELD_SIZE: usize = 2;
pub const INDEXED_FIELD_SIZE_OFFSET: usize = FIELD_TYPE_PRIMARY_SIZE;
pub const INDEXED_FIELD_NAME_SIZE: usize = FIELD_NAME_SIZE;
pub const INDEXED_FIELD_NAME_SIZE_OFFSET: usize = INDEXED_FIELD_SIZE_OFFSET + INDEXED_FIELD_SIZE;
pub const BTREE_METADATA_SIZE: usize = INDEXED_FIELD_TYPE_PRIMARY + INDEXED_FIELD_SIZE + INDEXED_FIELD_NAME_SIZE;

/*
* Common Node Header Layout
*/
pub const NODE_TYPE_SIZE: usize = std::mem::size_of::<u8>();
pub const NODE_TYPE_OFFSET: usize = BTREE_METADATA_SIZE;
pub const IS_ROOT_SIZE: usize = std::mem::size_of::<u8>();
pub const IS_ROOT_OFFSET: usize = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
pub const PARENT_POINTER_SIZE: usize = std::mem::size_of::<u32>();
pub const PARENT_POINTER_OFFSET: usize = IS_ROOT_OFFSET + IS_ROOT_SIZE;
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;
/*
* Leaf Node Header Layout
*/
pub const LEAF_NODE_NUM_CELLS_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_NUM_CELLS_OFFSET: usize = PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE;
pub const LEAF_NODE_NEXT_LEAF_SIZE: usize = std::mem::size_of::<u32>();
pub const LEAF_NODE_NEXT_LEAF_OFFSET: usize = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;
pub const LEAF_NODE_BODY_OFFSET: usize = LEAF_NODE_NEXT_LEAF_OFFSET + LEAF_NODE_NEXT_LEAF_SIZE;

/*
* Internal Node Header Layout
*/
pub const INTERNAL_NODE_NUM_KEYS_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_NUM_KEYS_OFFSET: usize = PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE;
pub const INTERNAL_NODE_RIGHT_CHILD_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_RIGHT_CHILD_OFFSET: usize = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
pub const INTERNAL_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE;
pub const INTERNAL_NODE_BODY_OFFSET: usize = INTERNAL_NODE_RIGHT_CHILD_OFFSET + INTERNAL_NODE_RIGHT_CHILD_SIZE;

/*
* Internal Node Body Layout
*/
pub const INTERNAL_NODE_CHILD_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_KEY_SIZE: usize = std::mem::size_of::<u32>();
pub const INTERNAL_NODE_CELL_SIZE: usize = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
pub const INTERNAL_NODE_MAX_KEYS: usize = 3;
pub const INVALID_PAGE_NUM: usize = u32::MAX as usize;


/*
metadata file structure
*/
pub const FIELD_NUMBER_SIZE: usize = 2;
/*
1 byte for field data type + if it is primary
00000001
first bit is for primary,
the following 2 bits for data
00 -> text
01 -> int
10 -> float
11 -> boolean
 */
pub const FIELD_TYPE_PRIMARY_SIZE: usize = 1;
pub const FIELD_NAME_SIZE: usize = 64;


// by default, Text can have at max 255 bytes.
pub const TEXT_DEFAULT_SIZE: usize = 255;

// number of bytes used to store each data type size in metadata file
// if the type is TEXT, there is an additional byte that represents the max size of this text
pub const TEXT_CHARS_NUM_SIZE: usize = 2;
pub const INTEGER_SIZE: usize = 4;
pub const FLOAT_SIZE: usize = 4;
pub const BOOLEAN_SIZE: usize = 1;

pub const DATA_FOLDER: &str = "./data";


// Sequential Page Header
pub const SEQUENTIAL_CELLS_NUM_SIZE: usize = 4;
pub const SEQUENTIAL_NODE_HEADER_SIZE: usize = SEQUENTIAL_CELLS_NUM_SIZE;
pub const SEQUENTIAL_NODE_BODY_OFFSET: usize = SEQUENTIAL_NODE_HEADER_SIZE;
