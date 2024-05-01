use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::process::exit;
use std::ptr;
use crate::sql_engine::sql_structs::{DataType, Value};
use crate::storage_engine::config::*;
use crate::storage_engine::common::Page;
use crate::storage_engine::enums::NodeType;

pub trait Pager {
    fn get_page(&self, page_index: usize) -> *const u8;
    fn get_or_create_page(&mut self, page_index: usize) -> *mut u8;
}

#[derive(Debug)]
pub struct AbstractPager {
    pages: Box<[Option<Page>; TABLE_MAX_PAGES]>,
    total_pages: usize,
    fd: File
}

impl AbstractPager {
    pub(crate) fn new(total_pages: usize, file: File) -> AbstractPager {
        AbstractPager {
            pages: Box::new([None; TABLE_MAX_PAGES]),
            total_pages,
            fd: file
        }
    }
}

impl AbstractPager {
    fn page_in_disk(&self, page_num: usize) -> bool {
        self.total_pages > page_num
    }

    fn read_page_from_disk(&self, page_index: usize) -> Page {
        let mut bytes = [0; PAGE_SIZE];
        self.fd.seek_read(&mut bytes, (page_index * PAGE_SIZE + BTREE_METADATA_SIZE) as u64).unwrap();
        bytes
    }

    fn flush_page_to_disk(&mut self, page_index: usize) -> bool {
        let page: Option<&Page> = self.pages[page_index].as_ref();

        if page.is_none() {
            return false;
        }

        self.fd.seek(SeekFrom::Start((page_index * PAGE_SIZE + BTREE_METADATA_SIZE) as u64)).unwrap();
        self.fd.write(page.unwrap()).unwrap();
        true
    }
}

impl Pager for AbstractPager {
    fn get_page(&self, page_index: usize) -> *const u8 {
        unsafe {
            let s_ptr: &mut Self = std::mem::transmute(self as *const Self);
            (*s_ptr).get_or_create_page(page_index).cast_const()
        }
    }


    fn get_or_create_page(&mut self, page_index: usize) -> *mut u8 {
        if page_index > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}\n", page_index, TABLE_MAX_PAGES);
            exit(1);
        }

        let page = self.pages[page_index];
        if page.is_none() {
            let loaded_page;
            if self.page_in_disk(page_index) {
                loaded_page = self.read_page_from_disk(page_index);
            } else {
                let new_page: Page = [0; PAGE_SIZE];
                loaded_page = new_page;
                self.total_pages += 1;
            }
            self.pages[page_index] = Some(loaded_page);
        }
        self.pages[page_index].as_mut().unwrap().as_mut_ptr()
    }
}

pub struct BtreePager {
    abstract_pager: AbstractPager,
    updated: [bool; TABLE_MAX_PAGES],
    size: usize,
    btree_leaf_node_body_layout: BtreeLeafNodeBodyLayout,
}

impl BtreePager {
    pub(crate) fn open(key_size: usize, row_size: usize, file: File) -> BtreePager {
        let size = file.metadata().unwrap().len() as usize;
        if (size - BTREE_METADATA_SIZE) % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(1);
        }
        let total_pages = size / PAGE_SIZE;
        BtreePager {
            abstract_pager: AbstractPager::new(total_pages, file),
            updated: [false; TABLE_MAX_PAGES],
            size,
            btree_leaf_node_body_layout: BtreeLeafNodeBodyLayout::new(key_size, row_size),
        }
    }

    pub(crate) fn get_or_create_page(&mut self, page_index: usize) -> *mut u8 {
        self.abstract_pager.get_or_create_page(page_index)
    }

    pub(crate) fn get_page(&self, page_index: usize) -> *const u8 {
        self.abstract_pager.get_page(page_index)
    }

    pub fn get_pager_total_size(&self) -> usize {
        self.size
    }

    pub fn get_body_layout(&self) -> &BtreeLeafNodeBodyLayout {
        &self.btree_leaf_node_body_layout
    }

    pub(crate) fn get_unused_page_num(&self) -> usize {
        self.abstract_pager.total_pages
    }

    pub(crate) fn get_node_type_by_index(&mut self, page_index: usize) -> NodeType {
        let page = self.abstract_pager.get_or_create_page(page_index);
        Self::get_node_type(page)
    }

    pub(crate) fn get_leaf_node_num_cells(page: *const u8) -> usize {
        unsafe {
            let page_ptr = page.add(LEAF_NODE_NUM_CELLS_OFFSET);
            let cells_num: usize = 0;
            ptr::copy_nonoverlapping(
                page_ptr,
                &cells_num as *const usize as *mut u8,
                LEAF_NODE_NUM_CELLS_SIZE,
            );
            cells_num
        }
    }

    pub(crate) fn set_leaf_node_cells_num(page: *mut u8, num: usize) {
        unsafe {
            ptr::copy_nonoverlapping(
                &num as *const usize as *mut u8,
                page.add(LEAF_NODE_NUM_CELLS_OFFSET),
                LEAF_NODE_NUM_CELLS_SIZE,
            );
        }
    }

    pub(crate) fn increment_leaf_node_cells_num(page: *mut u8) {
        unsafe {
            let page_ptr = page.add(LEAF_NODE_NUM_CELLS_OFFSET);
            let mut cells_num: usize = 0;
            ptr::copy_nonoverlapping(
                page_ptr,
                &mut cells_num as *mut usize as *mut u8,
                LEAF_NODE_NUM_CELLS_SIZE,
            );
            cells_num += 1;
            ptr::copy_nonoverlapping(
                &mut cells_num as *mut usize as *mut u8,
                page_ptr,
                LEAF_NODE_NUM_CELLS_SIZE,
            );
        }
    }

    pub(crate) fn get_leaf_node_cell_key(&self, page: *const u8, cell_index: usize, key_type: &DataType) -> Value {
        unsafe {
            Value::from_ptr(key_type, page.add(LEAF_NODE_BODY_OFFSET + cell_index * self.btree_leaf_node_body_layout.LEAF_NODE_CELL_SIZE))
        }
    }

    pub(crate) fn set_leaf_node_cell_key(&self, page: *mut u8, cell_index: usize, key_size: usize, key: &Value) {
        unsafe {
            let dst = page.add(LEAF_NODE_BODY_OFFSET + cell_index * self.btree_leaf_node_body_layout.LEAF_NODE_CELL_SIZE);
            Self::set_key(key_size, key, dst)
        }
    }

    pub(crate) fn leaf_node_cell(&self, page: *mut u8, cell_index: usize) -> *mut u8 {
        unsafe {
            let page_ptr = page.add(LEAF_NODE_BODY_OFFSET + cell_index * self.btree_leaf_node_body_layout.LEAF_NODE_CELL_SIZE);
            page_ptr
        }
    }

    pub(crate) fn get_leaf_node_value(&self, page: *mut u8, cell_index: usize) -> *mut u8 {
        let ptr = self.leaf_node_cell(page, cell_index);
        unsafe {
            ptr.add(self.btree_leaf_node_body_layout.LEAF_NODE_VALUE_OFFSET)
        }
    }

    pub(crate) fn get_node_type(ptr: *const u8) -> NodeType {
        unsafe {
            let node_type: u8 = 0;

            ptr::copy_nonoverlapping(
                ptr.add(NODE_TYPE_OFFSET),
                &node_type as *const u8 as *mut u8,
                NODE_TYPE_SIZE,
            );

            NodeType::from(node_type)
        }
    }

    fn set_node_type(page: *mut u8, node_type: NodeType) {
        unsafe {
            ptr::copy_nonoverlapping(
                &(node_type as u8) as *const u8 as *mut u8,
                page.add(NODE_TYPE_OFFSET),
                NODE_TYPE_SIZE,
            );
        }
    }

    pub(crate) fn is_root_node(page: *mut u8) -> bool {
        unsafe {
            let root = false;
            ptr::copy_nonoverlapping(
                page.add(IS_ROOT_OFFSET),
                &root as *const bool as *mut u8,
                IS_ROOT_SIZE,
            );

            root
        }
    }

    pub(crate) fn set_root_node(page: *mut u8, root: bool) {
        unsafe {
            ptr::copy_nonoverlapping(
                &root as *const bool as *mut u8,
                page.add(IS_ROOT_OFFSET),
                IS_ROOT_SIZE,
            );
        }
    }

    pub(crate) fn get_internal_node_cell(page: *mut u8, cell_index: usize) -> *mut u8 {
        unsafe {
            page.add(INTERNAL_NODE_BODY_OFFSET + cell_index * INTERNAL_NODE_CELL_SIZE)
        }
    }

    pub fn get_internal_node_num_keys(node: *const u8) -> usize {
        unsafe {
            let num: usize = 0;
            ptr::copy_nonoverlapping(node.add(INTERNAL_NODE_NUM_KEYS_OFFSET),
                                     &num as *const usize as *mut u8,
                                     INTERNAL_NODE_NUM_KEYS_SIZE);
            num
        }
    }

    pub fn set_internal_node_num_keys(node: *mut u8, num: usize) {
        unsafe {
            ptr::copy_nonoverlapping(&num as *const usize as *mut u8,
                                     node.add(INTERNAL_NODE_NUM_KEYS_OFFSET),
                                     INTERNAL_NODE_NUM_KEYS_SIZE);
        }
    }

    /*
        set a child into cells.
        each page can have multiple child cells
    */
    pub fn set_internal_node_child(node: *mut u8, child_index: usize, value: usize) {
        let num_keys = BtreePager::get_internal_node_num_keys(node);
        if child_index > num_keys {
            println!("Tried to access child_num {} > num_keys {}", child_index, num_keys);
            exit(1);
        } else if child_index == num_keys {
            BtreePager::set_internal_node_right_child(node, value);
        } else {
            BtreePager::set_internal_node_cell_child(node, child_index, value);
        }
    }

    pub fn get_internal_node_child(node: *const u8, child_index: usize) -> usize {
        let num_keys = BtreePager::get_internal_node_num_keys(node);
        if child_index > num_keys {
            println!("Tried to access child_num {} > num_keys {}", child_index, num_keys);
            exit(1);
        } else if child_index == num_keys {
            let right_child = BtreePager::get_internal_node_right_child(node);
            if right_child == INVALID_PAGE_NUM {
                println!("Tried to access right child of node, but was invalid page");
                exit(1);
            }
            right_child
        } else {
            let right_child = BtreePager::get_internal_node_cell_child(node, child_index);
            if right_child == INVALID_PAGE_NUM {
                println!("Tried to access child {} of node, but was invalid page", child_index);
                exit(1);
            }
            right_child
        }
    }

    pub fn get_internal_node_right_child(node: *const u8) -> usize {
        unsafe {
            let right_child_index: usize = 0;
            ptr::copy_nonoverlapping(node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET),
                                     &right_child_index as *const usize as *mut u8,
                                     INTERNAL_NODE_RIGHT_CHILD_SIZE);
            right_child_index
        }
    }

    pub fn set_internal_node_right_child(node: *mut u8, cell_index: usize) {
        unsafe {
            ptr::copy_nonoverlapping(&cell_index as *const usize as *mut u8,
                                     node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET),
                                     INTERNAL_NODE_RIGHT_CHILD_SIZE);
        }
    }

    pub fn set_internal_node_cell_child(node: *mut u8, cell_index: usize, child_index: usize) {
        unsafe {
            ptr::copy_nonoverlapping(&child_index as *const usize as *mut u8,
                                     node.add(INTERNAL_NODE_BODY_OFFSET + cell_index * INTERNAL_NODE_CELL_SIZE),
                                     INTERNAL_NODE_CHILD_SIZE);
        }
    }

    pub fn get_internal_node_cell_child(node: *const u8, cell_index: usize) -> usize {
        unsafe {
            let child_index: usize = 0;
            ptr::copy_nonoverlapping(
                node.add(INTERNAL_NODE_BODY_OFFSET + cell_index * INTERNAL_NODE_CELL_SIZE),
                &child_index as *const usize as *mut u8,
                INTERNAL_NODE_CHILD_SIZE);
            child_index
        }
    }

    pub fn get_internal_node_cell_key(node: *const u8, cell_index: usize, key_type: &DataType) -> Value {
        unsafe {
            let src = node.add(INTERNAL_NODE_BODY_OFFSET + cell_index * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE);
            Value::from_ptr(key_type, src)
        }
    }

    pub fn set_internal_node_cell_key(node: *mut u8, cell_index: usize, key_size: usize, key: &Value) {
        unsafe {
            let dst = node.add(INTERNAL_NODE_BODY_OFFSET + cell_index * INTERNAL_NODE_CELL_SIZE + INTERNAL_NODE_CHILD_SIZE);
            Self::set_key(key_size, key, dst)
        }
    }

    unsafe fn set_key(key_size: usize, key: &Value, dst: *mut u8) {
        match key {
            Value::STRING(string) => {
                let mut bytes = Vec::<u8>::with_capacity(key_size);
                bytes.as_mut_slice().copy_from_slice(string.as_bytes());
                ptr::copy_nonoverlapping(bytes.as_ptr(), dst, key_size);
            }
            Value::INTEGER(i) => {
                ptr::copy_nonoverlapping(i as *const i32 as *const u8, dst, key_size);
            }
            Value::FLOAT(f) => {
                ptr::copy_nonoverlapping(f as *const f32 as *const u8, dst, key_size);
            }
            Value::BOOLEAN(b) => {
                ptr::copy_nonoverlapping(b as *const bool as *const u8, dst, key_size);
            }
            _ => {}
        }
    }

    pub(crate) fn set_leaf_node_next_leaf(node: *mut u8, next_leaf: usize) {
        unsafe {
            ptr::copy_nonoverlapping(&next_leaf as *const usize as *mut u8,
                                     node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET),
                                     INTERNAL_NODE_RIGHT_CHILD_SIZE);
        }
    }

    pub(crate) fn get_leaf_node_next_leaf(node: *const u8) -> usize {
        unsafe {
            let next_leaf: usize = 0;
            ptr::copy_nonoverlapping(node.add(INTERNAL_NODE_RIGHT_CHILD_OFFSET),
                                     &next_leaf as *const usize as *mut u8,
                                     INTERNAL_NODE_RIGHT_CHILD_SIZE);
            next_leaf
        }
    }


    pub fn get_node_biggest_key(&mut self, node: *const u8, key_type: &DataType) -> Value {
        match BtreePager::get_node_type(node) {
            NodeType::Internal => {
                let right_child = self.abstract_pager.get_or_create_page(BtreePager::get_internal_node_right_child(node));
                self.get_node_biggest_key(right_child, key_type)
            }
            NodeType::Leaf => {
                self.get_leaf_node_cell_key(node, BtreePager::get_leaf_node_num_cells(node) - 1, key_type)
            }
        }
    }

    pub(crate) fn flush_page_to_disk(&mut self, page_num: usize) -> bool {
        self.abstract_pager.flush_page_to_disk(page_num)
    }

    pub(crate) fn mark_page_as_updated(&mut self, page_index: usize) {
        self.updated[page_index] = true;
    }

    pub(crate) fn initialize_leaf_node(node: *mut u8) {
        BtreePager::set_node_type(node, NodeType::Leaf);
        BtreePager::set_root_node(node, false);
        BtreePager::set_leaf_node_cells_num(node, 0);
        BtreePager::set_leaf_node_next_leaf(node, 0);
    }

    pub(crate) fn initialize_internal_node(node: *mut u8) {
        BtreePager::set_node_type(node, NodeType::Internal);
        BtreePager::set_root_node(node, false);
        BtreePager::set_internal_node_num_keys(node, 0);
        /*
         Necessary because the root page number is 0; by not initializing an internal
         node's right child to an invalid page number when initializing the node, we may
         end up with 0 as the node's right child, which makes the node a parent of the root
         */
        BtreePager::set_internal_node_right_child(node, INVALID_PAGE_NUM);
    }

    pub fn set_parent(node: *mut u8, parent_index: usize) {
        unsafe {
            ptr::copy_nonoverlapping(
                &parent_index as *const usize as *mut u8,
                node.add(PARENT_POINTER_OFFSET),
                PARENT_POINTER_SIZE,
            );
        }
    }

    pub fn get_parent(node: *const u8) -> usize {
        unsafe {
            let parent_index: usize = 0;
            ptr::copy_nonoverlapping(
                node.add(PARENT_POINTER_OFFSET),
                &parent_index as *const usize as *mut u8,
                PARENT_POINTER_SIZE,
            );
            parent_index
        }
    }
}

pub struct SequentialPager {
    abstract_pager: AbstractPager,
    size: usize
}

impl SequentialPager {
    pub(crate) fn open(file: File) -> SequentialPager {
        let size = file.metadata().unwrap().len() as usize;
        if (size - SEQUENTIAL_NODE_HEADER_SIZE) % PAGE_SIZE != 0 {
            panic!("Db file is not a whole number of pages. Corrupt file.");
        }
        let total_pages = size / PAGE_SIZE;
        SequentialPager {
            abstract_pager: AbstractPager::new(total_pages, file),
            size
        }
    }

    pub fn get_pager_total_size(&self) -> usize {
        self.size
    }

    pub fn get_num_cells(page: *const u8) -> usize {
        unsafe {
            let cells_num: usize = 0;
            ptr::copy_nonoverlapping(
                page,
                &cells_num as *const usize as *mut u8,
                SEQUENTIAL_CELLS_NUM_SIZE,
            );
            cells_num
        }
    }

    pub(crate) fn get_or_create_page(&mut self, page_index: usize) -> *mut u8 {
        self.abstract_pager.get_or_create_page(page_index)
    }

    pub(crate) fn get_page(&self, page_index: usize) -> *const u8 {
        self.abstract_pager.get_page(page_index)
    }

    pub fn get_total_page(&self) -> usize {
        self.abstract_pager.total_pages
    }

    pub(crate) fn get_row_value(&self, page: *const u8, cell_index: usize, row_size: usize) -> *const u8 {
        unsafe {
            page.add(SEQUENTIAL_NODE_BODY_OFFSET + cell_index * row_size)
        }
    }

    pub(crate) fn get_row_value_mut(&mut self, page: *mut u8, cell_index: usize, row_size: usize) -> *mut u8 {
        unsafe {
            page.add(SEQUENTIAL_NODE_BODY_OFFSET + cell_index * row_size)
        }
    }

    pub fn flush_page_to_disk(&mut self, page_index: usize) -> bool {
        self.abstract_pager.flush_page_to_disk(page_index)
    }

    pub fn increment_cells_num(&mut self, page_index: usize){
        let page_ptr = self.get_or_create_page(page_index);
        let mut cells_num: usize = 0;
        unsafe {
            ptr::copy_nonoverlapping(
                page_ptr,
                &mut cells_num as *mut usize as *mut u8,
                SEQUENTIAL_CELLS_NUM_SIZE,
            );
            cells_num += 1;
            ptr::copy_nonoverlapping(
                &mut cells_num as *mut usize as *mut u8,
                page_ptr,
                SEQUENTIAL_CELLS_NUM_SIZE,
            );
        }
    }
}