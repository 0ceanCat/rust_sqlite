extern crate core;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::process::{exit};
use std::{fs, ptr};
use std::cell::OnceCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;
use crate::build_path;
use crate::sql_engine::sql_structs::{ConditionExpr, DataType, FieldDefinition, Operator, Value};
use crate::utils::utils::{copy, indent, u8_array_to_string};
use crate::storage_engine::config::*;
use crate::storage_engine::cursor::Cursor;
use crate::storage_engine::enums::*;

trait ToU8 {
    fn to_u8(&self) -> u8;
}

impl ToU8 for bool {
    fn to_u8(&self) -> u8 {
        if *self { 1 } else { 0 }
    }
}

pub struct TableManager {
    tables: HashMap<String, Box<dyn Table>>,
    metadata: HashMap<String, TableStructureMetadata>,
}

impl TableManager {
    pub fn new() -> TableManager {
        TableManager {
            tables: HashMap::new(),
            metadata: HashMap::new(),
        }
    }

    pub fn get_or_load_table(&mut self, table_name: &str, path: &Path) -> Result<&mut Box<dyn Table>, String> {
        let path_str = String::from(path.to_str().unwrap());
        if !self.tables.contains_key(&path_str) {
            let table = BtreeTable::new(path_str.as_str(), self.get_table_metadata(table_name)?)?;
            self.tables.insert(path_str.clone(), Box::new(table));
        }

        Ok(self.tables.get_mut(&path_str).unwrap())
    }

    pub fn is_field_of_table(&mut self, table_name: &str, field_name: &str) -> bool {
        self.get_field_metadata(table_name, field_name).is_ok()
    }

    pub fn get_field_metadata(&mut self, table_name: &str, field_name: &str) -> Result<&FieldMetadata, String> {
        let map = self.get_or_load_metadata(table_name)?;

        map.get_field_metadata(field_name)
    }

    pub fn get_table_metadata(&mut self, table_name: &str) -> Result<&TableStructureMetadata, String> {
        let map = self.get_or_load_metadata(table_name)?;
        Ok(map)
    }

    fn get_or_load_metadata(&mut self, table_name: &str) -> Result<&TableStructureMetadata, String> {
        let path = build_path!(DATA_FOLDER, table_name, table_name.to_owned() + "_frm");
        if !self.metadata.contains_key(table_name) {
            let metadata = unsafe { Self::load_metadata(&path)? };
            self.metadata.insert(String::from(table_name), TableStructureMetadata::new(table_name, metadata));
        }

        Ok(self.metadata.get(table_name).unwrap())
    }

    pub fn flash_to_disk(&mut self) {
        for table in self.tables.values_mut() {
            table.flush_to_disk();
        }
    }

    pub fn print_btree(&mut self, table_name: &str) {
        match self.tables.get_mut(table_name) {
            None => {}
            Some(table) => {
                table.print_tree(0, 0)
            }
        }
    }

    pub fn create_table(&self, table_name: &str, field_definitions: &Vec<FieldDefinition>) -> Result<(), String> {
        let frm_path = build_path!(DATA_FOLDER, table_name, table_name.to_owned() + "_frm");

        if Path::new(&frm_path).exists() {
            return Err(format!("Table {} already exists.", table_name));
        }

        match File::create(frm_path) {
            Ok(mut file) => {
                Self::write_metadata(&mut file, table_name, field_definitions)
            }
            Err(_) => {
                return Err(String::from("Can not open create table."));
            }
        }
    }

    fn write_metadata(fd: &mut File, table_name: &str, field_definitions: &Vec<FieldDefinition>) -> Result<(), String> {
        let mut total_size = 0;
        total_size += FIELD_NUMBER_SIZE;
        total_size += field_definitions.len() * FIELD_NAME_SIZE;
        total_size += field_definitions.len() * FIELD_TYPE_PRIMARY;

        field_definitions.iter().for_each(|field_definition| {
            total_size += field_definition.data_type.get_size();
        });

        let mut vec = Vec::<u8>::with_capacity(total_size);
        let buf = vec.as_mut_ptr();
        let mut buf_pointer = 0; // pointer that points to the position where we should start reading

        copy(field_definitions.len() as *const u8, buf, FIELD_NUMBER_SIZE);
        buf_pointer += FIELD_NUMBER_SIZE;

        field_definitions.iter().for_each(|field_definition| unsafe {
            copy(field_definition.field_name.as_ptr(), buf.add(buf_pointer), FIELD_NAME_SIZE);
            buf_pointer += FIELD_NAME_SIZE;
            let data_type_primary: u8 = (field_definition.data_type.to_bit_code() << 1) | field_definition.is_primary_key.to_u8();
            copy(data_type_primary as *mut u8, buf.add(buf_pointer), FIELD_TYPE_PRIMARY);
            buf_pointer += FIELD_TYPE_PRIMARY;
            match field_definition.data_type {
                DataType::TEXT(size) => {
                    copy(size as *const usize as *mut u8, buf.add(buf_pointer), TEXT_SIZE);
                    buf_pointer += TEXT_SIZE;
                }
                _ => {}
            }
        });

        unsafe {
            vec.set_len(total_size);
        }

        if fd.write(vec.as_slice()).is_err() {
            return Err(format!("Can not write metadata for table {}!", table_name));
        };
        Ok(())
    }

    unsafe fn load_metadata(path: &Path) -> Result<HashMap<String, FieldMetadata>, String> {
        let metadata = fs::read(path).unwrap();

        let mut metadata_pointer = 0; // pointer that points to the position where we should start reading

        let ptr = metadata.as_ptr();
        let fields_number: usize = 0;
        copy(ptr, fields_number as *const usize as *mut u8, FIELD_NUMBER_SIZE);
        metadata_pointer += FIELD_NUMBER_SIZE;
        let mut map: HashMap<String, FieldMetadata> = HashMap::with_capacity(fields_number);

        let data_type_mask: u8 = 0b0000_0000;
        let primary: u8 = 0b0000_0001;
        let mut value_offset = 0; // offset of the current field's value

        for _ in 0..fields_number {
            let field_type_primary: u8 = 0;
            copy(ptr.add(metadata_pointer), field_type_primary as *mut u8, FIELD_TYPE_PRIMARY);
            metadata_pointer += FIELD_TYPE_PRIMARY;

            let data_type_bit_code = (field_type_primary >> 1) | data_type_mask;
            let mut size: usize = 0;

            let data_type = match DataType::from_bit_code(data_type_bit_code)? {
                DataType::TEXT(_) => {
                    copy(ptr.add(metadata_pointer), size as *const usize as *mut u8, TEXT_SIZE);
                    metadata_pointer += TEXT_SIZE;
                    DataType::TEXT(size)
                }
                DataType::INTEGER => {
                    size = INTEGER_SIZE;
                    DataType::INTEGER
                }
                DataType::FLOAT => {
                    size = FLOAT_SIZE;
                    DataType::FLOAT
                }
                DataType::BOOLEAN => {
                    size = BOOLEAN_SIZE;
                    DataType::BOOLEAN
                }
            };


            let is_primary = (field_type_primary & primary) == 1;

            let mut buf: [u8; FIELD_NAME_SIZE] = [0; FIELD_NAME_SIZE];

            copy(ptr.add(metadata_pointer), buf.as_mut_ptr(), FIELD_NAME_SIZE);
            metadata_pointer += FIELD_NAME_SIZE;

            let definition = FieldDefinition::new(u8_array_to_string(&buf), data_type, is_primary);

            map.insert(u8_array_to_string(&buf), FieldMetadata::new(definition, value_offset, size));

            value_offset += size;
        }

        Ok(map)
    }

    fn load_data_type(byte: u8) {
        let second_bit_mask: u8 = 0b0000_0010;
        let third_bit_mask: u8 = 0b0000_0100;

        let second_bit = (byte & second_bit_mask) != 0;
        let third_bit = (byte & third_bit_mask) != 0;

        if !second_bit && !third_bit {
            println!("Flag 0 is set");
        } else if !second_bit && third_bit {
            println!("Flag 1 is set");
        } else if second_bit && !third_bit {
            println!("Flag 2 is set");
        } else {
            println!("Flag 3 is set");
        }
    }
}

pub struct BtreePager {
    pages: [Option<Page>; TABLE_MAX_PAGES],
    updated: [bool; TABLE_MAX_PAGES],
    fd: File,
    size: usize,
    total_pages: usize,
    btree_leaf_node_body_layout: BtreeLeafNodeBodyLayout,
}

impl BtreePager {
    pub(crate) fn open(key_size: usize, row_size: usize, file: File) -> BtreePager {
        let size = file.metadata().unwrap().len() as usize;
        if size % PAGE_SIZE != 0 {
            println!("Db file is not a whole number of pages. Corrupt file.");
            exit(1);
        }
        let mut total_pages = size / PAGE_SIZE;
        BtreePager {
            pages: [None; TABLE_MAX_PAGES],
            fd: file,
            updated: [false; TABLE_MAX_PAGES],
            size,
            total_pages,
            btree_leaf_node_body_layout: BtreeLeafNodeBodyLayout::new(key_size, row_size),
        }
    }

    pub(crate) fn get_unused_page_num(&self) -> usize {
        self.total_pages
    }

    fn get_node_type_by_index(&mut self, page_index: usize) -> NodeType {
        let page = self.get_page_or_create(page_index);
        Self::get_node_type(page)
    }

    pub(crate) fn get_page(&self, page_index: usize) -> *const u8 {
        if page_index > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}\n", page_index, TABLE_MAX_PAGES);
            exit(1);
        }
        self.pages[page_index].unwrap().as_mut_ptr()
    }

    pub(crate) fn get_page_or_create(&mut self, page_index: usize) -> *mut u8 {
        if page_index > TABLE_MAX_PAGES {
            println!("Tried to fetch page number out of bounds. {} > {}\n", page_index, TABLE_MAX_PAGES);
            exit(1);
        }

        let page = self.pages[page_index];
        if page.is_none() {
            let loaded_page;
            if self.page_in_disk(page_index) {
                loaded_page = self.read_page_from_disk((page_index * PAGE_SIZE) as u64);
            } else {
                let new_page: Page = [0; PAGE_SIZE];
                loaded_page = new_page;
                self.total_pages += 1;
            }
            self.pages[page_index] = Some(loaded_page);
        }
        self.pages[page_index].as_mut().unwrap().as_mut_ptr()
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

    pub fn set_internal_node_cell_child(node: *mut u8, child_index: usize, key: usize) {
        unsafe {
            ptr::copy_nonoverlapping(&key as *const usize as *mut u8,
                                     node.add(INTERNAL_NODE_BODY_OFFSET + child_index * INTERNAL_NODE_CELL_SIZE),
                                     INTERNAL_NODE_CHILD_SIZE);
        }
    }

    pub fn get_internal_node_cell_child(node: *const u8, child_index: usize) -> usize {
        unsafe {
            let key: usize = 0;
            ptr::copy_nonoverlapping(
                node.add(INTERNAL_NODE_BODY_OFFSET + child_index * INTERNAL_NODE_CELL_SIZE),
                &key as *const usize as *mut u8,
                INTERNAL_NODE_CHILD_SIZE);
            key
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
                let right_child = self.get_page_or_create(BtreePager::get_internal_node_right_child(node));
                self.get_node_biggest_key(right_child, key_type)
            }
            NodeType::Leaf => {
                self.get_leaf_node_cell_key(node, BtreePager::get_leaf_node_num_cells(node) - 1, key_type)
            }
        }
    }

    fn read_page_from_disk(&self, offset: u64) -> Page {
        let mut bytes = [0; PAGE_SIZE];
        self.fd.seek_read(&mut bytes, offset).unwrap();
        bytes
    }

    fn page_in_disk(&self, page_num: usize) -> bool {
        self.total_pages > page_num
    }

    fn flush_page_to_disk(&mut self, page_num: usize) -> bool {
        let page: Option<&Page> = self.pages[page_num].as_ref();

        if page.is_none() {
            return false;
        }

        self.fd.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)).unwrap();
        self.fd.write(page.unwrap()).unwrap();
        true
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
    pages: [Option<Page>; TABLE_MAX_PAGES],
    updated: [bool; TABLE_MAX_PAGES],
    fd: File,
    size: usize,
    total_pages: usize,
}

impl SequentialPager {}

pub trait Table {
    fn begin(&mut self) -> Cursor;
    fn insert(&mut self, page_index: usize, cell_index: usize, row: &Row);
    fn find_by_condition(&mut self, condition_expr: &ConditionExpr) -> Cursor;
    fn end(&mut self) -> Cursor;
    fn is_index_table(&self) -> bool;
    fn get_row_size(&self) -> usize;
    fn get_num_cells(&self, page_index: usize) -> usize;
    fn get_next_page(&self, page_index: usize) -> usize;
    fn get_row_value(&mut self, page_index: usize, cell_index: usize) -> *mut u8;
    fn flush_to_disk(&self);
    fn print_tree(&self, page_index: usize, cell_index: usize);
}

pub struct BtreeTable {
    pub root_page_index: usize,
    pub pager: BtreePager,
    pub is_primary: bool,
    pub key_type: DataType,
    pub key_size: usize,
    pub key_offset_in_row: usize,
    pub row_size: usize,
}

impl Table for BtreeTable {
    fn begin(&mut self) -> Cursor {
        self.find_smallest_or_biggest_key(false)
    }

    fn insert(&mut self, page_index: usize, cell_index: usize, row: &Row) {
        let page = self.pager.get_page_or_create(page_index);
        let num_cells = BtreePager::get_leaf_node_num_cells(page);

        if num_cells >= self.pager.btree_leaf_node_body_layout.LEAF_NODE_MAX_CELLS {
            self.split_and_insert(page_index, cell_index, &row);
        } else {
            self.move_and_insert(page_index, cell_index, &row);
        }
    }

    fn find_by_condition(&mut self, condition_expr: &ConditionExpr) -> Cursor {
        todo!()
    }

    fn end(&mut self) -> Cursor {
        self.find_smallest_or_biggest_key(true)
    }

    fn is_index_table(&self) -> bool {
        true
    }

    fn get_row_size(&self) -> usize {
        self.row_size
    }

    fn get_num_cells(&self, page_index: usize) -> usize {
        BtreePager::get_leaf_node_num_cells(self.pager.get_page(page_index))
    }

    fn get_next_page(&self, page_index: usize) -> usize {
        BtreePager::get_leaf_node_next_leaf(self.pager.get_page(page_index))
    }

    fn get_row_value(&mut self, page_index: usize, cell_index: usize) -> *mut u8 {
        let page = self.pager.get_page_or_create(page_index);
        self.pager.get_leaf_node_value(page, cell_index)
    }

    fn flush_to_disk(&self) {
        todo!()
    }

    fn print_tree(&self, page_index: usize, cell_index: usize) {
        todo!()
    }
}

impl BtreeTable {
    pub(crate) fn new(path: &str, table_metadata: &TableStructureMetadata) -> Result<BtreeTable, String> {
        match OpenOptions::new().create(true).read(true).write(true).open(path) {
            Ok(mut file) => {
                let (is_primary, data_type, key_size, key_name) = Self::load_metadata(&mut file)?;
                let pager = BtreePager::open(key_size, table_metadata.row_size, file);
                let mut pager = pager;
                if pager.size == 0 {
                    let first_page = pager.get_page_or_create(0);
                    BtreePager::initialize_leaf_node(first_page);
                    BtreePager::set_root_node(first_page, true);
                }
                Ok(BtreeTable {
                    root_page_index: 0,
                    pager,
                    is_primary,
                    key_type: data_type,
                    key_size,
                    key_offset_in_row: table_metadata.get_field_metadata(&key_name)?.offset,
                    row_size: table_metadata.row_size,
                })
            }
            Err(_) => {
                Err(format!("Can not open index data file of table {}!", table_metadata.table_name))
            }
        }
    }

    fn load_metadata(file: &mut File) -> Result<(bool, DataType, usize, String), String> {
        let mut metadata: [u8; BTREE_METADATA_SIZE] = [0; BTREE_METADATA_SIZE];
        file.read(&mut metadata);

        let data_type_mask: u8 = 0b0000_0000;
        let primary_mask: u8 = 0b0000_0001;
        let field_type_primary: u8 = 0;

        copy(metadata.as_ptr(), field_type_primary as *mut u8, INDEXED_FIELD_TYPE_PRIMARY);
        let data_type_bit_code = (field_type_primary >> 1) | data_type_mask;
        let is_primary = (field_type_primary & primary_mask) == 1;

        let data_type = DataType::from_bit_code(data_type_bit_code)?;
        let key_size: usize = 0;
        let mut key_name: [u8; INDEXED_FIELD_NAME_SIZE] = [0; INDEXED_FIELD_NAME_SIZE];
        unsafe {
            copy(metadata.as_ptr().add(INDEXED_FIELD_SIZE_OFFSET), key_size as *const usize as *mut u8, INDEXED_FIELD_SIZE);
            copy(metadata.as_ptr().add(INDEXED_FIELD_NAME_SIZE_OFFSET), key_name.as_mut_ptr(), INDEXED_FIELD_NAME_SIZE);
        }
        let key_name = u8_array_to_string(&key_name);
        Ok((is_primary, data_type, key_size, key_name))
    }

    fn split_and_insert(&mut self, page_index: usize, cell_index: usize, row: &Row) {
        /*
         Create a new node and move half the cells over.
         Insert the new value in one of the two nodes.
         Update parent or create a new parent.
       */
        let old_node = self.pager.get_page_or_create(page_index);
        let old_biggest_key = self.pager.get_node_biggest_key(old_node, &self.key_type);
        let new_page_index = self.pager.get_unused_page_num();
        let new_node = self.pager.get_page_or_create(new_page_index);
        BtreePager::initialize_leaf_node(new_node);

        BtreePager::set_parent(new_node, BtreePager::get_parent(old_node.cast_const()));

        BtreePager::set_leaf_node_next_leaf(new_node, BtreePager::get_leaf_node_next_leaf(old_node.cast_const()));
        BtreePager::set_leaf_node_next_leaf(old_node, new_page_index);

        /*
          All existing keys plus new key should be divided
          evenly between old (left) and new (right) nodes.
          Starting from the right, move each key to correct position.
        */
        for i in (0..=self.pager.btree_leaf_node_body_layout.LEAF_NODE_MAX_CELLS).rev() {
            let destination_node;
            if i >= self.pager.btree_leaf_node_body_layout.LEAF_NODE_LEFT_SPLIT_COUNT {
                // upper halves (right halves) will be stored in the new_node
                destination_node = new_node;
            } else {
                destination_node = old_node;
            }
            // index_within_node will always decrement until it arrives to 0, then destination_node will be switched to old_node
            let index_within_node = i % self.pager.btree_leaf_node_body_layout.LEAF_NODE_LEFT_SPLIT_COUNT;
            let cell_pointer = self.pager.leaf_node_cell(destination_node, index_within_node);

            if i == cell_index {
                // when this code executes, the value in the cell_pointer was already moved to position i + 1, if cell_pointer is old_node
                // if cell_pointer is new_node, position `index_within_node` is empty
                let key = row.read_key(&self.key_type, self.key_offset_in_row, self.key_size);
                self.pager.set_leaf_node_cell_key(destination_node, index_within_node, self.key_size, &key);
                row.serialize_row(self.pager.get_leaf_node_value(destination_node, index_within_node));
            } else if i > cell_index {
                // copy a node from old_node tail (position i - 1), to destination_node (index_within_node)
                copy(self.pager.leaf_node_cell(old_node, i - 1), cell_pointer, self.pager.btree_leaf_node_body_layout.LEAF_NODE_CELL_SIZE);
            } else {
                copy(self.pager.leaf_node_cell(old_node, i), cell_pointer, self.pager.btree_leaf_node_body_layout.LEAF_NODE_CELL_SIZE);
            }
        }

        BtreePager::set_leaf_node_cells_num(old_node, self.pager.btree_leaf_node_body_layout.LEAF_NODE_LEFT_SPLIT_COUNT);
        BtreePager::set_leaf_node_cells_num(new_node, self.pager.btree_leaf_node_body_layout.LEAF_NODE_RIGHT_SPLIT_COUNT);

        if BtreePager::is_root_node(old_node) {
            self.create_new_root(new_page_index);
        } else {
            let parent_page_index = BtreePager::get_parent(old_node.cast_const());
            let new_biggest = self.pager.get_node_biggest_key(old_node, &self.key_type);
            let parent_page = self.pager.get_page_or_create(parent_page_index);

            let old_key_cell_index = self.internal_node_find_child(parent_page, &old_biggest_key);
            // old_node is split and contains left halves rows (lower halves)
            // so it's necessary to replace old_biggest_key to new_biggest_key
            BtreePager::set_internal_node_cell_key(parent_page, old_key_cell_index, self.key_size, &new_biggest);
            self.internal_node_insert(parent_page_index, new_page_index);
        }
    }

    fn move_and_insert(&mut self, page_index: usize, cell_index: usize, row: &Row) {
        let page = self.pager.get_page_or_create(page_index);
        let num_cells = BtreePager::get_leaf_node_num_cells(page);
        if cell_index < num_cells {
            copy(self.pager.leaf_node_cell(page, cell_index),
                 self.pager.leaf_node_cell(page, cell_index + 1),
                 self.pager.btree_leaf_node_body_layout.LEAF_NODE_CELL_SIZE * (num_cells - cell_index))
        }
        let key = row.read_key(&self.key_type, self.key_offset_in_row, self.key_size);
        self.pager.set_leaf_node_cell_key(page, cell_index, self.key_size, &key);
        BtreePager::increment_leaf_node_cells_num(page);
        self.pager.mark_page_as_updated(page_index);
        row.serialize_row(self.pager.get_leaf_node_value(page, cell_index));
    }

    pub fn read_all(&mut self) -> Vec<Row> {
        let row_size = self.row_size;
        let mut cursor = self.begin();
        let mut result = Vec::new();
        while !cursor.is_end() {
            result.push(Row::deserialize_row(cursor.cursor_value(), row_size));
            cursor.cursor_advance();
        }
        result
    }

    pub(crate) fn table_find_by_key(&mut self, key: Value) -> Cursor {
        let node_type = self.pager.get_node_type_by_index(self.root_page_index);
        match node_type {
            NodeType::Internal => {
                self.internal_node_find(self.root_page_index, &key)
            }
            NodeType::Leaf => {
                self.leaf_node_find(self.root_page_index, &key)
            }
        }
    }

    pub(crate) fn find_smallest_or_biggest_key(&mut self, biggest: bool) -> Cursor {
        let node_type = self.pager.get_node_type_by_index(self.root_page_index);
        match node_type {
            NodeType::Internal => {
                self.internal_node_find_smallest_or_biggest(self.root_page_index, biggest)
            }
            NodeType::Leaf => {
                self.leaf_node_find_smallest_or_biggest(self.root_page_index, biggest)
            }
        }
    }

    fn leaf_node_find(&mut self, page_index: usize, key: &Value) -> Cursor {
        let node = self.pager.get_page_or_create(page_index);
        let cells_num = BtreePager::get_leaf_node_num_cells(node);

        let mut min_index = 0;
        let mut right = cells_num;
        while right != min_index {
            let index = (min_index + right) / 2;
            let key_at_index = self.pager.get_leaf_node_cell_key(node, index, &self.key_type);
            if *key == key_at_index {
                return Cursor::at(self, page_index, index);
            }
            if *key < key_at_index {
                right = index;
            } else {
                min_index = index + 1;
            }
        }

        Cursor::at(self, page_index, min_index)
    }

    fn leaf_node_find_smallest_or_biggest(&mut self, page_index: usize, biggest: bool) -> Cursor {
        let mut cell_index = 0;
        if biggest {
            cell_index = BtreePager::get_leaf_node_num_cells(self.pager.get_page_or_create(page_index))
        }
        Cursor::at(self, page_index, cell_index)
    }

    pub fn internal_node_find_child(&mut self, node: *const u8, key: &Value) -> usize {
        /*
          Return the index of the child which should contain
          the given key.
        */
        let num_keys = BtreePager::get_internal_node_num_keys(node);
        let mut min_index = 0;
        let mut max_index = num_keys;
        while max_index != min_index {
            let index = (min_index + max_index) / 2;
            let key_at_index = BtreePager::get_internal_node_cell_key(node, index, &self.key_type);
            if *key <= key_at_index {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        min_index
    }

    pub fn internal_node_find_smallest_or_biggest(&mut self, page_index: usize, biggest: bool) -> Cursor {
        /*
          Return the index of the child which contains the smallest key
        */
        let node = self.pager.get_page_or_create(page_index);

        let mut key_index: usize = 0;

        if biggest {
            key_index = BtreePager::get_internal_node_num_keys(node) - 1;
        }

        let child_index = BtreePager::get_internal_node_child(node, key_index);
        let child = self.pager.get_page_or_create(child_index);

        match BtreePager::get_node_type(child) {
            NodeType::Leaf => {
                self.leaf_node_find_smallest_or_biggest(child_index, biggest)
            }
            NodeType::Internal => {
                self.internal_node_find_smallest_or_biggest(child_index, biggest)
            }
        }
    }

    fn internal_node_find(&mut self, page_index: usize, key: &Value) -> Cursor {
        let node = self.pager.get_page_or_create(page_index);
        let cell_index = self.internal_node_find_child(node, key);
        let child_index = BtreePager::get_internal_node_child(node, cell_index);
        let child = self.pager.get_page_or_create(child_index);
        match BtreePager::get_node_type(child) {
            NodeType::Leaf => {
                self.leaf_node_find(child_index, key)
            }
            NodeType::Internal => {
                self.internal_node_find(child_index, key)
            }
        }
    }

    pub(crate) fn flush_to_disk(&mut self) {
        for x in 0..TABLE_MAX_PAGES {
            if !self.pager.flush_page_to_disk(x) {
                break;
            }
        }
    }

    pub(crate) fn create_new_root(&mut self, right_child_page_index: usize) {
        /*
          Handle splitting the root.
          Old root copied to new page, becomes left child.
          Address of right child passed in.
          Re-initialize root page to contain the new root node.
          New root node points to two children.
        */
        let root = self.pager.get_page_or_create(self.root_page_index);
        let right_child = self.pager.get_page_or_create(right_child_page_index);
        let left_child_page_num = self.pager.get_unused_page_num();
        let left_child = self.pager.get_page_or_create(left_child_page_num);

        if let NodeType::Internal = BtreePager::get_node_type(root) {
            BtreePager::initialize_internal_node(right_child);
            BtreePager::initialize_internal_node(left_child);
        }

        /* Left child has data copied from old root */
        unsafe {
            ptr::copy_nonoverlapping(root, left_child, PAGE_SIZE);
            BtreePager::set_root_node(left_child, false)
        };

        if let NodeType::Internal = BtreePager::get_node_type(left_child) {
            let mut child: *mut u8;
            let num_keys = BtreePager::get_internal_node_num_keys(left_child);
            for i in 0..num_keys {
                child = self.pager.get_page_or_create(BtreePager::get_internal_node_child(left_child, i));
                BtreePager::set_parent(child, left_child_page_num);
            }
            child = self.pager.get_page_or_create(BtreePager::get_internal_node_right_child(left_child));
            BtreePager::set_parent(child, left_child_page_num);
        }


        /* Root node is a new internal node with one key and two children */
        BtreePager::initialize_internal_node(root);
        BtreePager::set_root_node(root, true);

        BtreePager::set_internal_node_num_keys(root, 1);
        // first child index = left child index
        BtreePager::set_internal_node_child(root, 0, left_child_page_num);
        let left_child_biggest_key = self.pager.get_node_biggest_key(left_child, &self.key_type);
        BtreePager::set_internal_node_cell_key(root, 0, self.key_size, &left_child_biggest_key);
        BtreePager::set_internal_node_right_child(root, right_child_page_index);

        BtreePager::set_parent(left_child, self.root_page_index);
        BtreePager::set_parent(right_child, self.root_page_index);
    }

    pub fn internal_node_split_and_insert(&mut self, parent_page_index: usize, child_page_index: usize) {
        let mut old_page_index = parent_page_index;
        let mut old_node = self.pager.get_page_or_create(parent_page_index);
        let old_max = self.pager.get_node_biggest_key(old_node, &self.key_type);

        let child = self.pager.get_page_or_create(child_page_index);
        let child_max = self.pager.get_node_biggest_key(child, &self.key_type);

        let new_page_index = self.pager.get_unused_page_num();
        /*
             Declaring a flag before updating pointers which
             records whether this operation involves splitting the root -
             if it does, we will insert our newly created node during
             the step where the table's new root is created. If it does
             not, we have to insert the newly created node into its parent
             after the old node's keys have been transferred over. We are not
             able to do this if the newly created node's parent is not a newly
             initialized root node, because in that case its parent may have existing
             keys aside from our old node which we are splitting. If that is true, we
             need to find a place for our newly created node in its parent, and we
             cannot insert it at the correct index if it does not yet have any keys
         */
        let splitting_root = BtreePager::is_root_node(old_node);
        let parent;
        let mut new_node: *mut u8 = null_mut();
        if splitting_root {
            self.create_new_root(new_page_index);
            parent = self.pager.get_page_or_create(self.root_page_index);
            /*
           If we are splitting the root, we need to update old_node to point
           to the new root's left child, new_page_num will already point to
           the new root's right child
            */
            old_page_index = BtreePager::get_internal_node_child(parent.cast_const(), 0);
            old_node = self.pager.get_page_or_create(old_page_index);
        } else {
            parent = self.pager.get_page_or_create(BtreePager::get_parent(old_node.cast_const()));
            new_node = self.pager.get_page_or_create(new_page_index);
            BtreePager::initialize_internal_node(new_node);
        }

        let mut old_num_keys = BtreePager::get_internal_node_num_keys(old_node.cast_const());

        let mut cur_page_num = BtreePager::get_internal_node_right_child(old_node.cast_const());
        let mut cur = self.pager.get_page_or_create(cur_page_num);

        /*
          First put right child into new node and set right child of old node to invalid page number
          */
        self.internal_node_insert(new_page_index, cur_page_num);
        BtreePager::set_parent(cur, new_page_index);
        BtreePager::set_internal_node_right_child(old_node, INVALID_PAGE_NUM);
        /*
         For each key until you get to the middle key, move the key and the child to the new node
         */
        for i in (INTERNAL_NODE_MAX_KEYS / 2 + 1..INTERNAL_NODE_MAX_KEYS - 1).rev() {
            cur_page_num = BtreePager::get_internal_node_child(old_node, i);
            cur = self.pager.get_page_or_create(cur_page_num);

            self.internal_node_insert(new_page_index, cur_page_num);
            BtreePager::set_parent(cur, new_page_index);
            old_num_keys -= 1;
            BtreePager::set_internal_node_num_keys(old_node, old_num_keys);
        }

        /*
          Set child before middle key, which is now the highest key, to be node's right child,
          and decrement number of keys
        */
        BtreePager::set_internal_node_right_child(old_node, BtreePager::get_internal_node_child(old_node, old_num_keys - 1));

        old_num_keys -= 1;
        BtreePager::set_internal_node_num_keys(old_node, old_num_keys);

        /*
      Determine which of the two nodes after the split should contain the child to be inserted,
      and insert the child
      */
        let max_after_split = self.pager.get_node_biggest_key(old_node, &self.key_type);

        let destination_page_index = if child_max < max_after_split {
            old_page_index
        } else {
            new_page_index
        };

        self.internal_node_insert(destination_page_index, child_page_index);
        BtreePager::set_parent(child, destination_page_index);

        let old_key_cell_index = self.internal_node_find_child(parent, &old_max);
        BtreePager::set_internal_node_cell_key(parent, old_key_cell_index, self.key_size, &self.pager.get_node_biggest_key(old_node, &self.key_type));

        if !splitting_root {
            self.internal_node_insert(BtreePager::get_parent(old_node), new_page_index);
            BtreePager::set_parent(new_node, BtreePager::get_parent(old_node));
        }
    }

    pub fn print_tree(&mut self, page_num: usize, indentation_level: usize) {
        let node = self.pager.get_page(page_num);
        match BtreePager::get_node_type(node) {
            NodeType::Leaf => {
                indent(indentation_level);
                println!("* node {:p}, index: {}: ", node, page_num);
                let num_keys = BtreePager::get_leaf_node_num_cells(node);
                indent(indentation_level + 1);
                println!("- leaf (size {})", num_keys);
                for i in 0..num_keys {
                    indent(indentation_level + 2);
                    println!("- {:?}", self.pager.get_leaf_node_cell_key(node, i, &self.key_type));
                }
            }
            NodeType::Internal => {
                let num_keys = BtreePager::get_internal_node_num_keys(node);
                indent(indentation_level);
                println!("- internal (size {})", num_keys);
                if num_keys > 0 {
                    let mut child: usize = 0;
                    for i in 0..num_keys {
                        let child = BtreePager::get_internal_node_child(node, i);
                        self.print_tree(child, indentation_level + 1);

                        indent(indentation_level + 1);
                        println!("- key {:?}", BtreePager::get_internal_node_cell_key(node, i, &self.key_type));
                    }
                    child = BtreePager::get_internal_node_right_child(node);
                    self.print_tree(child, indentation_level + 1);
                }
            }
        }
    }

    pub fn internal_node_insert(&mut self, parent_index: usize, child_index: usize) {
        /*
       +  Add a new child/key pair to parent that corresponds to child
       +  */

        let parent = self.pager.get_page_or_create(parent_index);
        let child = self.pager.get_page_or_create(child_index);
        let child_max_key = self.pager.get_node_biggest_key(child, &self.key_type);

        let parent_const = parent.cast_const();
        // cell that contains the key -> position of the child in the parent cells
        let cell_index = self.internal_node_find_child(parent_const, &child_max_key);

        let original_num_keys = BtreePager::get_internal_node_num_keys(parent_const);

        /*
          An internal node with a right child of INVALID_PAGE_NUM is empty
          */
        if original_num_keys >= INTERNAL_NODE_MAX_KEYS {
            self.internal_node_split_and_insert(parent_index, child_index);
            return;
        }

        let right_child_page_index = BtreePager::get_internal_node_right_child(parent_const);
        /*
        An internal node with a right child of INVALID_PAGE_NUM is empty
        */
        if right_child_page_index == INVALID_PAGE_NUM {
            BtreePager::set_internal_node_right_child(parent, child_index);
            return;
        }

        let right_child = self.pager.get_page_or_create(right_child_page_index);

        /*
        If we are already at the max number of cells for a node, we cannot increment
        before splitting. Incrementing without inserting a new key/child pair
        and immediately calling internal_node_split_and_insert has the effect
        of creating a new key at (max_cells + 1) with an uninitialized value
        */
        BtreePager::set_internal_node_num_keys(parent, original_num_keys + 1);

        let biggest_key = self.pager.get_node_biggest_key(right_child, &self.key_type);
        if child_max_key > biggest_key {
            /* Replace right child */
            BtreePager::set_internal_node_child(parent, original_num_keys, right_child_page_index);
            BtreePager::set_internal_node_cell_key(parent, original_num_keys, self.key_size, &biggest_key);
            BtreePager::set_internal_node_right_child(parent, child_index);
        } else {
            /* Make room for the new cell */
            unsafe {
                copy(BtreePager::get_internal_node_cell(parent, cell_index),
                     BtreePager::get_internal_node_cell(parent, cell_index + 1),
                     INTERNAL_NODE_CELL_SIZE * (original_num_keys - cell_index))
            }
            BtreePager::set_internal_node_child(parent, cell_index, child_index);
            BtreePager::set_internal_node_cell_key(parent, cell_index, self.key_size, &child_max_key);
        }
    }
}

pub struct SequentialTable {
    pub root_page_index: usize,
    pub row_size: usize,
    pub max_rows: usize,
    pub pager: BtreePager,
}

impl SequentialTable {
    pub(crate) fn new(path: &str, table_metadata: &TableStructureMetadata) -> SequentialTable {
        todo!()
    }

    pub fn read_all(&mut self) -> Vec<Row> {
        let row_size = self.row_size;
        let mut cursor = Cursor::at(self, 0, 0);
        let mut result = Vec::new();
        while !cursor.is_end() {
            result.push(Row::deserialize_row(cursor.cursor_value(), row_size));
            cursor.cursor_advance();
        }
        result
    }
}

impl Table for SequentialTable {
    fn begin(&mut self) -> Cursor {
        todo!()
    }

    fn insert(&mut self, page_index: usize, cell_index: usize, row: &Row) {
        todo!()
    }

    fn find_by_condition(&mut self, condition_expr: &ConditionExpr) -> Cursor {
        todo!()
    }

    fn end(&mut self) -> Cursor {
        todo!()
    }

    fn is_index_table(&self) -> bool {
        todo!()
    }

    fn get_row_size(&self) -> usize {
        todo!()
    }

    fn get_num_cells(&self, page_index: usize) -> usize {
        todo!()
    }

    fn get_next_page(&self, page_index: usize) -> usize {
        todo!()
    }

    fn get_row_value(&mut self, page_index: usize, cell_index: usize) -> *mut u8 {
        todo!()
    }

    fn flush_to_disk(&self) {
        todo!()
    }

    fn print_tree(&self, page_index: usize, cell_index: usize) {
        todo!()
    }
}


type Page = [u8; PAGE_SIZE];


#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Row {
    pub data: Box<[u8]>
}

impl Row {
    fn new_indexed_row(data: Box<[u8]>) -> Row {
        Row {
            data
        }
    }


    pub(crate) fn serialize_row(&self, destination: *mut u8) {
        unsafe {
            ptr::copy_nonoverlapping(
                self.data.as_ptr(),
                destination,
                self.data.len(),
            );
        }
    }

    fn deserialize_row(source: *const u8, row_size: usize) -> Row {
        let mut data = Vec::<u8>::with_capacity(row_size);
        unsafe {
            ptr::copy_nonoverlapping(
                source,
                data.as_mut_ptr(),
                row_size,
            );
            data.set_len(row_size);
        }

        Row {
            data: data.into_boxed_slice()
        }
    }

    pub(crate) fn deserialize_row_from_bytes(bytes: &[u8]) -> Row {
        Self::deserialize_row(bytes.as_ptr(), bytes.len())
    }

    pub fn read_key(&self, key_type: &DataType, key_offset: usize, key_size: usize) -> Value {
        Value::from_bytes(key_type, &self.data[key_offset..key_offset + key_size])
    }
}

pub struct Statement {
    type_: StatementType,
    row: Option<Row>,
}

impl Statement {
    fn new(type_: StatementType, row: Option<Row>) -> Statement {
        Statement {
            type_,
            row,
        }
    }
}

pub fn new_input_buffer() -> &'static str {
    let mut input = String::new();
    print!("sql>");
    loop {
        std::io::stdout().flush().expect("flush failed!");
        std::io::stdin().read_line(&mut input).unwrap();
        if input.trim().ends_with(";") {
            break;
        }
        print!(">")
    }
    input.leak().trim()
}

pub struct TableStructureMetadata {
    pub table_name: String,
    pub row_size: usize,
    pub fields_metadata: HashMap<String, FieldMetadata>,
}

impl TableStructureMetadata {
    fn new(table_name: &str, fields_metadata: HashMap<String, FieldMetadata>) -> TableStructureMetadata {
        let row_size = fields_metadata.values().map(|m| m.size).reduce(|a, b| a + b).unwrap();
        TableStructureMetadata {
            table_name: table_name.to_string(),
            row_size,
            fields_metadata,
        }
    }

    pub fn get_field_metadata(&self, field_name: &str) -> Result<&FieldMetadata, String> {
        match self.fields_metadata.get(field_name) {
            None => {
                Err(format!("Field {} does not found in the table {}!", field_name, self.table_name))
            }
            Some(fm) => {Ok(fm)}
        }
    }
}

pub struct FieldMetadata {
    field_definition: FieldDefinition,
    pub offset: usize,
    pub size: usize,
}

impl FieldMetadata {
    pub fn new(field_definition: FieldDefinition, offset: usize, size: usize) -> FieldMetadata {
        FieldMetadata {
            field_definition,
            offset,
            size,
        }
    }
}