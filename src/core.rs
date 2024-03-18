#![feature(raw_ref_op)]

extern crate core;

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::process::{exit};
use std::ptr;
use regex::Regex;
use crate::config::*;
use crate::cursor::Cursor;
use crate::enums::*;


macro_rules! to_u8_array {
    ($s:ident, $size: expr) => {
        {
           let mut array: [u8; $size] = [0; $size];
            let bytes = $s.as_bytes();
            array[..bytes.len()].copy_from_slice(bytes);
            array
        }
    };
}

pub struct Pager {
    pages: Vec<Page>,
    updated: [bool; TABLE_MAX_PAGES],
    fd: File,
    size: usize,
    total_pages: usize,
}

impl Pager {
    pub(crate) fn open(db_file: &str) -> Pager {
        let r = OpenOptions::new().create(true).read(true).write(true).open(db_file);
        match r {
            Ok(file) => {
                let size = file.metadata().unwrap().len() as usize;
                if size % PAGE_SIZE != 0 {
                    println!("Db file is not a whole number of pages. Corrupt file.");
                    exit(1);
                }
                let mut total_pages = size / PAGE_SIZE;
                Pager {
                    pages: Vec::with_capacity(TABLE_MAX_PAGES),
                    fd: file,
                    updated: [false; TABLE_MAX_PAGES],
                    size,
                    total_pages,
                }
            }
            Err(_) => {
                panic!("Can not open db file!")
            }
        }
    }

    pub(crate) fn get_unused_page_num(&self) -> usize {
        self.total_pages
    }

    fn get_node_type_by_index(&mut self, page_index: usize) -> NodeType {
        let page = self.get_page(page_index);
        Self::get_node_type(page)
    }

    pub(crate) fn get_page(&mut self, page_index: usize) -> *mut u8 {
        let page = self.pages.get(page_index);
        if page.is_none() {
            if self.page_in_disk(page_index) {
                let loaded_page = self.read_page_from_disk((page_index * PAGE_SIZE) as u64);
                self.pages.push(loaded_page);
            } else {
                self.pages.push(Page::new_page());
                self.total_pages += 1;
            }
        }
        self.pages.get_mut(page_index).unwrap().content.as_mut_ptr()
    }

    pub(crate) fn get_leaf_node_cells_num(page: *mut u8) -> usize {
        unsafe {
            let page_ptr = page.offset(LEAF_NODE_NUM_CELLS_OFFSET as isize);
            let mut cells_num: usize = 0;
            ptr::copy_nonoverlapping(
                page_ptr,
                &mut cells_num as *mut usize as *mut u8,
                LEAF_NODE_NUM_CELLS_SIZE,
            );
            cells_num
        }
    }

    pub(crate) fn set_leaf_node_cells_num(page: *mut u8, num: usize) {
        unsafe {
            let page_ptr = page.offset(LEAF_NODE_NUM_CELLS_OFFSET as isize);
            let mut num = num;
            ptr::copy_nonoverlapping(
                page_ptr,
                &mut num as *const usize as *mut u8,
                LEAF_NODE_NUM_CELLS_SIZE,
            );
        }
    }

    pub(crate) fn increment_leaf_node_cells_num(page: *mut u8) {
        unsafe {
            let page_ptr = page.offset(LEAF_NODE_NUM_CELLS_OFFSET as isize);
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

    pub(crate) fn get_leaf_node_cell_key(page: *mut u8, cell_index: usize) -> usize {
        unsafe {
            let key: usize = 0;
            ptr::copy_nonoverlapping(
                page.offset((LEAF_NODE_HEADER_SIZE + cell_index * LEAF_NODE_CELL_SIZE) as isize),
                &key as *const usize as *mut u8,
                LEAF_NODE_KEY_SIZE,
            );
            key
        }
    }

    pub(crate) fn set_leaf_node_cell_key(page: *mut u8, cell_index: usize, key: usize) {
        unsafe {
            ptr::copy_nonoverlapping(
                &key as *const usize as *mut u8,
                page.offset((LEAF_NODE_HEADER_SIZE + cell_index * LEAF_NODE_CELL_SIZE) as isize),
                LEAF_NODE_KEY_SIZE,
            );
        }
    }

    pub(crate) fn leaf_node_cell(page: *mut u8, cell_index: usize) -> *mut u8 {
        unsafe {
            let page_ptr = page
                .offset((LEAF_NODE_HEADER_SIZE + cell_index * LEAF_NODE_CELL_SIZE) as isize);
            page_ptr
        }
    }

    pub(crate) fn leaf_node_value(page: *mut u8, cell_index: usize) -> *mut u8 {
        let ptr = Self::leaf_node_cell(page, cell_index);
        unsafe {
            ptr.add(LEAF_NODE_VALUE_OFFSET)
        }
    }

    pub(crate) fn get_node_type(ptr: *mut u8) -> NodeType {
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
                (node_type as u8) as *const u8 as *mut u8,
                page.add(NODE_TYPE_OFFSET),
                NODE_TYPE_SIZE,
            );
        }
    }

    pub(crate) fn is_root_node(page: *mut u8) -> bool {
        unsafe {
            let mut root = false;
            ptr::copy_nonoverlapping(
                page.add(IS_ROOT_OFFSET),
                &root as *const bool as *mut u8,
                IS_ROOT_SIZE,
            );

            root
        }
    }

    fn read_page_from_disk(&self, offset: u64) -> Page {
        let mut bytes = [0; PAGE_SIZE];
        self.fd.seek_read(&mut bytes, offset).unwrap();
        Page::from(bytes)
    }

    fn page_in_disk(&self, page_num: usize) -> bool {
        self.total_pages > page_num
    }

    fn flush_page_to_disk(&mut self, page_num: usize) -> bool {
        let page_op = self.pages.get(page_num);
        match page_op {
            None => {
                false
            }
            Some(page) => {
                self.fd.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)).unwrap();
                self.fd.write(&page.content).unwrap();
                true
            }
        }
    }

    pub(crate) fn mark_page_as_updated(&mut self, page_index: usize) {
        self.updated[page_index] = true;
    }
}

pub struct Table {
    pub root_page_index: usize,
    pub pager: Pager,
}

impl Table {
    pub(crate) fn new(pager: Pager) -> Table {
        Table {
            root_page_index: 0,
            pager,
        }
    }

    pub(crate) fn table_find(&mut self, key: usize) -> Cursor {
        let node_type = self.pager.get_node_type_by_index(self.root_page_index);
        match node_type {
            NodeType::Internal => {
                panic!("not implemented")
            }
            NodeType::Leaf => {
                self.find(key)
            }
        }
    }

    fn find(&mut self, key: usize) -> Cursor {
        let root = self.pager.get_page(self.root_page_index);
        let cells_num = Pager::get_leaf_node_cells_num(root);

        let mut min_index = 0;
        let mut right = cells_num;
        while right != min_index {
            let index = (min_index + right) / 2;
            let key_at_index = Pager::get_leaf_node_cell_key(root, index);
            if key == key_at_index {
                return Cursor::at(self, index);
            }
            if key < key_at_index {
                right = index;
            } else {
                min_index = index + 1;
            }
        }

        Cursor::at(self, min_index)
    }

    pub(crate) fn row_slot(&mut self, row_num: usize) -> *mut u8 {
        let page_num = row_num / ROWS_PER_PAGE;
        let page = self.pager.get_page(page_num);
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        unsafe {
            page.offset(byte_offset as isize)
        }
    }

    pub(crate) fn flush_to_disk(mut self) {
        for x in 0..TABLE_MAX_PAGES {
            if !self.pager.flush_page_to_disk(x) {
                break;
            }
        }
    }
}

struct Page {
    content: [u8; PAGE_SIZE],
}

impl Page {
    fn new_page() -> Page {
        Page {
            content: [0; PAGE_SIZE]
        }
    }

    fn from(b: [u8; PAGE_SIZE]) -> Page {
        Page {
            content: b
        }
    }

    fn as_ptr(&self) -> *const u8 {
        self.content.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.content.as_mut_ptr()
    }
}


#[derive(Debug)]
pub struct Row {
    id: usize,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    fn new(id: usize, username: [u8; COLUMN_USERNAME_SIZE], email: [u8; COLUMN_EMAIL_SIZE]) -> Row {
        Row {
            id,
            username,
            email,
        }
    }


    pub(crate) fn serialize_row(&self, destination: *mut u8) {
        unsafe {
            ptr::copy_nonoverlapping(
                (&self.id as *const usize) as *const u8,
                destination.add(ID_OFFSET),
                1,
            );

            ptr::copy_nonoverlapping(
                self.username.as_ptr(),
                destination.add(USERNAME_OFFSET),
                USERNAME_SIZE,
            );

            ptr::copy_nonoverlapping(
                self.email.as_ptr(),
                destination.add(EMAIL_OFFSET),
                EMAIL_SIZE,
            );
        }
    }

    fn deserialize_row(source: *const u8) -> Row {
        let mut destination = Row {
            id: 0,
            username: [0u8; COLUMN_USERNAME_SIZE],
            email: [0u8; COLUMN_EMAIL_SIZE],
        };

        unsafe {
            ptr::copy_nonoverlapping(
                source.add(ID_OFFSET),
                &mut destination.id as *mut usize as *mut u8,
                ID_SIZE,
            );

            ptr::copy_nonoverlapping(
                source.add(USERNAME_OFFSET),
                destination.username.as_mut_ptr(),
                USERNAME_SIZE,
            );

            ptr::copy_nonoverlapping(
                source.add(EMAIL_OFFSET),
                destination.email.as_mut_ptr(),
                EMAIL_SIZE,
            );
        }

        destination
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
    print!("db>");
    std::io::stdout().flush().expect("flush failed!");
    std::io::stdin().read_line(&mut input).unwrap();
    input.leak().trim()
}

pub fn do_meta_command(input: &str) -> MetaCommandResult {
    if input == ".exit" {
        return MetaCommandResult::MetaCommandExit;
    }
    return MetaCommandResult::MetaCommandSuccess;
}

pub fn prepare_statement(input: &str) -> Result<Statement, &'static str> {
    let re = Regex::new(r"insert (\d+) (\S+) (\S+)").unwrap();
    if input.starts_with("insert") && re.is_match(input) {
        if let Some(captures) = re.captures(input) {
            let id: usize = captures.get(1).unwrap().as_str().parse().unwrap();
            let username = captures.get(2).unwrap().as_str();
            let email = captures.get(3).unwrap().as_str();

            if username.len() > COLUMN_USERNAME_SIZE || email.len() > COLUMN_EMAIL_SIZE {
                return Err("String is too long.");
            }

            let row = Row::new(id, to_u8_array!(username, COLUMN_USERNAME_SIZE), to_u8_array!(email, COLUMN_EMAIL_SIZE));
            return Ok(Statement::new(StatementType::StatementInsert, Some(row)));
        }
        return Err("Insert syntax error");
    } else if input == "select" {
        return Ok(Statement::new(StatementType::StatementSelect, None));
    }

    Err("GG")
}

pub fn execute_statement(statement: &Statement, table: &mut Table) -> ExecutionResult {
    match statement.type_ {
        StatementType::StatementInsert => {
            execute_insert(statement, table)
        }
        StatementType::StatementSelect => {
            execute_select(Cursor::table_start(table))
        }
    }
}

pub fn execute_insert(statement: &Statement, table: &mut Table) -> ExecutionResult {
    let row_to_insert = &statement.row;

    let row = row_to_insert.as_ref().unwrap();
    let mut cursor = table.table_find(row.id);
    cursor.insert_row(row.id, row);
    ExecutionResult::ExecutionSuccess
}

pub fn execute_select(mut cursor: Cursor) -> ExecutionResult {
    while !cursor.is_end() {
        let mut row = Row::deserialize_row(cursor.cursor_value());
        println!("[id:{}, username:{}, email:{}]", row.id, String::from_utf8_lossy(row.username.as_slice()), String::from_utf8_lossy(row.email.as_slice()));
        cursor.cursor_advance();
    }
    ExecutionResult::ExecutionSuccess
}