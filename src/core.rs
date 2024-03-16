#![feature(raw_ref_op)]

extern crate core;

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::windows::fs::FileExt;
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
                let mut total_pages = size.div_ceil(PAGE_SIZE);
                Pager {
                    pages: Vec::with_capacity(TABLE_MAX_PAGES),
                    fd: file,
                    size,
                    total_pages,
                }
            }
            Err(_) => {
                panic!("Can not open db file!")
            }
        }
    }

    fn get_page(&mut self, page_num: usize) -> &mut Page {
        let page = self.pages.get(page_num);
        if page.is_none() {
            if self.page_in_disk(page_num) {
                let loaded_page = self.read_page_from_disk((page_num * PAGE_SIZE) as u64);
                self.pages.push(loaded_page);
            } else {
                self.pages.push(Page::new_page());
            }
        }
        self.pages.get_mut(page_num).unwrap()
    }

    fn read_page_from_disk(&self, offset: u64) -> Page {
        let mut bytes = [0; PAGE_SIZE];
        self.fd.seek_read(&mut bytes, offset).unwrap();
        Page::from(bytes)
    }

    fn page_in_disk(&self, page_num: usize) -> bool {
        self.total_pages > page_num
    }

    fn flush_page_to_disk(&mut self, page_num: usize, size_to_write: usize) {
        let page = &self.pages[page_num];
        self.fd.seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64)).unwrap();
        self.fd.write(&page.content[..size_to_write]).unwrap();
    }
}

pub struct Table {
    pub num_rows: usize,
    pub pager: Pager,
}

impl Table {
    pub(crate) fn new(pager: Pager) -> Table {
        Table {
            num_rows: pager.size / ROW_SIZE,
            pager,
        }
    }

    pub(crate) fn row_slot(&mut self, row_num: usize) -> *mut u8 {
        let page_num = row_num / ROWS_PER_PAGE;
        let page: &mut Page = self.pager.get_page(page_num);
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        unsafe {
            page.content.as_mut_ptr().offset(byte_offset as isize)
        }
    }

    pub(crate) fn is_full(&self) -> bool {
        self.num_rows >= TABLE_MAX_ROWS
    }

    pub(crate) fn flush_to_disk(mut self) {
        let full_pages = self.num_rows / PAGE_SIZE;
        for x in 0 .. full_pages {
            self.pager.flush_page_to_disk(x, PAGE_SIZE)
        }

        let additional_rows = self.num_rows % ROWS_PER_PAGE;
        if additional_rows > 0 {
            let page_num = full_pages;
            self.pager.flush_page_to_disk(page_num, ROW_SIZE * additional_rows);
        }
    }
}

struct Page {
    content: [u8; PAGE_SIZE]
}

impl Page {
    fn new_page() -> Page {
        Page{
            content: [0; PAGE_SIZE]
        }
    }

    fn from(b: [u8; PAGE_SIZE]) -> Page {
        Page{
            content: b
        }
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
            execute_insert(statement, Cursor::table_end(table))
        }
        StatementType::StatementSelect => {
            execute_select(Cursor::table_start(table))
        }
    }
}

pub fn execute_insert(statement: &Statement, mut cursor: Cursor) -> ExecutionResult {
    if cursor.is_full() {
        return ExecutionResult::ExecutionTableFull;
    }

    let row_to_insert = &statement.row;

    let row = row_to_insert.as_ref().unwrap();
    cursor.insert_row(row);

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