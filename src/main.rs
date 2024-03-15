#![feature(raw_ref_op)]

mod enums;

extern crate core;

use std::cell::Cell;
use std::fs::{File, OpenOptions};
use std::io::{Seek, Write};
use std::os::windows::fs::FileExt;
use std::process::exit;
use std::ptr;
use regex::Regex;
use enums::*;

const fn size<T>(_: *const T) -> usize {
    std::mem::size_of::<T>()
}


macro_rules! size_of {
    ($struct_:ident, $filed: ident) => {
        {
            let null: *const $struct_ = std::ptr::null();
            size(unsafe { &raw const (*null).$filed })
        }
    };
}

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

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 32;

const ID_SIZE: usize = std::mem::size_of::<usize>();
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; COLUMN_USERNAME_SIZE]>();
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; COLUMN_EMAIL_SIZE]>();
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Pager {
    pages: Vec<Page>,
    fd: File,
    size: usize,
    total_pages: usize,
}

impl Pager {
    fn open(db_file: &str) -> Pager {
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

    fn row_slot(&mut self, row_num: usize) -> *mut u8 {
        let page_num = row_num / ROWS_PER_PAGE;
        let page: &mut Page = self.get_page(page_num);
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        unsafe {
            page.content.as_mut_ptr().offset(byte_offset as isize)
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
        self.fd.seek_read(&mut bytes, offset);
        Page::from(bytes)
    }

    fn page_in_disk(&self, page_num: usize) -> bool {
        self.total_pages > page_num
    }

    fn flush_page_to_disk(&mut self, page_num: usize, size_to_write: usize) {
        let page = self.get_page(page_num);
        self.fd.write(&page.content[..size_to_write]).unwrap();
    }
}

struct Table {
    num_rows: usize,
    pager: Pager,
}

impl Table {
    fn new(pager: Pager) -> Table {
        Table {
            num_rows: pager.total_pages,
            pager,
        }
    }

    fn insert_row(&mut self, row: &Row) {
        row.serialize_row(self.pager.row_slot(self.num_rows));
        self.num_rows += 1
    }

    fn read_row(&mut self, row_number: usize) -> *const u8 {
        self.pager.row_slot(row_number)
    }

    fn is_full(&self) -> bool {
        self.num_rows >= TABLE_MAX_ROWS
    }

    fn flush_to_disk(mut self) {
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
struct Row {
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


    fn serialize_row(&self, destination: *mut u8) {
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

struct Statement {
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

fn new_input_buffer() -> &'static str {
    let mut input = String::new();
    print!("db>");
    std::io::stdout().flush().expect("flush failed!");
    std::io::stdin().read_line(&mut input).unwrap();
    input.leak().trim()
}

fn do_meta_command(input: &str) -> MetaCommandResult {
    if input == ".exit" {
        return MetaCommandResult::MetaCommandExit;
    }
    return MetaCommandResult::MetaCommandSuccess;
}

fn prepare_statement(input: &str) -> Result<Statement, &'static str> {
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

fn execute_statement(statement: &Statement, table: &mut Table) -> ExecutionResult {
    match statement.type_ {
        StatementType::StatementInsert => {
            execute_insert(statement, table)
        }
        StatementType::StatementSelect => {
            execute_select(statement, table)
        }
    }
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecutionResult {
    if table.is_full() {
        return ExecutionResult::ExecutionTableFull;
    }

    let row_to_insert = &statement.row;

    let row = row_to_insert.as_ref().unwrap();
    table.insert_row(row);

    ExecutionResult::ExecutionSuccess
}

fn execute_select(statement: &Statement, table: &mut Table) -> ExecutionResult {
    for i in 0..table.num_rows {
        let mut row = Row::deserialize_row(table.read_row(i));
        println!("[id:{}, username:{}, email:{}]", row.id, String::from_utf8_lossy(row.username.as_slice()), String::from_utf8_lossy(row.email.as_slice()));
    }
    ExecutionResult::ExecutionSuccess
}


fn main() -> Result<(), &'static str> {
    let file_name = "./db";
    let pager = Pager::open(file_name);
    let mut table = Table::new(pager);
    loop {
        let input = new_input_buffer();
        if input.starts_with('.') {
            match do_meta_command(&input) {
                MetaCommandResult::MetaCommandSuccess => {}
                MetaCommandResult::MetaCommandUnrecognizedCommand => {
                    println!("Unrecognized command '%s'{}", input);
                    continue;
                }
                MetaCommandResult::MetaCommandExit => {
                    table.flush_to_disk();
                    exit(0);
                }
            }
        }

        match prepare_statement(&input) {
            Ok(statement) => {
                match execute_statement(&statement, &mut table) {
                    ExecutionResult::ExecutionSuccess => {
                        println!("Executed.")
                    }
                    ExecutionResult::ExecutionTableFull => {
                        println!("Table is full.")
                    }
                };
            }
            Err(error) => {
                println!("{}", error)
            }
        }
    }
}