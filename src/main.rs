#![feature(raw_ref_op)]

extern crate core;

use std::io::Write;
use std::process::exit;
use std::ptr;
use regex::Regex;
use crate::ExecutionResult::{ExecutionSuccess, ExecutionTableFull};

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
const USERNAME_SIZE: usize = std::mem::size_of::<[u8; 50]>();
const EMAIL_SIZE: usize = std::mem::size_of::<[u8; 50]>();
const ID_OFFSET: usize = 0;
const USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
const EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table {
    num_rows: usize,
    pages: Vec<Page>,
}

impl Table {
    fn new() -> Table {
        Table {
            num_rows: 0,
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
        }
    }

    fn row_slot(&mut self, row_num: usize) -> *mut u8 {
        let page_num = row_num / ROWS_PER_PAGE;
        if self.pages.len() < page_num + 1 {
            self.pages.push(Page::new())
        }
        let row_offset = row_num % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        let option = self.pages.get_mut(page_num);
        option.map(|x| {
            unsafe {
                x.content.as_mut_ptr().offset(byte_offset as isize)
            }
        }).unwrap()
    }

    fn insert_row(&mut self, row: &Row) {
        row.serialize_row(self.row_slot(self.num_rows));
        self.num_rows += 1
    }

    fn read_row(&mut self, row_number: usize) -> *const u8 {
        self.row_slot(row_number)
    }

    fn is_full(&self) -> bool {
        self.num_rows >= TABLE_MAX_ROWS
    }
}

struct Page {
    content: [u8; PAGE_SIZE],
}

impl Page {
    fn new() -> Page {
        Page {
            content: [0; PAGE_SIZE]
        }
    }
}

struct InputBuffer {}

enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
}

enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
}

enum ExecutionResult {
    ExecutionSuccess,
    ExecutionTableFull,
}

enum StatementType {
    StatementInsert,
    StatementSelect,
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
        exit(0);
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
        return ExecutionTableFull;
    }

    let row_to_insert = &statement.row;

    let row = row_to_insert.as_ref().unwrap();
    table.insert_row(row);

    ExecutionSuccess
}

fn execute_select(statement: &Statement, table: &mut Table) -> ExecutionResult {
    for i in 0..table.num_rows {
        let mut row = Row::deserialize_row(table.read_row(i));
        println!("[id:{}, username:{}, email:{}]", row.id, String::from_utf8_lossy(row.username.as_slice()), String::from_utf8_lossy(row.email.as_slice()));
    }
    ExecutionSuccess
}


fn main() -> Result<(), &'static str> {
    let mut table = Table::new();
    loop {
        let input = new_input_buffer();
        if input.starts_with('.') {
            match do_meta_command(&input) {
                MetaCommandResult::MetaCommandSuccess => {}
                MetaCommandResult::MetaCommandUnrecognizedCommand => {
                    println!("Unrecognized command '%s'{}", input);
                    continue;
                }
            }
        }

        let statement = prepare_statement(&input)?;

        match execute_statement(&statement, &mut table) {
            ExecutionSuccess => {
                println!("Executed.")
            }
            ExecutionTableFull => {
                println!("Table is full.")
            }
        };
    }
}