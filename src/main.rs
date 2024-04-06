mod core;
mod config;
mod cursor;
mod enums;
mod common;
mod sql_engine;

use std::io::Write;
use std::process::exit;
use core::*;
use enums::*;
use crate::sql_engine::sql_parser::SqlParser;
use crate::sql_engine::sql_structs::SelectStmt;

fn main() -> Result<(), &'static str> {
    /*let file_name = "./db";
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
    }*/
    let mut input = String::new();
    print!("sql>");
    std::io::stdout().flush().expect("flush failed!");
    std::io::stdin().read_line(&mut input).unwrap();
    let result = SqlParser::parse_sql(input)?;
    println!("{:?}", result.print_stmt());
    Ok(())
}
