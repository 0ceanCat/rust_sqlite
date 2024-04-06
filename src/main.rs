mod core;
mod config;
mod cursor;
mod enums;
mod common;
mod sql_engine;

use std::process::exit;
use core::*;
use enums::*;
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

    Ok(())
}
