mod storage_engine;
mod sql_engine;


use std::io::{Read, Write};
use std::process::exit;
use crate::sql_engine::sql_parser::SqlParser;
use crate::sql_engine::sql_structs::SqlStmt;
use crate::storage_engine::core::*;
use crate::storage_engine::enums::*;

fn main() -> Result<(), String> {
    let file_name = "./user";
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

        let sql = SqlParser::parse_sql(input)?;
        match sql {
            SqlStmt::SELECT(select) => {
                let result = select.execute()?;
                for row in result {
                    println!("[id:{}, username:{}, email:{}]", row.id, String::from_utf8_lossy(row.username.as_slice()), String::from_utf8_lossy(row.email.as_slice()));
                }
            }
            SqlStmt::INSERT(insert) => {
                println!("{:?}", insert.execute()?);
            }
            SqlStmt::CREATE(create) => {
                println!("{:?}", create);
            }
        }
    }
    Ok(())
}
