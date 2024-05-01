mod storage_engine;
mod sql_engine;
mod utils;


use std::cmp::Ordering;
use std::io::{Read, Write};
use crate::sql_engine::sql_parser::SqlParser;
use crate::sql_engine::sql_structs::SqlStmt;
use crate::storage_engine::common::*;

fn main() -> Result<(), String> {
    let mut table_manager = TableManager::new();
    loop {
        let input = new_input_buffer();

        if input == "flush;" {
            table_manager.flash_to_disk();
            continue;
        } else if input.starts_with("btree;") {
            table_manager.print_btree(input.split_once(" ").unwrap().1);
            continue;
        } else if input == "exit;" {
            break;
        }

        let sql = match SqlParser::parse_sql(input) {
            Ok(sql) => {sql}
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        match sql {
            SqlStmt::SELECT(mut select) => {
                let result = select.execute(&mut table_manager)?;
                result.print();
            }
            SqlStmt::INSERT(insert) => {
                println!("{:?}", match insert.execute(&mut table_manager) {
                    Ok(_) => {
                        String::from("Data inserted.")
                    }
                    Err(e) => {e}
                });
            }
            SqlStmt::CREATE(create) => {
                println!("{:?}", match create.execute(&mut table_manager) {
                    Ok(_) => {
                        String::from("Table created.")
                    }
                    Err(e) => {e}
                });
            }
        }
    }
    Ok(())
}
