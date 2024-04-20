extern crate core;

mod storage_engine;
mod sql_engine;
mod utils;


use std::io::{Read, Write};
use crate::sql_engine::sql_parser::SqlParser;
use crate::sql_engine::sql_structs::SqlStmt;
use crate::storage_engine::core::*;

fn main() -> Result<(), String> {
    /*let mut table_manager = TableManager::new();
    loop {
        let input = new_input_buffer();

        if input == "flush" {
            table_manager.flash_to_disk()
        } else if input.starts_with("btree") {
            table_manager.print_btree(input.split_once(" ").unwrap().1)
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
    }*/
    let a = 0b0000_0110;
    let b = (a >> 1) | 0;
    println!("{}", b);
    Ok(())
}
