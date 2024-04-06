use std::array;
use std::ptr::addr_of;
use crate::sql_engine::sql_structs::{SqlStmt, SelectStmt, WhereStmt, ConditionExpr, Condition, Operator, LogicalOperator, Value};
use crate::sql_engine::keywords::*;

#[derive(Clone)]
pub struct SqlParser {
    position: usize,
    input: Vec<char>,
}

impl SqlParser {
    fn parse_sql(input_stream: String) -> Result<Box<dyn SqlStmt>, &'static str> {
        let vec = input_stream.chars().collect();
        let mut parser = SqlParser {
            position: 0,
            input: vec,
        };
        let result = parser.parse();
        result
    }

    fn parse(&mut self) -> Result<Box<dyn SqlStmt>, &'static str> {
        self.skip_white_spaces();

        if self.starts_with(SELECT) {
            let mut select_stmt_parser = SelectStmtParser { sql_parser: self };
            let select_stmt = select_stmt_parser.parse()?;
            Ok(Box::new(select_stmt))
        } else {
            Err("")
        }
    }

    fn skip_white_spaces(&mut self) {
        while !self.is_end() && matches!(self.current_char(), '\t' | '\n' | ' ') {
            self.advance();
        }
    }

    fn is_current_char_comma(&self) -> bool {
        self.current_char() == ','
    }

    fn is_end(&self) -> bool {
        self.position == self.input.len()
    }

    fn read_token(&mut self) -> String {
        let mut token = String::new();
        while !self.is_end() && matches!(self.current_char(), ' ' | ',') {
            token.push(self.current_char());
            self.advance();
        }
        token
    }

    fn current_char(&self) -> char {
        self.input[self.position]
    }

    fn advance(&mut self) {
        self.position += 1
    }

    fn starts_with(&self, s: &str) -> bool {
        let input = &self.input[self.position..];
        for (i, c) in s.char_indices() {
            if input[i] != c {
                return false;
            }
        }
        true
    }
}

struct SelectStmtParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> SelectStmtParser<'a> {
    fn parse(&mut self) -> Result<SelectStmt, &'static str> {
        self.sql_parser.position += SELECT.len();
        self.sql_parser.skip_white_spaces();

        if self.sql_parser.starts_with(FROM) {
            return Err("Syntax error, no selected columns found.");
        }

        let selected_fields = self.parse_selected_fields()?;

        self.sql_parser.position += FROM.len();

        self.sql_parser.skip_white_spaces();

        let table = self.parse_table_name()?;

        self.sql_parser.skip_white_spaces();

        Ok(SelectStmt::new(selected_fields, table, None))
    }

    fn parse_selected_fields(&mut self) -> Result<Vec<String>, &'static str> {
        let mut fields = Vec::<String>::new();
        let mut more_than_one = false;

        while !&self.sql_parser.starts_with(FROM) {
            if more_than_one {
                if !self.sql_parser.is_current_char_comma() {
                    return Err("Syntax error, there must be a ',' between two selected fields.");
                }
                self.sql_parser.advance(); // skip ','
            }

            self.sql_parser.skip_white_spaces();
            fields.push(self.sql_parser.read_token());
            self.sql_parser.skip_white_spaces();
            more_than_one = true;
        }

        Ok(fields)
    }

    fn parse_table_name(&mut self) -> Result<String, &'static str> {
        let string = self.sql_parser.read_token();
        if string.is_empty() {
            return Err("Syntax error, table is not specified.");
        }
        Ok(string)
    }
}

struct WhereStmtParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> WhereStmtParser<'a> {
    fn parse(&mut self) -> Result<WhereStmt, &'static str> {
        self.sql_parser.position += WHERE.len();
        self.sql_parser.skip_white_spaces();

        let mut condition_exprs = Vec::<ConditionExpr>::new();
        let mut logical_op = LogicalOperator::AND;

        while !&self.sql_parser.starts_with(ORDER_BY) {
            condition_exprs.push(self.parse_condition_expr(logical_op)?);
        }

        Ok(WhereStmt::new(condition_exprs))
    }

    fn parse_condition_expr(&mut self, logical_op: LogicalOperator) -> Result<ConditionExpr, &'static str> {
        Ok(ConditionExpr::new(logical_op, self.parse_condition()?))
    }

    fn parse_condition(&mut self) -> Result<Condition, &'static str> {
        self.sql_parser.skip_white_spaces();
        let field = self.sql_parser.read_token();
        self.sql_parser.skip_white_spaces();
        let op = Operator::try_from(self.sql_parser.read_token())?;
        self.sql_parser.skip_white_spaces();
        let v = match self.sql_parser.current_char() {
            '[' => {
                let mut array = Vec::<Value>::new();
                self.sql_parser.advance();
                while self.sql_parser.current_char() != ']' {
                    self.sql_parser.skip_white_spaces();
                    array.push(Value::String(self.sql_parser.read_token()));
                    self.sql_parser.skip_white_spaces();
                }
                self.sql_parser.advance();
                Value::Array(array)
            }
            '\'' => {
                Value::String(self.sql_parser.read_token())
            }
            '\"' => {
                Value::String(self.sql_parser.read_token())
            }
            _ => {
                Value::Integer(self.sql_parser.read_token().parse().unwrap())
            }
        };

        Ok(Condition::new(field, op, v))
    }
}

struct ValueParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> ValueParser<'a> {
    fn parse(&mut self) -> Result<Value, &'static str> {
        match self.sql_parser.current_char() {
            '[' => { self.parse_array() }
            '\"' => { self.parse_string() }
            _ => {self.parse_number()}
        }
    }

    fn parse_array(&mut self) -> Result<Value, &'static str> {
        let mut array = Vec::<Value>::new();
        self.sql_parser.advance(); // skip '['

        let mut value_type: Option<&Value> = None;

        while !self.sql_parser.is_end() && self.sql_parser.current_char() != ']' {
            self.sql_parser.skip_white_spaces();
            let value = self.parse()?;

            match value {
                Value::Array(_) => return Err("An array must contain only primitive values. But array detected."),
                Value::SelectStmt(_) => return Err("An array must contain only primitive values. But select statement detected."),
                _ => {}
            }

            if !array.is_empty() && Value::are_same_variant(&array[0], &value) {
                return Err("All element of an array must be the same type.");
            }

            array.push(value);
            self.sql_parser.skip_white_spaces();
        }

        if self.sql_parser.current_char() != ']' {
            return Err("Detected an array value, but it is not closed.");
        }

        self.sql_parser.advance();
        Ok(Value::Array(array))
    }

    fn parse_string(&mut self) -> Result<Value, &'static str> {
        self.sql_parser.advance(); // skip '"'
        let mut token = String::new();
        while !self.sql_parser.is_end() {
            if self.sql_parser.current_char() != '"' {
                token.push(self.sql_parser.current_char());
            } else {
                self.sql_parser.advance(); // skip '"'
                return Ok(Value::String(token));
            }
            self.sql_parser.advance();
        }

        Err("String value parse failed.")
    }

    fn parse_number(&mut self) -> Result<Value, &'static str> {
        let negative = self.sql_parser.current_char() == '-';
        let sign: i32 = if negative { -1 } else { 1 };
        if negative || self.sql_parser.current_char() == '+' {
            self.sql_parser.advance();
        }
        let mut result: Value = self.parse_int()?;
        if let Value::Integer(first_part) = result {
            if !self.sql_parser.is_end() && self.sql_parser.current_char() == '.' {
                self.sql_parser.advance();
                let mut base = 1.0;
                if let Value::Integer(nb) = self.parse_int()? {
                    let second_part = nb as f32;
                    while second_part / base > 0.0 {
                        base *= 10.0
                    }
                    result = Value::Float(sign as f32 * (first_part as f32 + second_part / base))
                }
            } else {
                result = Value::Integer(sign * first_part)
            }
        }
        Ok(result)
    }
    fn parse_int(&mut self) -> Result<Value, &'static str> {
        let c = self.sql_parser.current_char();
        match c {
            '0'..='9' => {
                let mut result = 0;
                while !self.sql_parser.is_end() && ('0'..='9').contains(&c) {
                    result = result * 10 + ValueParser::char_to_integer(c);
                    self.sql_parser.advance();
                }
                return Ok(Value::Integer(result));
            }
            _ => Err("Integer parse failed")
        }
    }

    fn char_to_integer(c: char) -> i32 {
        c as i32 - 0x30
    }
}