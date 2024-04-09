use crate::sql_engine::sql_structs::{SqlStmt, SelectStmt, WhereStmt, InsertStmt, ConditionExpr, Condition, Operator, LogicalOperator, Value, OrderByStmt, OrderByExpr, Order};
use crate::sql_engine::keywords::*;

static BLANK_SYMBOLS: [char; 4] = [' ', '\t', '\n', '\r'];
static TOKEN_SEPARATORS: [char; 7] = [' ', ',', '(', ')', '\t', '\n', '\r'];
static OPERATORS_SYMBOLS: [char; 4] = ['>', '<', '=', '!'];

#[derive(Clone)]
pub struct SqlParser {
    position: usize,
    input: Vec<char>,
}

impl SqlParser {
    pub fn parse_sql(input_stream: String) -> Result<Box<dyn SqlStmt>, String> {
        let vec = input_stream.chars().collect();
        SqlParser {
            position: 0,
            input: vec,
        }.parse()
    }

    fn parse(&mut self) -> Result<Box<dyn SqlStmt>, String> {
        self.skip_white_spaces();

        if self.starts_with(SELECT) {
            let mut select_stmt_parser = SelectStmtParser { sql_parser: self };
            let select_stmt = select_stmt_parser.parse()?;
            Ok(Box::new(select_stmt))
        } else if self.starts_with(INSERT_INTO) {
            let mut insert_stmt_parser = InsertStmtParser { sql_parser: self };
            let insert_stmt = insert_stmt_parser.parse()?;
            Ok(Box::new(insert_stmt))
        } else {
            Err(String::from("Unknown sql statement."))
        }
    }

    fn skip_white_spaces(&mut self) {
        while !self.is_end() && BLANK_SYMBOLS.contains(&self.current_char()) {
            self.advance();
        }
    }

    fn is_current_char_comma(&self) -> bool {
        self.current_char() == ','
    }

    fn is_end(&self) -> bool {
        self.position >= self.input.len()
    }

    fn read_token(&mut self) -> Result<String, String> {
        self.skip_white_spaces();
        let mut token = String::new();

        while !self.is_end() && !TOKEN_SEPARATORS.contains(&self.current_char()) {
            if OPERATORS_SYMBOLS.contains(&self.current_char()) {
                break;
            }
            token.push(self.current_char());
            self.advance();
        }
        self.skip_white_spaces();
        Ok(token)
    }

    fn parse_table_name(&mut self) -> Result<String, String> {
        let string = self.read_token()?;
        if string.is_empty() {
            return Err(String::from("Syntax error, table is not specified."));
        }
        Ok(string)
    }

    fn current_char(&self) -> char {
        self.input[self.position]
    }

    fn advance(&mut self) {
        self.position += 1
    }

    fn starts_with(&self, s: &str) -> bool {
        let input = &self.input[self.position..];
        if input.len() == 0 {
            return false;
        }
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
    fn parse(&mut self) -> Result<SelectStmt, String> {
        self.sql_parser.position += SELECT.len();
        self.sql_parser.skip_white_spaces();

        if self.sql_parser.starts_with(FROM) {
            return Err(String::from("Syntax error, no selected columns found."));
        }

        let selected_fields = self.parse_selected_fields()?;

        self.sql_parser.position += FROM.len();

        self.sql_parser.skip_white_spaces();

        let table = self.sql_parser.parse_table_name()?;

        self.sql_parser.skip_white_spaces();

        let where_stmt: Option<WhereStmt> = if self.sql_parser.is_end() {
            None
        } else {
            Some(WhereStmtParser { sql_parser: self.sql_parser }.parse()?)
        };

        let order_by_stmt: Option<OrderByStmt> = if self.sql_parser.is_end() {
            None
        } else {
            Some(OrderByStmtParser { sql_parser: self.sql_parser }.parse()?)
        };

        Ok(SelectStmt::new(selected_fields, table, where_stmt, order_by_stmt))
    }

    fn parse_selected_fields(&mut self) -> Result<Vec<String>, String> {
        let mut fields = Vec::<String>::new();
        self.sql_parser.skip_white_spaces();

        while !self.sql_parser.is_end() && !&self.sql_parser.starts_with(FROM) {
            self.sql_parser.skip_white_spaces();
            let field = self.sql_parser.read_token()?;

            if fields.contains(&field) {
                return Err(format!("Column `{field}` has already be selected."));
            }

            check_key_word(&field)?;
            check_valid_field_name(&field)?;
            fields.push(field);

            self.sql_parser.skip_white_spaces();

            if self.sql_parser.is_current_char_comma() {
                self.sql_parser.advance(); // skip ','
                self.sql_parser.skip_white_spaces();
            } else if !&self.sql_parser.starts_with(FROM) {
                return Err(String::from("Syntax error, there must be a ',' between two selected fields."));
            }
        }

        Ok(fields)
    }
}

struct WhereStmtParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> WhereStmtParser<'a> {
    fn parse(&mut self) -> Result<WhereStmt, String> {
        if !self.sql_parser.starts_with(WHERE) {
            return Err(format!("Syntax error, expected a Where statement, but a token `{}` was found.", self.sql_parser.read_token()?));
        }
        self.sql_parser.position += WHERE.len();
        self.sql_parser.skip_white_spaces();

        let mut condition_exprs = Vec::<ConditionExpr>::new();
        let mut logical_op = Some(LogicalOperator::AND);

        while !self.sql_parser.is_end() {
            let condition_expr = self.parse_condition_expr(logical_op.unwrap())?;
            condition_exprs.push(condition_expr);
            logical_op = None;

            if self.sql_parser.is_end() || self.sql_parser.starts_with(ORDER_BY) {
                break;
            }

            logical_op = Some(LogicalOperator::try_from(self.sql_parser.read_token()?.as_str())?);
        }

        if logical_op.is_some() {
            return Err(String::from("Syntax error, Where statement is not complete."));
        }

        if condition_exprs.is_empty() {
            return Err(String::from("Syntax error, empty Where statement detected."));
        }

        Ok(WhereStmt::new(condition_exprs))
    }

    fn parse_condition_expr(&mut self, logical_op: LogicalOperator) -> Result<ConditionExpr, String> {
        self.sql_parser.skip_white_spaces();
        let mut conditions = Vec::<Condition>::new();
        let mut more_than_one = false;
        let mut logical_op = logical_op;
        if self.sql_parser.current_char() == '(' {
            self.sql_parser.advance(); //skip '('
            while !self.sql_parser.is_end() {
                if more_than_one {
                    logical_op = LogicalOperator::try_from(self.sql_parser.read_token()?.as_str())?;
                }
                conditions.push(self.parse_condition(logical_op)?);
                if self.sql_parser.current_char() == ')' {
                    break;
                }
                more_than_one = true;
            }
            if self.sql_parser.current_char() != ')' {
                return Err(format!("Syntax error, Where statement is incorrectly formatted, expected a ')' but found {}", self.sql_parser.current_char()));
            }
            self.sql_parser.advance(); //skip ')'
        } else {
            conditions.push(self.parse_condition(logical_op)?);
        }
        self.sql_parser.skip_white_spaces();
        Ok(ConditionExpr::new(conditions))
    }

    fn parse_condition(&mut self, logical_operator: LogicalOperator) -> Result<Condition, String> {
        self.sql_parser.skip_white_spaces();
        let field = self.sql_parser.read_token()?;
        check_key_word(&field)?;
        check_valid_field_name(&field)?;

        self.sql_parser.skip_white_spaces();
        let op = {
            OperatorParser { sql_parser: self.sql_parser }.parse()?
        };
        self.sql_parser.skip_white_spaces();
        let v = ValueParser { sql_parser: self.sql_parser }.parse()?;
        self.sql_parser.skip_white_spaces();
        Ok(Condition::new(logical_operator, field, op, v))
    }
}

struct InsertStmtParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> InsertStmtParser<'a> {
    fn parse(&mut self) -> Result<InsertStmt, String> {
        self.sql_parser.position += INSERT_INTO.len();
        self.sql_parser.skip_white_spaces();

        let table_name = self.sql_parser.parse_table_name()?;

        self.sql_parser.skip_white_spaces();

        let fields = self.parse_inserted_fields()?;

        let values = self.parse_values()?;

        if fields.first().unwrap() != "*" && values.len() != fields.len() {
            return Err(String::from("Number of inserted rows and row values are not the same."))
        }

        Ok(InsertStmt::new(table_name, fields, values))
    }

    fn parse_inserted_fields(&mut self) -> Result<Vec<String>, String> {
        self.sql_parser.skip_white_spaces();
        let mut fields = Vec::<String>::new();
        if self.sql_parser.current_char() == '(' {
            self.sql_parser.advance(); // skip '('
            while !self.sql_parser.is_end() {
                let field = self.sql_parser.read_token()?;
                check_valid_field_name(&field)?;
                check_key_word(&field)?;
                fields.push(field);
                self.sql_parser.skip_white_spaces();
                if !self.sql_parser.is_end() && self.sql_parser.current_char() == ',' {
                    self.sql_parser.advance();
                } else if !self.sql_parser.is_end() && self.sql_parser.current_char() == ')' {
                    break;
                }
                self.sql_parser.skip_white_spaces();
            }

            if self.sql_parser.is_end() || self.sql_parser.current_char() != ')' {
                return Err(String::from("Syntax error, inserted fields is not closed, expected a ')'"));
            }
            self.sql_parser.advance();
        } else {
            fields.push(String::from("*"))
        }

        Ok(fields)
    }
    fn parse_values(&mut self) -> Result<Vec<Value>, String> {
        self.sql_parser.skip_white_spaces();
        if self.sql_parser.is_end() || !self.sql_parser.starts_with(VALUES) {
            return Err(String::from("Syntax error, `values` is missing."));
        }
        self.sql_parser.position += VALUES.len();
        self.sql_parser.skip_white_spaces();
        if !self.sql_parser.is_end() && self.sql_parser.current_char() == '(' {
            self.sql_parser.advance(); // skip '('
            let mut values = Vec::<Value>::new();
            while !self.sql_parser.is_end() {
                let value = ValueParser { sql_parser: self.sql_parser }.parse()?;
                values.push(value);

                self.sql_parser.skip_white_spaces();
                if !self.sql_parser.is_end() && self.sql_parser.current_char() == ',' {
                    self.sql_parser.advance();
                } else if !self.sql_parser.is_end() && self.sql_parser.current_char() == ')' {
                    break;
                }
            }
            if self.sql_parser.is_end() || self.sql_parser.current_char() != ')' {
                return Err(String::from("Syntax error, `values` is not closed, expected a ')'"));
            }
            self.sql_parser.advance();
            return Ok(values)
        } else {
            return Err(String::from("Syntax error, `values` is uncompleted."));
        }
    }
}

struct OrderByStmtParser<'a> {
    sql_parser: &'a mut SqlParser
}

impl<'a> OrderByStmtParser<'a> {
    pub(crate) fn parse(&mut self) -> Result<OrderByStmt, String> {
        self.sql_parser.skip_white_spaces();

        if !self.sql_parser.starts_with(ORDER_BY) {
            return Err(format!("Syntax error, expect `order by`, but found {}", self.sql_parser.read_token()?))
        }

        self.sql_parser.position += ORDER_BY.len();

        self.sql_parser.skip_white_spaces();

        let mut order_bys = Vec::<OrderByExpr>::new();

        while !self.sql_parser.is_end() {
            let field = self.sql_parser.read_token()?;
            check_valid_field_name(&field)?;
            check_key_word(&field)?;
            self.sql_parser.skip_white_spaces();
            let mut order: Order;
            if self.sql_parser.is_end() || self.sql_parser.current_char() == ','{
                order = Order::ASC;
            } else {
                order = Order::try_from(self.sql_parser.read_token()?.as_str())?;
            }
            self.sql_parser.advance();
            order_bys.push(OrderByExpr::new(field, order));
        }

        Ok(OrderByStmt{order_by_exprs: order_bys})
    }
}

struct ValueParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> ValueParser<'a> {
    fn parse(&mut self) -> Result<Value, String> {
        match self.sql_parser.current_char() {
            '[' => { self.parse_array() }
            '\"' => { self.parse_string() }
            '0'..='9' | '+' | '-' => { self.parse_number() }
            't' | 'f' => { self.parse_boolean() }
            _ => { return Err(format!("Unknown type of value `{}` detected.", self.sql_parser.read_token()?)); }
        }
    }

    fn parse_array(&mut self) -> Result<Value, String> {
        let mut array = Vec::<Value>::new();
        self.sql_parser.advance(); // skip '['

        while !self.sql_parser.is_end() && self.sql_parser.current_char() != ']' {
            self.sql_parser.skip_white_spaces();
            let value = self.parse()?;

            match value {
                Value::Array(_) => return Err(String::from("An array must contain only primitive values. But array detected.")),
                Value::SelectStmt(_) => return Err(String::from("An array must contain only primitive values. But select statement detected.")),
                _ => {}
            }

            if !array.is_empty() && !Value::are_same_variant(&array[0], &value) {
                return Err(String::from("All element of an array must be the same type."));
            }

            array.push(value);
            self.sql_parser.skip_white_spaces();
            if !self.sql_parser.is_end() && self.sql_parser.current_char() == ',' {
                self.sql_parser.advance();
            }
        }

        if self.sql_parser.is_end() || self.sql_parser.current_char() != ']' {
            return Err(String::from("Detected an array value, but it is not closed. ']' is expected."));
        }

        self.sql_parser.advance(); // skip ']'
        Ok(Value::Array(array))
    }

    fn parse_string(&mut self) -> Result<Value, String> {
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

        Err(String::from("String value parse failed."))
    }

    fn parse_number(&mut self) -> Result<Value, String> {
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
                    while second_part / base > 1.0 {
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

    pub(crate) fn parse_boolean(&mut self) -> Result<Value, String> {
        if self.sql_parser.starts_with("true") {
            self.sql_parser.position += "true".len();
            return Ok(Value::Boolean(true));
        } else if self.sql_parser.starts_with("false") {
            self.sql_parser.position += "false".len();
            return Ok(Value::Boolean(false));
        };
        Err(format!("Unknown type of value `{}` detected", self.sql_parser.read_token()?))
    }

    fn parse_int(&mut self) -> Result<Value, String> {
        match self.sql_parser.current_char() {
            '0'..='9' => {
                let mut result = 0;
                while !self.sql_parser.is_end() && ('0'..='9').contains(&self.sql_parser.current_char()) {
                    result = result * 10 + ValueParser::char_to_integer(self.sql_parser.current_char());
                    self.sql_parser.advance();
                }
                return Ok(Value::Integer(result));
            }
            _ => Err(String::from("Integer parse failed"))
        }
    }

    fn char_to_integer(c: char) -> i32 {
        c as i32 - 0x30
    }
}

struct OperatorParser<'a> {
    sql_parser: &'a mut SqlParser,
}

impl<'a> OperatorParser<'a> {
    fn parse(&mut self) -> Result<Operator, String> {
        self.sql_parser.skip_white_spaces();
        let mut operator = String::new();

        if !self.sql_parser.is_end() && self.sql_parser.starts_with(NOT) {
            operator.push_str("not ");
            self.sql_parser.position += NOT.len();
            self.sql_parser.skip_white_spaces();
        }

        while !self.sql_parser.is_end() && OPERATORS_SYMBOLS.contains(&self.sql_parser.current_char()) {
            operator.push(self.sql_parser.current_char());
            self.sql_parser.advance();
        }

        Operator::try_from(operator)
    }
}

fn check_key_word(k: &String) -> Result<(), String> {
    match !is_key_words(k) {
        true => {
            Ok(())
        }
        false => {
            Err(format!("You can not use keyword `{}` as a field.", k))
        }
    }
}

fn check_valid_field_name(k: &String) -> Result<(), String> {
    if k.chars().next().unwrap().is_numeric() {
        return Err(format!("Field name `{}` is invalid", k));
    }
    for c in k.chars() {
        if !c.is_numeric() && (c < 'a' || c > 'z') && c != '_' {
            return Err(format!("Field name `{}` is invalid", k));
        }
    }
    Ok(())
}