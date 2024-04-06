use crate::sql_engine::sql_structs::LogicalOperator::{AND, OR};
use crate::sql_engine::sql_structs::Operator::{EQUALS, GT, GTE, IN, LT, LTE};

pub(crate) trait SqlStmt{
    fn print_stmt(&self) {
    }
}

impl SqlStmt for SelectStmt{
    fn print_stmt(&self) {
        println!("selected fields: {:?}", self.selected_fields);
        println!("table name: {:?}", self.table);
        println!("where stmt: {:?}", self.where_stmt);
    }
}
impl SqlStmt for WhereStmt{
    fn print_stmt(&self) {
        println!("{:?}", self.condition_exprs)
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct SelectStmt {
    pub(crate) selected_fields: Vec<String>,
    pub(crate) table: String,
    pub(crate) where_stmt: Option<WhereStmt>,
}

impl SelectStmt{
    pub(crate) fn new(selected_fields: Vec<String>, table: String, where_stmt: Option<WhereStmt>) -> SelectStmt {
        SelectStmt{
            selected_fields,
            table,
            where_stmt
        }
    }
}

struct InsertStmt {
    table: String,
    fields: Vec<String>,
    values: Vec<String>
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct WhereStmt {
    condition_exprs: Vec<ConditionExpr>
}

impl WhereStmt {
    pub(crate) fn new(condition_exprs: Vec<ConditionExpr>) -> WhereStmt {
        WhereStmt {
            condition_exprs
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct Condition {
    pub field: String,
    pub operator: Operator,
    pub value: Value,
}

impl Condition {
    pub(crate) fn new(field: String, operator: Operator, value: Value) -> Condition {
        Condition {
            field,
            operator,
            value
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct ConditionExpr{
    logical_op: LogicalOperator,
    condition: Condition
}

impl ConditionExpr {
    pub(crate) fn new(logical_op: LogicalOperator, condition: Condition) -> ConditionExpr {
        ConditionExpr{
            logical_op,
            condition
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) enum Operator {
    EQUALS(bool),
    GT,
    GTE,
    LT,
    LTE,
    IN(bool)
}

#[derive(PartialEq, PartialOrd, Clone, Copy, Debug)]
pub(crate) enum LogicalOperator{
    OR,
    AND
}

impl TryFrom<String> for LogicalOperator {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            "or"  => {
                Ok(OR)
            }
            "and" => {
                Ok(AND)
            }
            _ => { Err("Unknown logical operator") }
        }
    }
}

impl Operator{
    fn operate(&self, a: Value, b: Value) -> bool {
        match self {
            EQUALS(negative) => { (a == b) ^ negative }
            GT => { a > b }
            GTE => { a >= b }
            LT => { a < b }
            LTE => { a <= b }
            IN(negative) => {
                let r = if let Value::Array(vec) = b {
                    vec.contains(&a)
                } else {
                    false
                };

                r ^ negative
            }
        }
    }
}

impl TryFrom<String> for Operator {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Operator, Self::Error> {
        match value.as_str() {
            "=" => {
                Ok(EQUALS(false))
            }
            "!=" => {
                Ok(EQUALS(true))
            }
            ">" => {
                Ok(GT)
            }
            ">=" => {
                Ok(GTE)
            }
            "<" => {
                Ok(LT)
            }
            "<=" => {
                Ok(LTE)
            }
            "in" => {
                Ok(IN(false))
            }
            "not in" => {
                Ok(IN(true))
            }
            _ => {
                Err("operator does not exist;")
            }
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) enum Value {
    Integer(i32),
    Float(f32),
    Boolean(bool),
    String(String),
    Array(Vec<Value>),
    SelectStmt(SelectStmt)
}

impl Value {
    pub(crate) fn are_same_variant(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Integer(_), Value::Integer(_)) => {
                true
            }
            (Value::Float(_), Value::Float(_)) => {
                true
            }
            (Value::String(_), Value::String(_)) => {
                true
            }
            (Value::Array(_), Value::Array(_)) => {
                true
            }
            (Value::Boolean(_), Value::Boolean(_)) => {
                true
            }
            (Value::SelectStmt(_), Value::SelectStmt(_)) => {
                true
            }
            _ => {
                false
            }
        }
    }

    fn unwrap_as_int(&self) -> Result<&i32, &str>{
        match self {
            Value::Integer(v) => Ok(v),
            _ => Err("Current Value is not an Integer.")
        }
    }

    fn unwrap_as_float(&self) -> Result<&f32, &str>{
        match self {
            Value::Float(v) => Ok(v),
            _ => Err("Current Value is not a Float.")
        }
    }

    fn unwrap_as_string(&self) -> Result<&String, &str>{
        match self {
            Value::String(v) => Ok(v),
            _ => Err("Current Value is not a String.")
        }
    }

    fn unwrap_as_array(&self) -> Result<&Vec<Value>, &str>{
        match self {
            Value::Array(v) => Ok(v),
            _ => Err("Current Value is not an Array.")
        }
    }

    fn unwrap_as_bool(&self) -> Result<&bool, &str>{
        match self {
            Value::Boolean(v) => Ok(v),
            _ => Err("Current Value is not a Boolean.")
        }
    }

    fn is_select_stmt(&self) -> bool {
        match self {
            Value::SelectStmt(_) => true,
            _ => false
        }
    }
}