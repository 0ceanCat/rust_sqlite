use crate::sql_engine::sql_structs::Operator::{EQUALS, GT, GTE, IN, LT, LTE};

pub(crate) trait SqlStmt{
}

impl SqlStmt for SelectStmt{}
impl SqlStmt for InsertStmt{}

#[derive(PartialEq, PartialOrd)]
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

#[derive(PartialEq, PartialOrd)]
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

#[derive(PartialEq, PartialOrd)]
pub(crate) struct Condition {
    field: String,
    operator: Operator,
    value: Value,
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

#[derive(PartialEq, PartialOrd)]
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

#[derive(PartialEq, PartialOrd)]
pub(crate) enum Operator {
    EQUALS,
    GT,
    GTE,
    LT,
    LTE,
    IN
}

#[derive(PartialEq, PartialOrd, Clone, Copy)]
pub(crate) enum LogicalOperator{
    OR,
    AND
}

impl Operator{
    fn operate(&self, a: Value, b: Value) -> bool {
        match self {
            EQUALS => { a == b }
            GT => { a > b }
            GTE => { a >= b }
            LT => { a < b }
            LTE => { a <= b }
            IN => {
                if let Value::Array(vec) = b {
                    vec.contains(&a)
                } else {
                    false
                }
            }
        }
    }
}

impl TryFrom<String> for Operator {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Operator, Self::Error> {
        match value.as_str() {
            "equals" => {
                Ok(EQUALS)
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
                Ok(IN)
            }
            _ => {
                Err("operator does not exist;")
            }
        }
    }
}

#[derive(PartialEq, PartialOrd)]
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