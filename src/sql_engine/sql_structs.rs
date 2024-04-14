use std::cmp::{Ordering, PartialEq, PartialOrd};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use crate::sql_engine::sql_structs::LogicalOperator::{AND, OR};
use crate::sql_engine::sql_structs::Operator::{EQUALS, GT, GTE, IN, LT, LTE};
use crate::storage_engine::core::{Pager, Row, Table};

pub(crate) trait Printable {
    fn print_stmt(&self) {}
}

pub(crate) enum SqlStmt {
    SELECT(SelectStmt),
    INSERT(InsertStmt),
    CREATE(CreateStmt)
}

impl Printable for SelectStmt {
    fn print_stmt(&self) {
        println!("selected fields: {:?}", self.selected_fields);
        println!("table name: {:?}", self.table);
        println!("where stmt: {:?}", self.where_expr);
        println!("order by stmt: {:?}", self.order_by_expr);
    }
}

impl Printable for WhereExpr {
    fn print_stmt(&self) {
        println!("{:?}", self.condition_exprs)
    }
}

impl Printable for InsertStmt {
    fn print_stmt(&self) {
        println!("table name: {}", self.table);
        println!("fields: {:?}", self.fields);
        println!("values: {:?}", self.values);
    }
}

impl Printable for CreateStmt {
    fn print_stmt(&self) {
        println!("table name: {}", self.table);
        println!("defined fields: {:?}", self.definitions);
    }
}

#[derive(PartialEq, Debug, PartialOrd)]
pub(crate) struct SelectStmt {
    pub(crate) selected_fields: Vec<String>,
    pub(crate) table: String,
    pub(crate) where_expr: Option<WhereExpr>,
    pub(crate) order_by_expr: Option<OrderByCluster>,
}

impl SelectStmt {
    pub(crate) fn new(selected_fields: Vec<String>, table: String, where_stmt: Option<WhereExpr>, order_by_stmt: Option<OrderByCluster>) -> SelectStmt {
        SelectStmt {
            selected_fields,
            table,
            where_expr: where_stmt,
            order_by_expr: order_by_stmt,
        }
    }

    fn table_file(&self) -> Result<File, String> {
        match OpenOptions::new().read(true).open(&self.table) {
            Ok(file) => {
                Ok(file)
            }
            Err(_) => {
                Err(format!("Table {} does not exist.", self.table))
            }
        }
    }

    pub(crate) fn execute(&self) -> Result<Vec<Row>, String> {
        let file = self.table_file()?;
        let pager = Pager::open_from(file);
        let mut table = Table::new(pager);
        let mut result = Vec::<Row>::new();
        if self.where_expr.is_some() {
            let where_expr = self.where_expr.as_ref().unwrap();
            let set = where_expr.execute(&mut table)?;
            result = set.into_iter().collect();
        } else {
            result = table.read_all();
        }
        if self.order_by_expr.is_some() {
            todo!()
        }
        Ok(result)
    }
}


#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct InsertStmt {
    table: String,
    fields: Vec<String>,
    values: Vec<Value>,
}

impl InsertStmt {
    pub fn new(table: String, fields: Vec<String>, values: Vec<Value>) -> InsertStmt {
        InsertStmt {
            table,
            fields,
            values,
        }
    }

    pub fn execute(&self) -> Result<usize, String> {
        todo!()
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct WhereExpr {
    condition_exprs: Vec<(LogicalOperator, ConditionCluster)>,
}

impl WhereExpr {
    pub(crate) fn new(condition_exprs: Vec<(LogicalOperator, ConditionCluster)>) -> WhereExpr {
        WhereExpr {
            condition_exprs
        }
    }

    fn execute(&self, table: &mut Table) -> Result<HashSet<Row>, String> {
        let mut outer_set = HashSet::<Row>::new();
        for (op, cluster) in &self.condition_exprs {
            let set = Self::find_rows(cluster, table)?;
            match op {
                OR => outer_set.extend(set),
                AND => outer_set.retain(|r| set.contains(r))
            }
        }
        Ok(outer_set)
    }

    fn find_rows(condition_cluster: &ConditionCluster, table: &mut Table) -> Result<HashSet<Row>, String> {
        let mut set: HashSet<Row> = HashSet::<Row>::new();
        for condition in &condition_cluster.conditions {
            let result = table.table_find_by_value(&condition.field, &condition.operator, &condition.value)?;
            match condition.logical_operator {
                OR => {
                    result.into_iter().for_each(|r| {
                        set.insert(r);
                    });
                }
                AND => {
                    set.retain(|e| result.contains(e));
                }
            };
        }
        Ok(set)
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct CreateStmt {
    table: String,
    definitions: Vec<FieldDefinition>
}

impl CreateStmt {
    pub(crate) fn new(table: String, definitions: Vec<FieldDefinition>) -> CreateStmt {
        CreateStmt {
            table,
            definitions
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct FieldDefinition {
    field: String,
    data_type: DataType,
    is_primary_key: bool
}

impl FieldDefinition {
    pub fn new(field: String, data_type: DataType, is_primary_key: bool) -> FieldDefinition {
        FieldDefinition {
            field,
            data_type,
            is_primary_key
        }
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary_key
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct IndexCreationStmt {
    field: String
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct OrderByCluster {
    pub(crate) order_by_exprs: Vec<OrderByExpr>,
}

impl OrderByCluster {
    pub fn new(order_by_exprs: Vec<OrderByExpr>) -> OrderByCluster {
        OrderByCluster {
            order_by_exprs
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct OrderByExpr {
    field: String,
    order: Order,
}

impl OrderByExpr {
    pub fn new(field: String, order: Order) -> OrderByExpr {
        OrderByExpr {
            field,
            order,
        }
    }
}

#[derive(PartialEq, Debug, PartialOrd)]
pub(crate) struct ConditionCluster {
    conditions: Vec<ConditionExpr>,
}

impl ConditionCluster {
    pub(crate) fn new(conditions: Vec<ConditionExpr>) -> ConditionCluster {
        ConditionCluster {
            conditions,
        }
    }
}

#[derive(PartialEq, Debug, PartialOrd)]
pub(crate) struct ConditionExpr {
    pub logical_operator: LogicalOperator,
    pub field: String,
    pub operator: Operator,
    pub value: Value,
}

impl ConditionExpr {
    pub(crate) fn new(logical_operator: LogicalOperator, field: String, operator: Operator, value: Value) -> ConditionExpr {
        ConditionExpr {
            logical_operator,
            field,
            operator,
            value,
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) enum Order {
    ASC,
    DESC,
}

impl TryFrom<&str> for Order {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "asc" => {
                Ok(Order::ASC)
            }
            "desc" => { Ok(Order::DESC) }
            _ => { Err("Unknown Order.") }
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
    IN(bool),
}

#[derive(PartialEq, PartialOrd, Clone, Copy, Debug)]
pub(crate) enum LogicalOperator {
    OR,
    AND,
}

impl TryFrom<&str> for LogicalOperator {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "or" => {
                Ok(OR)
            }
            "and" => {
                Ok(AND)
            }
            _ => { Err("Unknown logical operator.") }
        }
    }
}

impl Operator {
    pub(crate) fn operate(&self, a: &Value, b: &Value) -> bool {
        match self {
            EQUALS(negative) => { (a == b) ^ negative }
            GT => { a > b }
            GTE => { a >= b }
            LT => { a < b }
            LTE => { a <= b }
            IN(negative) => {
                if let Value::Array(vec) = b {
                    vec.contains(&a) ^ negative
                } else {
                    false
                }
            }
        }
    }
}

impl TryFrom<String> for Operator {
    type Error = String;

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
                Err(format!("operator `{}` does not exist;", value))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum Value {
    Integer(i32),
    Float(f32),
    Boolean(bool),
    String(String),
    Array(Vec<Value>),
    SelectStmt(SelectStmt),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::Integer(i) => { *i == other.unwrap_into_int().unwrap() }
            Value::Float(f) => { *f == other.unwrap_into_float().unwrap() }
            Value::Boolean(b) => { *b == other.unwrap_into_bool().unwrap() }
            Value::String(s) => {
                s == other.unwrap_as_string().unwrap()
            }
            Value::Array(a) => { a == other.unwrap_as_array().unwrap() }
            Value::SelectStmt(s) => { other.is_select_stmt() }
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Value::Integer(i) => { i.partial_cmp(&other.unwrap_into_int().unwrap()) }
            Value::Float(f) => { f.partial_cmp(&other.unwrap_into_float().unwrap()) }
            Value::Boolean(b) => { b.partial_cmp(&other.unwrap_into_bool().unwrap()) }
            Value::String(s) => { s.partial_cmp(&other.unwrap_as_string().unwrap()) }
            Value::Array(a) => { None }
            Value::SelectStmt(s) => { None }
        }
    }
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

    pub fn unwrap_into_int(&self) -> Result<i32, &str> {
        match self {
            Value::Integer(v) => Ok(*v),
            _ => Err("Current Value is not an Integer.")
        }
    }

    pub fn unwrap_into_float(&self) -> Result<f32, &str> {
        match self {
            Value::Float(v) => Ok(*v),
            _ => Err("Current Value is not a Float.")
        }
    }

    pub fn unwrap_as_string(&self) -> Result<&String, &str> {
        match self {
            Value::String(v) => Ok(v),
            _ => Err("Current Value is not a String.")
        }
    }

    pub fn unwrap_as_array(&self) -> Result<&Vec<Value>, &str> {
        match self {
            Value::Array(v) => Ok(v),
            _ => Err("Current Value is not an Array.")
        }
    }

    pub fn unwrap_into_bool(&self) -> Result<bool, &str> {
        match self {
            Value::Boolean(v) => Ok(*v),
            _ => Err("Current Value is not a Boolean.")
        }
    }

    pub fn is_select_stmt(&self) -> bool {
        match self {
            Value::SelectStmt(_) => true,
            _ => false
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub enum DataType {
    TEXT(usize),
    INTEGER,
    FLOAT,
    BOOLEAN,
}