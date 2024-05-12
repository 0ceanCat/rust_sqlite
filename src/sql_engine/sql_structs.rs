use std::{fs, ptr};
use std::cmp::{max, Ordering, PartialEq, PartialOrd};
use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::slice::Iter;

use crate::build_path;
use crate::sql_engine::sql_structs::Operator::{EQUALS, GT, GTE, IN, LT, LTE};
use crate::storage_engine::common::{
    RowBytes, RowToInsert, RowValues, SelectResult, TableManager, TableStructureMetadata,
};
use crate::storage_engine::config::*;
use crate::storage_engine::tables::Table;
use crate::utils::utils::{copy_nonoverlapping, ToU8, u8_array_to_string};

pub(crate) enum SqlStmt {
    SELECT(SelectStmt),
    INSERT(InsertStmt),
    CREATE(CreateStmt),
}

#[derive(PartialEq, Debug, PartialOrd)]
pub(crate) struct SelectStmt {
    pub(crate) selected_fields: Vec<String>,
    pub(crate) table: String,
    pub(crate) where_expr: Option<WhereExpr>,
    pub(crate) order_by_expr: Option<OrderByCluster>,
}

impl SelectStmt {
    pub(crate) fn new(
        selected_fields: Vec<String>,
        table: String,
        where_stmt: Option<WhereExpr>,
        order_by_stmt: Option<OrderByCluster>,
    ) -> SelectStmt {
        SelectStmt {
            selected_fields,
            table,
            where_expr: where_stmt,
            order_by_expr: order_by_stmt,
        }
    }

    pub(crate) fn execute<'a>(
        &'a mut self,
        table_manager: &'a mut TableManager,
    ) -> Result<SelectResult, String> {
        let table = table_manager.get_tables(&self.table);
        if table.is_err() {
            return Err(format!("Table `{}` does not exist.", self.table));
        }

        let result = self.execute_where(table_manager);

        let table_meta = table_manager.get_table_metadata(&self.table)?;

        let selected_fields: Vec<&str> =
            if self.selected_fields.len() == 1 && self.selected_fields.first().unwrap() == "*" {
                table_meta.fields
                          .iter()
                          .map(|v| v.data_def.field_name.as_str())
                          .collect()
            } else {
                self.selected_fields.iter().map(|x| x.as_str()).collect()
            };

        let order_by_exprs = self.order_by_expr.take()
                                 .unwrap_or_else(|| OrderByCluster::new(vec![]))
                                 .order_by_exprs;

        let projected_results = self.order_by(order_by_exprs, &result, table_meta, &selected_fields)?;

        let human_readable_results = projected_results.into_iter().map(|(v, _)| v).collect();

        Ok(SelectResult::new(selected_fields, human_readable_results))
    }

    fn order_by(
        &self,
        order_by_exprs: Vec<OrderByExpr>,
        result: &Vec<RowBytes>,
        table_meta: &TableStructureMetadata,
        selected_fields: &Vec<&str>,
    ) -> Result<Vec<(RowValues, Vec<Rc<Value>>)>, String> {
        let order_by_fields: Vec<&str> = order_by_exprs.iter().map(|o| o.field.as_str()).collect();

        let max_row_size = table_meta.fields.iter().map(|x| x.size).max().unwrap();

        let mut row_buf = vec![0; max_row_size];
        let row_buf_ptr = row_buf.as_mut_ptr();

        let mut projected_results: Vec<(RowValues, Vec<Rc<Value>>)> =
            Vec::with_capacity(result.len());

        for row in result {
            let mut selected_values: Vec<Rc<Value>> = Vec::with_capacity(selected_fields.len());
            let mut order_values: Vec<Rc<Value>> = Vec::with_capacity(order_by_fields.len());
            let mut value_index: Vec<(usize, usize)> = Vec::new();

            for (index, field_name) in selected_fields.iter().enumerate() {
                let field_meta = table_meta.get_field_metadata(field_name)?;
                copy_nonoverlapping(
                    row[field_meta.offset..field_meta.offset + field_meta.size].as_ptr(),
                    row_buf_ptr,
                    field_meta.size,
                );
                let value = Rc::new(Value::from_ptr(
                    &field_meta.data_def.data_type,
                    row_buf[..field_meta.size].as_ptr(),
                ));

                let mut i = 0;
                if order_by_fields.iter().any(|v| {
                    i += 1;
                    v == field_name
                }) {
                    value_index.push((index, i - 1));
                }
                selected_values.push(Rc::clone(&value));
            }

            value_index.sort_by(|a, b| a.1.cmp(&b.1));

            for (index, field_name) in order_by_fields.iter().enumerate() {
                let (value_index_in_selected, index_in_order_fields) = value_index.first().unwrap();
                if *index_in_order_fields == index {
                    order_values.push(Rc::clone(&selected_values[*value_index_in_selected]));
                    value_index.remove(0);
                } else {
                    let field_meta = table_meta.get_field_metadata(field_name)?;
                    copy_nonoverlapping(
                        row[field_meta.offset..field_meta.offset + field_meta.size].as_ptr(),
                        row_buf_ptr,
                        field_meta.size,
                    );
                    let value = Rc::new(Value::from_ptr(
                        &field_meta.data_def.data_type,
                        row_buf[..field_meta.size].as_ptr(),
                    ));
                    order_values.push(Rc::clone(&value));
                }
            }

            projected_results.push((RowValues::new(selected_values), order_values));
        }

        projected_results.sort_by(|(_, order_values1), (_, order_values2)| {
            let mut index: usize = 0;
            for expr in &order_by_exprs {
                let mut ordering = order_values1[index]
                    .partial_cmp(&order_values2[index])
                    .unwrap();
                if expr.order.is_desc() {
                    ordering = ordering.reverse();
                }

                if !ordering.is_eq() {
                    return ordering;
                }
                index += 1;
            }
            Ordering::Equal
        });

        Ok(projected_results)
    }

    fn execute_where(&mut self, table_manager: &mut TableManager) -> Vec<RowBytes> {
        match &mut self.where_expr {
            None => table_manager.get_tables(&self.table)
                                 .unwrap()
                                 .first()
                                 .unwrap()
                                 .get_all(),
            Some(ref mut w) => {
                w.execute(&self.table, table_manager)
            }
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct InsertStmt {
    table: String,
    pub fields: Vec<String>,
    pub values: Vec<Value>,
}

impl InsertStmt {
    pub fn new(table: String, fields: Vec<String>, values: Vec<Value>) -> InsertStmt {
        InsertStmt {
            table,
            fields,
            values,
        }
    }

    pub fn execute(&mut self, table_manager: &mut TableManager) -> Result<(), String> {
        let meta = table_manager.get_table_metadata(&self.table)?;
        if self.fields.len() == 1 && self.fields.first().unwrap() == "*" {
            self.fields = meta.fields
                              .iter()
                              .map(|f| f.data_def.field_name.to_string())
                              .collect();
        } else {
            match self.fields
                      .iter()
                      .filter(|f| meta.get_field_metadata(f).is_err())
                      .next()
            {
                None => {}
                Some(field) => {
                    return Err(format!(
                        "Field `{}` not found in table `{}`.",
                        field, self.table
                    ));
                }
            };
        }

        let row = RowToInsert::new(
            &self.fields,
            &self.values,
            table_manager.get_table_metadata(&self.table)?,
        );
        let tables = table_manager.get_tables(&self.table)?;

        for table in tables.iter_mut() {
            table.insert(&row)?;
        }
        Ok(())
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct WhereExpr {
    condition_cluster: Vec<ConditionCluster>,
}

impl WhereExpr {
    pub(crate) fn new(condition_exprs: Vec<ConditionCluster>) -> WhereExpr {
        WhereExpr {
            condition_cluster: condition_exprs,
        }
    }

    fn execute(&mut self, table_name: &str, table_manager: &mut TableManager) -> Vec<RowBytes> {
        let mut index_scan = false;
        self.condition_cluster.sort_by(|c1, c2|{
            let indexed1 = c1.iter()
                             .any(|c| c.find_index(table_name, table_manager).is_some()).to_u8();

            let indexed2 = c2.iter()
                             .any(|c| c.find_index(table_name, table_manager).is_some()).to_u8();
            index_scan = (indexed1 | indexed2) == 1;
            indexed2.cmp(&indexed1)
        } );

        if index_scan {
            let mut global_result = vec![];
            let mut last_op = LogicalOperator::OR;
            for cluster in self.condition_cluster.iter() {
                let table = match cluster.iter().filter_map(|c| c.find_index(table_name, table_manager)).next() {
                    None => {
                        table_manager.get_tables(table_name).unwrap().first_mut().unwrap()
                    }
                    Some(mut index) => {
                        index
                    }
                };

                let local_result = table.find_by_condition_cluster(cluster);
                if last_op == LogicalOperator::OR {
                    global_result.extend(local_result.into_iter());
                    last_op = cluster.logical_operator;
                } else {
                    let set: HashSet<RowBytes> = HashSet::from_iter(global_result.into_iter());
                    global_result = vec![];
                    for r in local_result {
                        if set.contains(&r) {
                            global_result.push(r);
                        }
                    }
                }
            };

            global_result
        } else {
            // full scan
            table_manager.get_tables(table_name).unwrap().first_mut().unwrap().find_by_condition_clusters(&self.condition_cluster)
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct CreateStmt {
    table: String,
    definitions: Vec<FieldDefinition>,
}

impl CreateStmt {
    pub(crate) fn new(table: String, definitions: Vec<FieldDefinition>) -> CreateStmt {
        CreateStmt { table, definitions }
    }

    pub fn execute(&self, table_manager: &mut TableManager) -> Result<(), String> {
        let table_name = self.table.as_str();
        let frm_path = build_path!(DATA_FOLDER, table_name, table_name.to_owned() + ".frm");

        if Path::new(&frm_path).exists() {
            return Err(format!("Table {} already exists.", table_name));
        } else {
            let dir = build_path!(DATA_FOLDER, table_name);
            match fs::create_dir_all(dir) {
                Ok(_) => {}
                Err(_) => {
                    return Err(String::from("Can not create dir."));
                }
            };
        }

        let row_size = self
            .definitions
            .iter()
            .map(|d| d.data_type.get_size())
            .sum();

        unsafe {
            match File::create(frm_path) {
                Ok(file) => self.write_structure_metadata(file)?,
                Err(_) => {
                    return Err(String::from("Can not create table."));
                }
            }

            let primary_key = self.definitions.iter().filter(|d| d.is_primary_key).next();
            match primary_key {
                None => {
                    let sequential_path =
                        build_path!(DATA_FOLDER, table_name, table_name.to_owned() + ".seq");
                    let sequential_file = File::create(&sequential_path).unwrap();
                    self.write_seq_metadata(sequential_file, row_size)?;
                    table_manager.register_new_table(&self.table, &sequential_path)
                }
                Some(f) => {
                    let primary_path =
                        build_path!(DATA_FOLDER, table_name, table_name.to_owned() + ".idx");
                    let primary_file = File::create(&primary_path).unwrap();
                    self.write_index_metadata(primary_file, f)?;
                    table_manager.register_new_table(&self.table, &primary_path)
                }
            }
        }
    }

    unsafe fn write_structure_metadata(&self, mut file: File) -> Result<(), String> {
        let mut total_size = 0;
        total_size += FIELD_NUMBER_SIZE;
        total_size += self.definitions.len() * FIELD_NAME_SIZE;
        total_size += self.definitions.len() * FIELD_TYPE_PRIMARY_SIZE;

        let text_fields = self
            .definitions
            .iter()
            .filter(|d| d.data_type.is_text())
            .count();
        total_size += text_fields * TEXT_CHARS_NUM_SIZE;

        let mut vec = vec![0; total_size];
        let buf = vec.as_mut_ptr();
        let mut buf_pointer = 0; // pointer that points to the position where we should start reading

        ptr::copy_nonoverlapping(
            &self.definitions.len() as *const usize as *const u8,
            buf,
            FIELD_NUMBER_SIZE,
        );
        buf_pointer += FIELD_NUMBER_SIZE;

        self.definitions.iter().for_each(|field_definition| {
            ptr::copy_nonoverlapping(
                field_definition.field_name.as_ptr(),
                buf.add(buf_pointer),
                field_definition.field_name.len(),
            );
            buf_pointer += FIELD_NAME_SIZE;
            let data_type_primary: u8 = (field_definition.data_type.to_bit_code() << 1)
                | field_definition.is_primary_key.to_u8();
            ptr::copy_nonoverlapping(
                &data_type_primary as *const u8,
                buf.add(buf_pointer),
                FIELD_TYPE_PRIMARY_SIZE,
            );
            buf_pointer += FIELD_TYPE_PRIMARY_SIZE;
            match field_definition.data_type {
                DataType::TEXT(size) => {
                    ptr::copy_nonoverlapping(
                        &size as *const usize as *const u8,
                        buf.add(buf_pointer),
                        TEXT_CHARS_NUM_SIZE,
                    );
                    buf_pointer += TEXT_CHARS_NUM_SIZE;
                }
                _ => {}
            }
        });

        if file.write(vec.as_slice()).is_err() {
            return Err(format!(
                "Can not write structure metadata for table {}!",
                self.table
            ));
        };
        Ok(())
    }

    unsafe fn write_index_metadata(
        &self,
        mut file: File,
        primary_field: &FieldDefinition,
    ) -> Result<(), String> {
        let mut vec: [u8; BTREE_METADATA_SIZE] = [0; BTREE_METADATA_SIZE];
        let buf = vec.as_mut_ptr();
        let mut buf_pointer = 0; // pointer that points to the position where we should start reading

        let data_type_primary: u8 =
            (primary_field.data_type.to_bit_code() << 1) | primary_field.is_primary_key.to_u8();
        ptr::copy_nonoverlapping(
            &data_type_primary as *const u8,
            buf.add(buf_pointer),
            INDEXED_FIELD_TYPE_PRIMARY,
        );
        buf_pointer += INDEXED_FIELD_TYPE_PRIMARY;

        ptr::copy_nonoverlapping(
            &primary_field.data_type.get_size() as *const usize as *const u8,
            buf.add(buf_pointer),
            INDEXED_FIELD_SIZE,
        );
        buf_pointer += INDEXED_FIELD_SIZE;

        ptr::copy_nonoverlapping(
            primary_field.field_name.as_ptr(),
            buf.add(buf_pointer),
            primary_field.field_name.len(),
        );

        if file.write(&vec).is_err() {
            return Err(format!("Can not write metadata for table {}!", self.table));
        };
        Ok(())
    }

    unsafe fn write_seq_metadata(&self, mut file: File, row_size: usize) -> Result<(), String> {
        let mut vec = vec![0; SEQUENTIAL_NODE_HEADER_SIZE];
        let buf = vec.as_mut_ptr();
        let cells_num = (PAGE_SIZE - SEQUENTIAL_NODE_HEADER_SIZE) / row_size;
        ptr::copy_nonoverlapping(
            &cells_num as *const usize as *mut u8,
            buf,
            LEAF_NODE_NUM_CELLS_SIZE,
        );

        if file.write(vec.as_slice()).is_err() {
            return Err(format!("Can not write metadata for table {}!", self.table));
        };
        Ok(())
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct FieldDefinition {
    pub field_name: String,
    pub data_type: DataType,
    pub is_primary_key: bool,
}

impl FieldDefinition {
    pub fn new(field: String, data_type: DataType, is_primary_key: bool) -> FieldDefinition {
        FieldDefinition {
            field_name: field,
            data_type,
            is_primary_key,
        }
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary_key
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct OrderByCluster {
    pub(crate) order_by_exprs: Vec<OrderByExpr>,
}

impl OrderByCluster {
    pub fn new(order_by_exprs: Vec<OrderByExpr>) -> OrderByCluster {
        OrderByCluster { order_by_exprs }
    }
}

#[derive(PartialEq, PartialOrd, Debug)]
pub(crate) struct OrderByExpr {
    field: String,
    order: Order,
}

impl OrderByExpr {
    pub fn new(field: String, order: Order) -> OrderByExpr {
        OrderByExpr { field, order }
    }
}

#[derive(PartialEq, Debug, PartialOrd, Clone)]
pub enum Condition {
    Cluster(ConditionCluster),
    Expr(ConditionExpr)
}

impl Condition {
    pub fn unwrap_as_expr(&self) -> Result<&ConditionExpr, String> {
        match self {
            Condition::Cluster(_) => {Err(String::from("The current condition is not an Expr!"))}
            Condition::Expr(e) => {Ok(e)}
        }
    }

    pub fn unwrap_as_cluster(&self) -> Result<&ConditionCluster, String> {
        match self {
            Condition::Cluster(c) => {Ok(c)}
            Condition::Expr(_) => {Err(String::from("The current condition is not a Cluster!"))}
        }
    }

    pub fn is_expr(&self) -> bool {
        match self {
            Condition::Cluster(_) => {false}
            Condition::Expr(_) => {
                true
            }
        }
    }

    pub fn get_field_max_size(&self, table_meta: &TableStructureMetadata) -> usize{
        let mut max_size: usize = 0;
        match self {
            Condition::Cluster(cluster) => {
                for condition in cluster.iter() {
                    max_size = max(max_size, condition.get_field_max_size(table_meta));
                }
            }
            Condition::Expr(e) => {
                max_size = max(max_size, table_meta.get_field_metadata(&e.field).unwrap().size);
            }
        }
        max_size
    }

    pub fn find_index<'a>(&'a self, table_name: &str, table_manager: &'a TableManager) -> Option<&Box<dyn Table>> {
        let mut has_indexed: Option<&Box<dyn Table>> = None;
        match self {
            Condition::Cluster(cluster) => {
                for condition in cluster.iter() {
                    has_indexed = condition.find_index(table_name, table_manager);
                    if has_indexed.is_some() {
                        break;
                    }
                }
            }
            Condition::Expr(e) => {
                has_indexed = table_manager.find_index_for_field(table_name, &e.field);
            }
        }
        has_indexed
    }
}

#[derive(PartialEq, Debug, PartialOrd, Clone)]
pub(crate) struct ConditionCluster {
    pub logical_operator: LogicalOperator,
    pub conditions: Vec<Condition>,
}

impl ConditionCluster {
    pub(crate) fn new(logical_operator: LogicalOperator, conditions: Vec<Condition>) -> ConditionCluster {
        ConditionCluster { logical_operator, conditions }
    }

    pub fn iter(&self) -> Iter<Condition> {
        self.conditions.iter()
    }
}

#[derive(PartialEq, Debug, PartialOrd, Clone)]
pub(crate) struct ConditionExpr {
    pub logical_operator: LogicalOperator,
    pub field: String,
    pub operator: Operator,
    pub value: Value,
}

impl ConditionExpr {
    pub(crate) fn new(
        logical_operator: LogicalOperator,
        field: String,
        operator: Operator,
        value: Value,
    ) -> ConditionExpr {
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

impl Order {
    pub fn is_asc(&self) -> bool {
        match self {
            Order::ASC => true,
            _ => false,
        }
    }

    pub fn is_desc(&self) -> bool {
        !self.is_asc()
    }
}

impl TryFrom<&str> for Order {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "asc" => Ok(Order::ASC),
            "desc" => Ok(Order::DESC),
            _ => Err("Unknown Order."),
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug, Clone)]
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

impl LogicalOperator {
    pub fn operate(&self, b1: bool, b2: bool) -> bool {
        match self {
            LogicalOperator::OR => b1 | b2,
            LogicalOperator::AND => b1 & b2,
        }
    }

    pub fn combine(&self, other: LogicalOperator) -> LogicalOperator {
        match self {
            LogicalOperator::OR => {
                LogicalOperator::OR
            }
            LogicalOperator::AND => {
                other
            }
        }
    }
}

impl TryFrom<&str> for LogicalOperator {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "or" => Ok(LogicalOperator::OR),
            "and" => Ok(LogicalOperator::AND),
            _ => Err("Unknown logical operator."),
        }
    }
}

impl Operator {
    pub(crate) fn operate(&self, a: &Value, b: &Value) -> bool {
        match self {
            EQUALS(negative) => (a == b) ^ negative,
            GT => a > b,
            GTE => a >= b,
            LT => a < b,
            LTE => a <= b,
            IN(negative) => {
                if let Value::ARRAY(vec) = b {
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
            "=" => Ok(EQUALS(false)),
            "!=" => Ok(EQUALS(true)),
            ">" => Ok(GT),
            ">=" => Ok(GTE),
            "<" => Ok(LT),
            "<=" => Ok(LTE),
            "in" => Ok(IN(false)),
            "not in" => Ok(IN(true)),
            _ => Err(format!("operator `{}` does not exist;", value)),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    INTEGER(i32),
    FLOAT(f32),
    BOOLEAN(bool),
    STRING(String),
    ARRAY(Vec<Value>),
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Value::INTEGER(i) => *i == other.unwrap_as_int().unwrap(),
            Value::FLOAT(f) => *f == other.unwrap_as_float().unwrap(),
            Value::BOOLEAN(b) => *b == other.unwrap_into_bool().unwrap(),
            Value::STRING(s) => s == other.unwrap_as_string().unwrap(),
            Value::ARRAY(a) => a == other.unwrap_as_array().unwrap(),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            Value::INTEGER(i) => i.partial_cmp(&other.unwrap_as_int().unwrap()),
            Value::FLOAT(f) => f.partial_cmp(&other.unwrap_as_float().unwrap()),
            Value::BOOLEAN(b) => b.partial_cmp(&other.unwrap_into_bool().unwrap()),
            Value::STRING(s) => s.partial_cmp(&other.unwrap_as_string().unwrap()),
            Value::ARRAY(_) => None,
        }
    }
}

impl Value {
    pub(crate) fn are_same_variant(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::INTEGER(_), Value::INTEGER(_)) => true,
            (Value::FLOAT(_), Value::FLOAT(_)) => true,
            (Value::STRING(_), Value::STRING(_)) => true,
            (Value::ARRAY(_), Value::ARRAY(_)) => true,
            (Value::BOOLEAN(_), Value::BOOLEAN(_)) => true,
            _ => false,
        }
    }

    pub fn unwrap_as_int(&self) -> Result<i32, &str> {
        match self {
            Value::INTEGER(v) => Ok(*v),
            _ => Err("Current Value is not an Integer."),
        }
    }

    pub fn unwrap_as_float(&self) -> Result<f32, &str> {
        match self {
            Value::FLOAT(v) => Ok(*v),
            _ => Err("Current Value is not a Float."),
        }
    }

    pub fn unwrap_as_string(&self) -> Result<&String, &str> {
        match self {
            Value::STRING(v) => Ok(v),
            _ => Err("Current Value is not a String."),
        }
    }

    pub fn unwrap_as_array(&self) -> Result<&Vec<Value>, &str> {
        match self {
            Value::ARRAY(v) => Ok(v),
            _ => Err("Current Value is not an Array."),
        }
    }

    pub fn unwrap_into_bool(&self) -> Result<bool, &str> {
        match self {
            Value::BOOLEAN(v) => Ok(*v),
            _ => Err("Current Value is not a Boolean."),
        }
    }

    pub(crate) fn is_array(&self) -> bool {
        match self {
            Value::ARRAY(_) => true,
            _ => false,
        }
    }

    pub fn from_bytes(key_type: &DataType, bytes: &[u8]) -> Value {
        Self::from_ptr(key_type, bytes.as_ptr())
    }

    pub fn from_ptr(key_type: &DataType, src: *const u8) -> Value {
        unsafe {
            match key_type {
                DataType::TEXT(size) => {
                    let mut bytes = Vec::<u8>::with_capacity(*size);
                    ptr::copy_nonoverlapping(src, bytes.as_mut_ptr(), *size);
                    bytes.set_len(*size);
                    Value::STRING(u8_array_to_string(bytes.as_slice()))
                }
                DataType::INTEGER => {
                    let key: i32 = 0;
                    ptr::copy_nonoverlapping(
                        src,
                        &key as *const i32 as *mut u8,
                        key_type.get_size(),
                    );
                    Value::INTEGER(key)
                }
                DataType::FLOAT => {
                    let key: f32 = 0.0;
                    ptr::copy_nonoverlapping(
                        src,
                        &key as *const f32 as *mut u8,
                        key_type.get_size(),
                    );
                    Value::FLOAT(key)
                }
                DataType::BOOLEAN => {
                    let key: bool = false;
                    ptr::copy_nonoverlapping(
                        src,
                        &key as *const bool as *mut u8,
                        key_type.get_size(),
                    );
                    Value::BOOLEAN(key)
                }
            }
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            Value::INTEGER(i) => i.to_string(),
            Value::FLOAT(f) => f.to_string(),
            Value::BOOLEAN(b) => b.to_string(),
            Value::STRING(s) => s.to_string(),
            Value::ARRAY(a) => {
                let mut s = String::new();
                s.push('[');
                a.iter().for_each(|v| {
                    s.push_str(&v.to_string());
                    s.push_str(",")
                });
                s.remove(s.len() - 1);
                s.push(']');
                s
            }
        }
    }
}

#[derive(PartialEq, PartialOrd, Debug, Copy, Clone)]
pub enum DataType {
    TEXT(usize),
    INTEGER,
    FLOAT,
    BOOLEAN,
}

impl DataType {
    pub fn is_text(&self) -> bool {
        match self {
            DataType::TEXT(_) => true,
            _ => false,
        }
    }
    pub fn to_bit_code(&self) -> u8 {
        match self {
            DataType::TEXT(_) => 0b0000_0000,
            DataType::INTEGER => 0b0000_0001,
            DataType::FLOAT => 0b0000_0010,
            DataType::BOOLEAN => 0b0000_0011,
        }
    }

    pub fn from_bit_code(bit_code: u8) -> Result<DataType, String> {
        match bit_code {
            0b0000_0000 => Ok(DataType::TEXT(TEXT_DEFAULT_SIZE)),
            0b0000_0001 => Ok(DataType::INTEGER),
            0b0000_0010 => Ok(DataType::FLOAT),
            0b0000_0011 => Ok(DataType::BOOLEAN),
            _ => Err(format!("Unknown bit code {}", bit_code)),
        }
    }

    pub fn get_size(&self) -> usize {
        match self {
            DataType::TEXT(size) => *size,
            DataType::INTEGER => INTEGER_SIZE,
            DataType::FLOAT => FLOAT_SIZE,
            DataType::BOOLEAN => BOOLEAN_SIZE,
        }
    }
}
