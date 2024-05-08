extern crate core;

use std::{fs, ptr};
use std::collections::HashMap;
use std::io::Write;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;

use prettytable::Row;

use crate::build_path;
use crate::sql_engine::sql_structs::{DataType, FieldDefinition, Value};
use crate::storage_engine::config::*;
use crate::storage_engine::tables::{BtreeTable, SequentialTable, Table};
use crate::utils::utils::{copy, copy_nonoverlapping, list_files_of_folder, u8_array_to_string};

pub struct TableManager {
    tables: HashMap<String, (Rc<TableStructureMetadata>, Vec<Box<dyn Table>>)>
}

impl TableManager {
    pub fn new() -> TableManager {
        TableManager {
            tables: HashMap::new()
        }
    }

    pub fn find_index_for_field(&self, table_name: &str, field: &str) -> Option<&Box<dyn Table>> {
        let tables = self.tables.get(table_name).unwrap();
        tables.1.iter().find(|t| t.as_any().downcast_ref::<BtreeTable>().unwrap().key_field_name == field)
    }

    pub fn register_new_table(
        &mut self,
        table_name: &str,
        storage_file: &PathBuf,
    ) -> Result<(), String> {
        if !self.tables.contains_key(table_name) {
            self.load_tables(table_name)
        } else {
            let (meta, tables) = self.tables.get_mut(table_name).unwrap();
            let table = Self::load_table(storage_file, Rc::clone(&meta))?;
            Ok(tables.push(table))
        }
    }

    pub fn get_tables(&mut self, table_name: &str) -> Result<&mut Vec<Box<dyn Table>>, String> {
        if !self.tables.contains_key(table_name) {
            self.load_tables(table_name).unwrap();
        }

        let result: &mut (Rc<TableStructureMetadata>, Vec<Box<dyn Table>>) =
            self.tables.get_mut(table_name).unwrap();
        Ok(&mut result.1)
    }

    fn load_tables(&mut self, table_name: &str) -> Result<(), String> {
        let table_meta = Rc::new(self.load_metadata(table_name)?);
        let storage_files = list_files_of_folder(&build_path!(DATA_FOLDER, table_name))?;
        let mut tables = Vec::<Box<dyn Table>>::new();

        for (file_name, path) in storage_files {
            let file_name = file_name.into_string().unwrap();
            if file_name.ends_with(".frm") {
                continue;
            }
            let index = file_name.ends_with(".idx");
            let table: Box<dyn Table> = if index {
                Box::new(BtreeTable::new(&path, Rc::clone(&table_meta))?)
            } else {
                Box::new(SequentialTable::new(&path, Rc::clone(&table_meta)).unwrap())
            };
            tables.push(table);
        }
        self.tables
            .insert(table_name.to_string(), (table_meta, tables));
        Ok(())
    }

    fn load_table(
        storage_file_name: &PathBuf,
        table_meta: Rc<TableStructureMetadata>,
    ) -> Result<Box<dyn Table>, String> {
        let is_index = storage_file_name.ends_with(".idx");
        if is_index {
            Ok(Box::new(BtreeTable::new(
                storage_file_name,
                Rc::clone(&table_meta),
            )?))
        } else {
            Ok(Box::new(SequentialTable::new(
                storage_file_name,
                Rc::clone(&table_meta),
            )?))
        }
    }

    pub fn get_table_metadata(
        &mut self,
        table_name: &str,
    ) -> Result<&TableStructureMetadata, String> {
        if self.tables.is_empty() {
            self.load_tables(table_name)?;
        }
        match self.tables.get(table_name) {
            None => {
                return Err(format!("Table {} is not exist.", table_name));
            }
            Some(rc) => Ok(&*(rc.0)),
        }
    }

    fn load_metadata(&mut self, table_name: &str) -> Result<TableStructureMetadata, String> {
        let path = build_path!(DATA_FOLDER, table_name, table_name.to_owned() + ".frm");
        let metadata = unsafe { Self::load_metadata_from_disk(&path)? };
        let tm = TableStructureMetadata::new(table_name, metadata);
        Ok(tm)
    }

    pub fn flash_to_disk(&mut self) {
        for (_, tables) in self.tables.values_mut() {
            tables.iter_mut().for_each(|t| t.flush_to_disk())
        }
    }

    pub fn print_btree(&mut self, table_name: &str) {
        println!("{}", table_name)
    }

    unsafe fn load_metadata_from_disk(
        path: &Path,
    ) -> Result<Vec<(String, u32, Rc<FieldMetadata>)>, String> {
        let metadata = fs::read(path).unwrap();
        let mut metadata_pointer = 0; // pointer that points to the position where we should start reading

        let ptr = metadata.as_ptr();
        let fields_number: usize = 0;
        copy_nonoverlapping(
            ptr,
            &fields_number as *const usize as *mut u8,
            FIELD_NUMBER_SIZE,
        );
        metadata_pointer += FIELD_NUMBER_SIZE;
        let mut fields: Vec<(String, u32, Rc<FieldMetadata>)> = Vec::with_capacity(fields_number);

        let data_type_mask: u8 = 0b0000_0000;
        let primary: u8 = 0b0000_0001;
        let mut value_offset = 0; // offset of the current field's value

        let mut buf: [u8; FIELD_NAME_SIZE] = [0; FIELD_NAME_SIZE];
        for i in 0..fields_number {
            copy_nonoverlapping(ptr.add(metadata_pointer), buf.as_mut_ptr(), FIELD_NAME_SIZE);
            metadata_pointer += FIELD_NAME_SIZE;

            let field_type_primary: u8 = 0;
            copy_nonoverlapping(
                ptr.add(metadata_pointer),
                &field_type_primary as *const u8 as *mut u8,
                FIELD_TYPE_PRIMARY_SIZE,
            );
            metadata_pointer += FIELD_TYPE_PRIMARY_SIZE;

            let data_type_bit_code = (field_type_primary >> 1) | data_type_mask;
            let mut size: usize = 0;

            let data_type = match DataType::from_bit_code(data_type_bit_code)? {
                DataType::TEXT(_) => {
                    copy(
                        ptr.add(metadata_pointer),
                        &size as *const usize as *mut u8,
                        TEXT_CHARS_NUM_SIZE,
                    );
                    metadata_pointer += TEXT_CHARS_NUM_SIZE;
                    DataType::TEXT(size)
                }
                DataType::INTEGER => {
                    size = INTEGER_SIZE;
                    DataType::INTEGER
                }
                DataType::FLOAT => {
                    size = FLOAT_SIZE;
                    DataType::FLOAT
                }
                DataType::BOOLEAN => {
                    size = BOOLEAN_SIZE;
                    DataType::BOOLEAN
                }
            };

            let is_primary = (field_type_primary & primary) == 1;

            let definition = FieldDefinition::new(u8_array_to_string(&buf), data_type, is_primary);

            fields.push((
                u8_array_to_string(&buf),
                i as u32,
                Rc::new(FieldMetadata::new(definition, value_offset, size)),
            ));

            value_offset += size;
        }

        Ok(fields)
    }
}

pub(crate) type Page = [u8; PAGE_SIZE];

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct RowBytes {
    pub data: Vec<u8>,
}

impl Deref for RowBytes {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl RowBytes {
    pub(crate) fn serialize_row(&self, destination: *mut u8) {
        unsafe {
            ptr::copy_nonoverlapping(self.as_ptr(), destination, self.len());
        }
    }

    pub(crate) fn deserialize_row(source: *const u8, row_size: usize) -> RowBytes {
        let mut data = Vec::<u8>::with_capacity(row_size);
        unsafe {
            ptr::copy_nonoverlapping(source, data.as_mut_ptr(), row_size);
            data.set_len(row_size);
        }

        RowBytes { data }
    }

    pub fn read_key(&self, key_type: &DataType, key_offset: usize, key_size: usize) -> Value {
        Value::from_bytes(key_type, &self[key_offset..key_offset + key_size])
    }
}

pub struct SelectResult<'a> {
    pub fields: Vec<&'a str>,
    pub rows: Vec<RowValues>,
}

impl<'a> SelectResult<'a> {
    pub fn new(fields: Vec<&'a str>, rows: Vec<RowValues>) -> SelectResult {
        SelectResult { fields, rows }
    }

    pub(crate) fn print(&self) {
        let mut table = prettytable::Table::new();

        table.add_row(Row::new(
            self.fields
                .iter()
                .map(|f| prettytable::Cell::new(f))
                .collect(),
        ));

        self.rows.iter().for_each(|r| {
            table.add_row(Row::new(
                r.fields
                    .iter()
                    .map(|f| prettytable::Cell::new(f.to_string().as_str()))
                    .collect(),
            ));
        });

        table.printstd();
    }
}

pub struct RowToInsert<'a> {
    pub(crate) field_value_pairs: Vec<(&'a String, &'a Value)>,
    pub(crate) raw_data: RowBytes,
}

impl<'a> RowToInsert<'a> {
    pub fn new(
        fields: &'a Vec<String>,
        values: &'a Vec<Value>,
        table_meta: &TableStructureMetadata,
    ) -> RowToInsert<'a> {
        let field_value_pairs: Vec<(&String, &Value)> = fields.iter().zip(values.iter()).collect();

        let bytes = Self::to_bytes(&field_value_pairs, table_meta);
        RowToInsert {
            field_value_pairs,
            raw_data: bytes,
        }
    }

    pub fn to_bytes(
        field_value_pair: &Vec<(&'a String, &'a Value)>,
        table_meta: &TableStructureMetadata,
    ) -> RowBytes {
        let mut data = vec![0; table_meta.row_size];
        let buf = data.as_mut_ptr();

        unsafe {
            for (name, value) in field_value_pair {
                let field_meta = table_meta.get_field_metadata(name).unwrap();
                match value {
                    Value::INTEGER(i) => {
                        copy_nonoverlapping(
                            i as *const i32 as *const u8,
                            buf.add(field_meta.offset),
                            field_meta.size,
                        );
                    }
                    Value::FLOAT(f) => {
                        copy_nonoverlapping(
                            f as *const f32 as *const u8,
                            buf.add(field_meta.offset),
                            field_meta.size,
                        );
                    }
                    Value::BOOLEAN(b) => {
                        copy_nonoverlapping(
                            b as *const bool as *const u8,
                            buf.add(field_meta.offset),
                            field_meta.size,
                        );
                    }
                    Value::STRING(s) => {
                        copy_nonoverlapping(s.as_ptr(), buf.add(field_meta.offset), s.len());
                    }
                    Value::ARRAY(_) => {}
                }
            }
        }

        RowBytes { data }
    }
}

pub struct RowValues {
    pub fields: Vec<Rc<Value>>,
}

impl Deref for RowValues {
    type Target = Vec<Rc<Value>>;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

impl RowValues {
    pub fn new(fields: Vec<Rc<Value>>) -> RowValues {
        RowValues { fields }
    }
}

pub fn new_input_buffer() -> &'static str {
    let mut input = String::new();
    print!("sql>");
    loop {
        std::io::stdout().flush().expect("flush failed!");
        std::io::stdin().read_line(&mut input).unwrap();
        if input.trim().ends_with(";") {
            break;
        }
        print!(">")
    }
    input.to_lowercase().leak().trim()
}

pub struct TableStructureMetadata {
    pub table_name: String,
    pub row_size: usize,
    pub fields_meta_map: HashMap<String, (u32, Rc<FieldMetadata>)>,
    pub fields: Vec<Rc<FieldMetadata>>,
    pub fields_max_size: usize
}

impl TableStructureMetadata {
    fn new(
        table_name: &str,
        fields_metadata: Vec<(String, u32, Rc<FieldMetadata>)>,
    ) -> TableStructureMetadata {
        let row_size = fields_metadata
            .iter()
            .map(|(_, _, m)| m.size)
            .reduce(|a, b| a + b)
            .unwrap();

        let fields:Vec<Rc<FieldMetadata>> = fields_metadata
                                                .iter()
                                                .map(|(_, _, m)| Rc::clone(m))
                                                .collect();

        let fields_max_size = fields.iter().map(|f| f.size).max().unwrap();

        let fields_meta_map: HashMap<String, (u32, Rc<FieldMetadata>)> = fields_metadata
            .into_iter()
            .map(|(name, offset, m)| (name, (offset, Rc::clone(&m))))
            .collect();
        TableStructureMetadata {
            table_name: table_name.to_string(),
            row_size,
            fields_meta_map,
            fields,
            fields_max_size
        }
    }

    pub fn get_field_metadata(&self, field_name: &str) -> Result<&FieldMetadata, String> {
        match self.fields_meta_map.get(field_name) {
            None => Err(format!(
                "Field `{}` does not found in the table `{}`!",
                field_name, self.table_name
            )),
            Some((_, fm)) => Ok(fm),
        }
    }
}

pub struct FieldMetadata {
    pub data_def: FieldDefinition,
    pub offset: usize,
    pub size: usize,
}

impl FieldMetadata {
    pub fn new(field_definition: FieldDefinition, offset: usize, size: usize) -> FieldMetadata {
        FieldMetadata {
            data_def: field_definition,
            offset,
            size,
        }
    }
}
