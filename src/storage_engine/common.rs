extern crate core;

use std::io::{Read, Write};
use std::{fs, ptr};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use crate::build_path;
use crate::sql_engine::sql_structs::{DataType, FieldDefinition, Value};
use crate::utils::utils::{copy, copy_nonoverlapping, list_files_of_folder, u8_array_to_string};
use crate::storage_engine::config::*;
use crate::storage_engine::tables::{BtreeTable, SequentialTable, Table};

pub struct TableManager {
    tables: HashMap<String, (Rc<TableStructureMetadata> , Vec<(Box<dyn Table>)>)>,
}

impl TableManager {
    pub fn new() -> TableManager {
        TableManager {
            tables: HashMap::new(),
        }
    }

    pub fn register_new_table(&mut self, table_name: &str, storage_file: &PathBuf) -> Result<(), String>{
        if !self.tables.contains_key(table_name) {
            self.load_tables(table_name)
        } else {
            let (meta, tables) = self.tables.get_mut(table_name).unwrap();
            let table = Self::load_table(storage_file, Rc::clone(&meta))?;
            Ok(tables.push(table))
        }
    }

    pub fn get_tables(&mut self, table_name: &str) -> Result<&Vec<Box<dyn Table>>, String> {
        if !self.tables.contains_key(table_name) {
            self.load_tables(table_name);
        }

        let result: &(Rc<TableStructureMetadata>, Vec<Box<dyn Table>>) = self.tables.get(table_name).unwrap();
        Ok(&result.1)
    }

    fn load_tables(&mut self, table_name: &str) -> Result<(), String>{
        let table_meta = Rc::new(self.load_metadata(table_name)?);
        let storage_files = list_files_of_folder(&build_path!(DATA_FOLDER, table_name))?;
        let mut tables = Vec::<Box<dyn Table>>::new();

        for (file_name, path) in storage_files {
            let file_name = file_name.into_string().unwrap();
            if file_name.ends_with(".frm") { continue }
            let index = file_name.ends_with(".idx");
            let table: Box<dyn Table> = if index {
                Box::new(BtreeTable::new(&path, Rc::clone(&table_meta))?)
            } else {
                Box::new(SequentialTable::new(&path, Rc::clone(&table_meta)).unwrap())
            };
            tables.push(table);
       }
        self.tables.insert(table_name.to_string(), (table_meta, tables));
        Ok(())
    }

    fn load_table(storage_file_name: &PathBuf, table_meta: Rc<TableStructureMetadata>) -> Result<Box<dyn Table>, String> {
        let is_index = storage_file_name.ends_with(".idx");
        if is_index {
            Ok(Box::new(BtreeTable::new(storage_file_name, Rc::clone(&table_meta))?))
        } else {
            Ok(Box::new(SequentialTable::new(storage_file_name, Rc::clone(&table_meta))?))
        }
    }

    pub fn is_field_of_table(&mut self, table_name: &str, field_name: &str) -> bool {
        self.get_field_metadata(table_name, field_name).is_ok()
    }

    pub fn get_field_metadata(&mut self, table_name: &str, field_name: &str) -> Result<&FieldMetadata, String> {
        let map = self.get_table_metadata(table_name)?;
        map.get_field_metadata(field_name)
    }

    pub fn get_table_metadata(&mut self, table_name: &str) -> Result<&TableStructureMetadata, String> {
        let rc = &self.tables.get(table_name).unwrap().0;
        Ok(&*rc)
    }

    fn load_metadata(&mut self, table_name: &str) -> Result<TableStructureMetadata, String> {
        let path = build_path!(DATA_FOLDER, table_name, table_name.to_owned() + ".frm");
        let metadata = unsafe { Self::load_metadata_from_disk(&path)? };
        let tm = TableStructureMetadata::new(table_name, metadata);
        Ok(tm)
    }

    pub fn flash_to_disk(&mut self) {
        for table in self.tables.values_mut() {
            //table.flush_to_disk();
        }
    }

    pub fn print_btree(&mut self, table_name: &str) {
        match self.tables.get_mut(table_name) {
            None => {}
            Some(table) => {
               // table.print_tree(0, 0)
            }
        }
    }

    unsafe fn load_metadata_from_disk(path: &Path) -> Result<HashMap<String, FieldMetadata>, String> {
        let metadata = fs::read(path).unwrap();
        let mut metadata_pointer = 0; // pointer that points to the position where we should start reading

        let ptr = metadata.as_ptr();
        let fields_number: usize = 0;
        copy_nonoverlapping(ptr, &fields_number as *const usize as *mut u8, FIELD_NUMBER_SIZE);
        metadata_pointer += FIELD_NUMBER_SIZE;
        let mut map: HashMap<String, FieldMetadata> = HashMap::with_capacity(fields_number);

        let data_type_mask: u8 = 0b0000_0000;
        let primary: u8 = 0b0000_0001;
        let mut value_offset = 0; // offset of the current field's value

        let mut buf: [u8; FIELD_NAME_SIZE] = [0; FIELD_NAME_SIZE];
        for _ in 0..fields_number {
            copy_nonoverlapping(ptr.add(metadata_pointer), buf.as_mut_ptr(), FIELD_NAME_SIZE);
            metadata_pointer += FIELD_NAME_SIZE;

            let field_type_primary: u8 = 0;
            copy_nonoverlapping(ptr.add(metadata_pointer), &field_type_primary as *const u8 as *mut u8, FIELD_TYPE_PRIMARY_SIZE);
            metadata_pointer += FIELD_TYPE_PRIMARY_SIZE;

            let data_type_bit_code = (field_type_primary >> 1) | data_type_mask;
            let mut size: usize = 0;

            let data_type = match DataType::from_bit_code(data_type_bit_code)? {
                DataType::TEXT(_) => {
                    copy(ptr.add(metadata_pointer), &size as *const usize as *mut u8, TEXT_CHARS_NUM_SIZE);
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

            map.insert(u8_array_to_string(&buf), FieldMetadata::new(definition, value_offset, size));

            value_offset += size;
        }

        Ok(map)
    }

    fn load_data_type(byte: u8) {
        let second_bit_mask: u8 = 0b0000_0010;
        let third_bit_mask: u8 = 0b0000_0100;

        let second_bit = (byte & second_bit_mask) != 0;
        let third_bit = (byte & third_bit_mask) != 0;

        if !second_bit && !third_bit {
            println!("Flag 0 is set");
        } else if !second_bit && third_bit {
            println!("Flag 1 is set");
        } else if second_bit && !third_bit {
            println!("Flag 2 is set");
        } else {
            println!("Flag 3 is set");
        }
    }
}

pub(crate) type Page = [u8; PAGE_SIZE];


#[derive(Debug, Hash, Eq, PartialEq)]
pub struct RowBytes {
    pub data: Box<[u8]>
}

impl RowBytes {
    fn new_indexed_row(data: Box<[u8]>) -> RowBytes {
        RowBytes {
            data
        }
    }

    pub(crate) fn serialize_row(&self, destination: *mut u8) {
        unsafe {
            ptr::copy_nonoverlapping(
                self.data.as_ptr(),
                destination,
                self.data.len(),
            );
        }
    }

    pub(crate) fn deserialize_row(source: *const u8, row_size: usize) -> RowBytes {
        let mut data = Vec::<u8>::with_capacity(row_size);
        unsafe {
            ptr::copy_nonoverlapping(
                source,
                data.as_mut_ptr(),
                row_size,
            );
            data.set_len(row_size);
        }

        RowBytes {
            data: data.into_boxed_slice()
        }
    }

    pub fn read_key(&self, key_type: &DataType, key_offset: usize, key_size: usize) -> Value {
        Value::from_bytes(key_type, &self.data[key_offset..key_offset + key_size])
    }
}

pub struct SelectResult<'a> {
    pub field_offset_size_triples: Vec<(&'a str, usize, usize)>,
    pub rows: Vec<HumanReadableRow>
}

impl<'a> SelectResult<'a> {
    pub fn new(field_offset_size_triples: Vec<(&'a str, usize, usize)>, rows: Vec<HumanReadableRow>) -> SelectResult {
        SelectResult {
            field_offset_size_triples,
            rows
        }
    }
}

pub struct RowToInsert {
    pub(crate) fields: Vec<(String, Value)>,
}

impl RowToInsert {
    pub fn to_bytes(&self, table_meta: &TableStructureMetadata) -> RowBytes {
        let mut bytes = vec![0; table_meta.row_size];
        let buf = bytes.as_mut_ptr();

        unsafe {
            for (name, value) in &self.fields {
                let field_meta = table_meta.get_field_metadata(name).unwrap();
                match value {
                    Value::INTEGER(i) => { copy_nonoverlapping(i as *const i32 as *const u8, buf.add(field_meta.offset), field_meta.size); }
                    Value::FLOAT(f) => { copy_nonoverlapping(f as *const f32 as *const u8, buf.add(field_meta.offset), field_meta.size); }
                    Value::BOOLEAN(b) => { copy_nonoverlapping(b as *const bool as *const u8, buf.add(field_meta.offset), field_meta.size); }
                    Value::STRING(s) => { copy_nonoverlapping(s.as_ptr(), buf.add(field_meta.offset), s.len()); }
                    Value::ARRAY(_) => {}
                    Value::SelectStmt(_) => {}
                }
            }
        }
        RowBytes {
            data: bytes.as_slice().into()
        }
    }
}


pub struct HumanReadableRow {
    pub(crate) fields: Vec<Value>,
}

impl HumanReadableRow {
    fn new(fields: Vec<Value>) -> HumanReadableRow{
        HumanReadableRow {
            fields
        }
    }

    fn to_string(&self) -> String {
        let mut s = String::new();
       /* self.fields.iter().for_each(|(name, value)| s.push_str(format!("{}: {},", name, value.to_string()).as_str()));
        s.remove(s.len() - 1);*/
        s
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
    pub fields_metadata: HashMap<String, FieldMetadata>,
}

impl TableStructureMetadata {
    fn new(table_name: &str, fields_metadata: HashMap<String, FieldMetadata>) -> TableStructureMetadata {
        let row_size = fields_metadata.values().map(|m| m.size).reduce(|a, b| a + b).unwrap();
        TableStructureMetadata {
            table_name: table_name.to_string(),
            row_size,
            fields_metadata,
        }
    }

    pub fn get_field_metadata(&self, field_name: &str) -> Result<&FieldMetadata, String> {
        match self.fields_metadata.get(field_name) {
            None => {
                Err(format!("Field {} does not found in the table {}!", field_name, self.table_name))
            }
            Some(fm) => {Ok(fm)}
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