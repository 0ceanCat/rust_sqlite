use std::any::Any;
use std::cmp::max;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::PathBuf;
use std::ptr;
use std::ptr::null_mut;
use std::rc::Rc;

use crate::sql_engine::sql_structs::{Condition, ConditionCluster, ConditionExpr, DataType, LogicalOperator, Value};
use crate::storage_engine::common::{RowBytes, RowToInsert, TableStructureMetadata};
use crate::storage_engine::config::*;
use crate::storage_engine::cursor::{ReadCursor, WriteReadCursor};
use crate::storage_engine::enums::NodeType;
use crate::storage_engine::pagers::{BtreePager, SequentialPager};
use crate::utils::utils::{copy, copy_nonoverlapping, ToU8, u8_array_to_string};

pub trait Table{
    fn begin(&mut self) -> WriteReadCursor;
    fn insert(&mut self, row: &RowToInsert) -> Result<(), String>;
    fn find_by_condition_clusters(
        &self,
        condition_clusters: &Vec<ConditionCluster>,
    ) -> Vec<RowBytes>;
    fn find_by_condition_cluster(
        &self,
        condition_cluster: &ConditionCluster,
    ) -> Vec<RowBytes>;
    fn end(&mut self) -> WriteReadCursor;
    fn is_btree(&self) -> bool;
    fn get_all(&self) -> Vec<RowBytes>;
    fn get_row_size(&self) -> usize;
    fn get_num_cells(&self, page_index: usize) -> usize;
    fn get_next_page_index(&self, page_index: usize) -> usize;
    fn get_row_value(&self, page_index: usize, cell_index: usize) -> *const u8;
    fn get_row_value_mut(&mut self, page_index: usize, cell_index: usize) -> *mut u8;
    fn flush_to_disk(&mut self);
    fn print_tree(&self, page_index: usize, cell_index: usize);
    fn as_any(&self) -> &dyn Any;
}

pub struct BtreeMeta {
    data_type: DataType,
    is_primary: bool,
    key_size: usize,
    key_field_name: String,
}

pub struct BtreeTable {
    pub root_page_index: usize,
    pub pager: BtreePager,
    pub is_primary: bool,
    pub key_type: DataType,
    pub key_size: usize,
    pub key_offset_in_row: usize,
    pub key_field_name: String,
    pub row_size: usize,
    table_metadata: Rc<TableStructureMetadata>,
}

impl Table for BtreeTable {
    fn begin(&mut self) -> WriteReadCursor {
        self.find_smallest_or_biggest_key(false)
    }

    fn insert(&mut self, row: &RowToInsert) -> Result<(), String> {
        let key = row
            .field_value_pairs
            .iter()
            .filter(|(name, _)| **name == self.key_field_name)
            .next();
        if key.is_none() {
            return Err(format!("Primary key {} must be set.", self.key_field_name));
        }

        let (_, key_value) = key.unwrap();

        let cursor = self.table_find_by_key(key_value);
        let page_index = cursor.page_index;
        let cell_index = cursor.cell_index;

        let page = self.pager.get_or_create_page(page_index);
        let num_cells = BtreePager::get_leaf_node_num_cells(page);
        if num_cells >= self.pager.get_body_layout().leaf_node_max_cells {
            self.split_and_insert(page_index, cell_index, &row.raw_data);
        } else {
            self.move_and_insert(page_index, cell_index, &row.raw_data);
        }
        Ok(())
    }

    fn find_by_condition_clusters(
        &self,
        condition_clusters: &Vec<ConditionCluster>,
    ) -> Vec<RowBytes> {
        let mut result = vec![];
        let mut last_op = LogicalOperator::OR;
        for cluster in condition_clusters {
            let cluster_result = self.find_by_condition_cluster(cluster);
            if last_op == LogicalOperator::OR {
                result.extend(cluster_result);
                last_op = last_op.combine(cluster.logical_operator);
            } else {
                let mut set: HashSet<RowBytes> = HashSet::from_iter(result.into_iter());
                result = vec![];
                for r in cluster_result {
                    if set.contains(&r) {
                        result.push(r);
                    }
                }
            }
        }
        result
    }

    fn find_by_condition_cluster(&self, cluster: &ConditionCluster) -> Vec<RowBytes> {
        let mut result = Vec::new();
        let mut first = true;
        unsafe {
            let mut and_exprs = vec![];
            let mut or_clusters = vec![];
            for condition in cluster.iter() {
                if cluster.logical_operator == LogicalOperator::AND && condition.is_expr(){
                    and_exprs.push(condition.unwrap_as_expr().unwrap());
                } else {
                    or_clusters.push(condition)
                }
            }

            if !and_exprs.is_empty() {
                result = self.find_by_condition_exprs(and_exprs);
            }

            for oc in or_clusters {
                if oc.is_expr() {
                    result.extend(self.find_by_condition_exprs(vec![oc.unwrap_as_expr().unwrap()]));
                } else {
                    let cluster = oc.unwrap_as_cluster().unwrap();
                    let cluster_result = self.find_by_condition_cluster(cluster);
                    if cluster.logical_operator == LogicalOperator::OR || first {
                        result.extend(cluster_result);
                        first = false;
                    } else {
                        let mut set: HashSet<RowBytes> = HashSet::from_iter(result.into_iter());
                        result = vec![];
                        for r in cluster_result {
                            if set.contains(&r) {
                                result.push(r);
                            }
                        }
                    }
                }
            }
        }
        result
    }

    fn end(&mut self) -> WriteReadCursor {
        self.find_smallest_or_biggest_key(true)
    }

    fn is_btree(&self) -> bool {
        true
    }

    fn get_all(&self) -> Vec<RowBytes> {
        let row_size = self.table_metadata.row_size;
        let mut cursor = ReadCursor::at(self, 0, 0);
        let mut result = Vec::new();
        while !cursor.is_end() {
            result.push(RowBytes::deserialize_row(cursor.cursor_value(), row_size));
            cursor.cursor_advance();
        }
        result
    }

    fn get_row_size(&self) -> usize {
        self.row_size
    }

    fn get_num_cells(&self, page_index: usize) -> usize {
        BtreePager::get_leaf_node_num_cells(self.pager.get_page(page_index))
    }

    fn get_next_page_index(&self, page_index: usize) -> usize {
        BtreePager::get_leaf_node_next_leaf(self.pager.get_page(page_index))
    }

    fn get_row_value(&self, page_index: usize, cell_index: usize) -> *const u8 {
        let page = self.pager.get_page(page_index);
        self.pager
            .get_leaf_node_value(page.cast_mut(), cell_index)
            .cast_const()
    }

    fn get_row_value_mut(&mut self, page_index: usize, cell_index: usize) -> *mut u8 {
        let page = self.pager.get_or_create_page(page_index);
        self.pager.get_leaf_node_value(page, cell_index)
    }

    fn flush_to_disk(&mut self) {
        for x in 0..TABLE_MAX_PAGES {
            if !self.pager.flush_page_to_disk(x) {
                break;
            }
        }
    }

    fn print_tree(&self, page_index: usize, cell_index: usize) {
        println!("{}", page_index);
        println!("{}", cell_index);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl BtreeTable {
    pub(crate) fn new(
        path: &PathBuf,
        table_metadata: Rc<TableStructureMetadata>,
    ) -> Result<BtreeTable, String> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
        {
            Ok(mut file) => {
                let meta = Self::load_metadata(&mut file, &table_metadata.table_name)?;
                let pager = BtreePager::open(meta.key_size, table_metadata.row_size, file);
                let mut pager = pager;
                if pager.get_pager_total_size() == 0 {
                    let first_page = pager.get_or_create_page(0);
                    BtreePager::initialize_leaf_node(first_page);
                    BtreePager::set_root_node(first_page, true);
                }
                Ok(BtreeTable {
                    root_page_index: 0,
                    pager,
                    is_primary: meta.is_primary,
                    key_type: meta.data_type,
                    key_size: meta.key_size,
                    key_offset_in_row: table_metadata
                        .get_field_metadata(&meta.key_field_name)?
                        .offset,
                    key_field_name: meta.key_field_name,
                    row_size: table_metadata.row_size,
                    table_metadata,
                })
            }
            Err(_) => Err(format!(
                "Can not open index data file of table {}!",
                table_metadata.table_name
            )),
        }
    }

    unsafe fn find_by_condition_exprs(&self, mut exprs: Vec<&ConditionExpr>) -> Vec<RowBytes> {
        exprs.sort_by(|e1, e2| (e2.field == self.key_field_name).to_u8().cmp(&(e1.field == self.key_field_name).to_u8()));
        let max_field_size = exprs.iter().map(|e| self.table_metadata.get_field_metadata(&e.field).unwrap().size).max().unwrap();
        let mut buf = Vec::<u8>::with_capacity(max_field_size);
        let mut result = vec![];

        let mut cursor;

        if exprs.first().unwrap().field == self.key_field_name {
            cursor = self.table_find_by_key(&exprs.first().unwrap().value);
        } else {
            cursor = self.find_smallest_or_biggest_key(false);
        }

        while !cursor.is_end() {
            let row_ptr = cursor.cursor_value();
            let mut matched = true;
            for expr in exprs.iter() {
                matched &= self.read_compare_value(row_ptr, &mut buf, expr);
                if !matched {
                    break
                }
            }

            if matched {
                result.push(RowBytes::deserialize_row(row_ptr, self.row_size));
            }
            cursor.cursor_advance();
        }


        result
    }

    fn load_metadata(file: &mut File, table_name: &str) -> Result<BtreeMeta, String> {
        let mut metadata: [u8; BTREE_METADATA_SIZE] = [0; BTREE_METADATA_SIZE];
        match file.read(&mut metadata) {
            Ok(_) => {}
            Err(_) => {
                return Err(format!(
                    "Can not load Btree metadata for table {}",
                    table_name
                ));
            }
        };

        let data_type_mask: u8 = 0b0000_0000;
        let primary_mask: u8 = 0b0000_0001;
        let field_type_primary: u8 = 0;

        copy_nonoverlapping(
            metadata.as_ptr(),
            &field_type_primary as *const u8 as *mut u8,
            INDEXED_FIELD_TYPE_PRIMARY,
        );
        let data_type_bit_code = (field_type_primary >> 1) | data_type_mask;
        let is_primary = (field_type_primary & primary_mask) == 1;

        let data_type = DataType::from_bit_code(data_type_bit_code)?;
        let key_size: usize = 0;
        let mut key_name: [u8; INDEXED_FIELD_NAME_SIZE] = [0; INDEXED_FIELD_NAME_SIZE];
        unsafe {
            copy_nonoverlapping(
                metadata.as_ptr().add(INDEXED_FIELD_SIZE_OFFSET),
                &key_size as *const usize as *mut u8,
                INDEXED_FIELD_SIZE,
            );
            copy_nonoverlapping(
                metadata.as_ptr().add(INDEXED_FIELD_NAME_SIZE_OFFSET),
                key_name.as_mut_ptr(),
                INDEXED_FIELD_NAME_SIZE,
            );
        }
        let key_field_name = u8_array_to_string(&key_name);
        Ok(BtreeMeta {
            data_type,
            is_primary,
            key_size,
            key_field_name,
        })
    }

    fn split_and_insert(&mut self, page_index: usize, cell_index: usize, row: &RowBytes) {
        /*
          Create a new node and move half the cells over.
          Insert the new value in one of the two nodes.
          Update parent or create a new parent.
        */
        let old_node = self.pager.get_or_create_page(page_index);
        let old_biggest_key = self.pager.get_node_biggest_key(old_node, &self.key_type);
        let new_page_index = self.pager.get_unused_page_num();
        let new_node = self.pager.get_or_create_page(new_page_index);
        BtreePager::initialize_leaf_node(new_node);

        BtreePager::set_parent(new_node, BtreePager::get_parent(old_node.cast_const()));

        BtreePager::set_leaf_node_next_leaf(
            new_node,
            BtreePager::get_leaf_node_next_leaf(old_node.cast_const()),
        );
        BtreePager::set_leaf_node_next_leaf(old_node, new_page_index);

        /*
          All existing keys plus new key should be divided
          evenly between old (left) and new (right) nodes.
          Starting from the right, move each key to correct position.
        */
        for i in (0..=self.pager.get_body_layout().leaf_node_max_cells).rev() {
            let destination_node;
            if i >= self.pager.get_body_layout().leaf_node_left_split_count {
                // upper halves (right halves) will be stored in the new_node
                destination_node = new_node;
            } else {
                destination_node = old_node;
            }
            // index_within_node will always decrement until it arrives to 0, then destination_node will be switched to old_node
            let index_within_node = i % self.pager.get_body_layout().leaf_node_left_split_count;
            let cell_pointer = self
                .pager
                .leaf_node_cell(destination_node, index_within_node);

            if i == cell_index {
                // when this code executes, the value in the cell_pointer was already moved to position i + 1, if cell_pointer is old_node
                // if cell_pointer is new_node, position `index_within_node` is empty
                let key = row.read_key(&self.key_type, self.key_offset_in_row, self.key_size);
                self.pager.set_leaf_node_cell_key(
                    destination_node,
                    index_within_node,
                    self.key_size,
                    &key,
                );
                row.serialize_row(
                    self.pager
                        .get_leaf_node_value(destination_node, index_within_node),
                );
            } else if i > cell_index {
                // copy a node from old_node tail (position i - 1), to destination_node (index_within_node)
                copy(
                    self.pager.leaf_node_cell(old_node, i - 1),
                    cell_pointer,
                    self.pager.get_body_layout().leaf_node_cell_size,
                );
            } else {
                copy(
                    self.pager.leaf_node_cell(old_node, i),
                    cell_pointer,
                    self.pager.get_body_layout().leaf_node_cell_size,
                );
            }
        }

        BtreePager::set_leaf_node_cells_num(
            old_node,
            self.pager.get_body_layout().leaf_node_left_split_count,
        );
        BtreePager::set_leaf_node_cells_num(
            new_node,
            self.pager.get_body_layout().leaf_node_right_split_count,
        );

        if BtreePager::is_root_node(old_node) {
            self.create_new_root(new_page_index);
        } else {
            let parent_page_index = BtreePager::get_parent(old_node.cast_const());
            let new_biggest = self.pager.get_node_biggest_key(old_node, &self.key_type);
            let parent_page = self.pager.get_or_create_page(parent_page_index);

            let old_key_cell_index = self.internal_node_find_child(parent_page, &old_biggest_key);
            // old_node is split and contains left halves rows (lower halves)
            // so it's necessary to replace old_biggest_key to new_biggest_key
            BtreePager::set_internal_node_cell_key(
                parent_page,
                old_key_cell_index,
                self.key_size,
                &new_biggest,
            );
            self.internal_node_insert(parent_page_index, new_page_index);
        }
    }

    fn move_and_insert(&mut self, page_index: usize, cell_index: usize, row: &RowBytes) {
        let page = self.pager.get_or_create_page(page_index);
        let num_cells = BtreePager::get_leaf_node_num_cells(page);
        if cell_index < num_cells {
            copy(
                self.pager.leaf_node_cell(page, cell_index),
                self.pager.leaf_node_cell(page, cell_index + 1),
                self.pager.get_body_layout().leaf_node_cell_size * (num_cells - cell_index),
            )
        }
        let key = row.read_key(&self.key_type, self.key_offset_in_row, self.key_size);
        self.pager
            .set_leaf_node_cell_key(page, cell_index, self.key_size, &key);
        BtreePager::increment_leaf_node_cells_num(page);
        self.pager.mark_page_as_updated(page_index);
        row.serialize_row(self.pager.get_leaf_node_value(page, cell_index));
    }

    pub(crate) fn table_find_by_key(&self, key: &Value) -> WriteReadCursor {
        unsafe {
            let s_ptr: &mut Self = std::mem::transmute(self as *const Self);

            let node_type = (*s_ptr).pager.get_node_type_by_index(self.root_page_index);
            match node_type {
                NodeType::Internal => (*s_ptr).internal_node_find(self.root_page_index, &key),
                NodeType::Leaf => (*s_ptr).leaf_node_find(self.root_page_index, &key),
            }
        }
    }

    pub(crate) fn find_smallest_or_biggest_key(&self, biggest: bool) -> WriteReadCursor {
        unsafe {
            let s_ptr: &mut Self = std::mem::transmute(self as *const Self);
            let node_type = (*s_ptr).pager.get_node_type_by_index(self.root_page_index);
            match node_type {
                NodeType::Internal => {
                    (*s_ptr).internal_node_find_smallest_or_biggest(self.root_page_index, biggest)
                }
                NodeType::Leaf => {
                    (*s_ptr).leaf_node_find_smallest_or_biggest(self.root_page_index, biggest)
                }
            }
        }
    }

    fn leaf_node_find(&mut self, page_index: usize, key: &Value) -> WriteReadCursor {
        let node = self.pager.get_or_create_page(page_index);
        let cells_num = BtreePager::get_leaf_node_num_cells(node);

        let mut min_index = 0;
        let mut right = cells_num;
        while right != min_index {
            let index = (min_index + right) / 2;
            let key_at_index = self
                .pager
                .get_leaf_node_cell_key(node, index, &self.key_type);
            if *key == key_at_index {
                return WriteReadCursor::at(self, page_index, index);
            }
            if *key < key_at_index {
                right = index;
            } else {
                min_index = index + 1;
            }
        }

        WriteReadCursor::at(self, page_index, min_index)
    }

    fn leaf_node_find_smallest_or_biggest(
        &mut self,
        page_index: usize,
        biggest: bool,
    ) -> WriteReadCursor {
        let mut cell_index = 0;
        if biggest {
            cell_index =
                BtreePager::get_leaf_node_num_cells(self.pager.get_or_create_page(page_index))
        }
        WriteReadCursor::at(self, page_index, cell_index)
    }

    pub fn internal_node_find_child(&mut self, node: *const u8, key: &Value) -> usize {
        /*
          Return the index of the child which should contain
          the given key.
        */
        let num_keys = BtreePager::get_internal_node_num_keys(node);
        let mut min_index = 0;
        let mut max_index = num_keys;
        while max_index != min_index {
            let index = (min_index + max_index) / 2;
            let key_at_index = BtreePager::get_internal_node_cell_key(node, index, &self.key_type);
            if *key <= key_at_index {
                max_index = index;
            } else {
                min_index = index + 1;
            }
        }
        min_index
    }

    pub fn internal_node_find_smallest_or_biggest(
        &mut self,
        page_index: usize,
        biggest: bool,
    ) -> WriteReadCursor {
        /*
          Return the index of the child which contains the smallest key
        */
        let node = self.pager.get_or_create_page(page_index);

        let mut key_index: usize = 0;

        if biggest {
            key_index = BtreePager::get_internal_node_num_keys(node) - 1;
        }

        let child_index = BtreePager::get_internal_node_child(node, key_index);
        let child = self.pager.get_or_create_page(child_index);

        match BtreePager::get_node_type(child) {
            NodeType::Leaf => self.leaf_node_find_smallest_or_biggest(child_index, biggest),
            NodeType::Internal => self.internal_node_find_smallest_or_biggest(child_index, biggest),
        }
    }

    fn internal_node_find(&mut self, page_index: usize, key: &Value) -> WriteReadCursor {
        let node = self.pager.get_or_create_page(page_index);
        let cell_index = self.internal_node_find_child(node, key);
        let child_index = BtreePager::get_internal_node_child(node, cell_index);
        let child = self.pager.get_or_create_page(child_index);
        match BtreePager::get_node_type(child) {
            NodeType::Leaf => self.leaf_node_find(child_index, key),
            NodeType::Internal => self.internal_node_find(child_index, key),
        }
    }

    pub(crate) fn create_new_root(&mut self, right_child_page_index: usize) {
        /*
          Handle splitting the root.
          Old root copied to new page, becomes left child.
          Address of right child passed in.
          Re-initialize root page to contain the new root node.
          New root node points to two children.
        */
        let root = self.pager.get_or_create_page(self.root_page_index);
        let right_child = self.pager.get_or_create_page(right_child_page_index);
        let left_child_page_num = self.pager.get_unused_page_num();
        let left_child = self.pager.get_or_create_page(left_child_page_num);

        if let NodeType::Internal = BtreePager::get_node_type(root) {
            BtreePager::initialize_internal_node(right_child);
            BtreePager::initialize_internal_node(left_child);
        }

        /* Left child has data copied from old root */
        unsafe {
            ptr::copy_nonoverlapping(root, left_child, PAGE_SIZE);
            BtreePager::set_root_node(left_child, false)
        };

        if let NodeType::Internal = BtreePager::get_node_type(left_child) {
            let mut child: *mut u8;
            let num_keys = BtreePager::get_internal_node_num_keys(left_child);
            for i in 0..num_keys {
                child = self
                    .pager
                    .get_or_create_page(BtreePager::get_internal_node_child(left_child, i));
                BtreePager::set_parent(child, left_child_page_num);
            }
            child = self
                .pager
                .get_or_create_page(BtreePager::get_internal_node_right_child(left_child));
            BtreePager::set_parent(child, left_child_page_num);
        }

        /* Root node is a new internal node with one key and two children */
        BtreePager::initialize_internal_node(root);
        BtreePager::set_root_node(root, true);

        BtreePager::set_internal_node_num_keys(root, 1);
        // first child index = left child index
        BtreePager::set_internal_node_child(root, 0, left_child_page_num);
        let left_child_biggest_key = self.pager.get_node_biggest_key(left_child, &self.key_type);
        BtreePager::set_internal_node_cell_key(root, 0, self.key_size, &left_child_biggest_key);
        BtreePager::set_internal_node_right_child(root, right_child_page_index);

        BtreePager::set_parent(left_child, self.root_page_index);
        BtreePager::set_parent(right_child, self.root_page_index);
    }

    pub fn internal_node_split_and_insert(
        &mut self,
        parent_page_index: usize,
        child_page_index: usize,
    ) {
        let mut old_page_index = parent_page_index;
        let mut old_node = self.pager.get_or_create_page(parent_page_index);
        let old_max = self.pager.get_node_biggest_key(old_node, &self.key_type);

        let child = self.pager.get_or_create_page(child_page_index);
        let child_max = self.pager.get_node_biggest_key(child, &self.key_type);

        let new_page_index = self.pager.get_unused_page_num();
        /*
            Declaring a flag before updating pointers which
            records whether this operation involves splitting the root -
            if it does, we will insert our newly created node during
            the step where the table's new root is created. If it does
            not, we have to insert the newly created node into its parent
            after the old node's keys have been transferred over. We are not
            able to do this if the newly created node's parent is not a newly
            initialized root node, because in that case its parent may have existing
            keys aside from our old node which we are splitting. If that is true, we
            need to find a place for our newly created node in its parent, and we
            cannot insert it at the correct index if it does not yet have any keys
        */
        let splitting_root = BtreePager::is_root_node(old_node);
        let parent;
        let mut new_node: *mut u8 = null_mut();
        if splitting_root {
            self.create_new_root(new_page_index);
            parent = self.pager.get_or_create_page(self.root_page_index);
            /*
            If we are splitting the root, we need to update old_node to point
            to the new root's left child, new_page_num will already point to
            the new root's right child
             */
            old_page_index = BtreePager::get_internal_node_child(parent.cast_const(), 0);
            old_node = self.pager.get_or_create_page(old_page_index);
        } else {
            parent = self
                .pager
                .get_or_create_page(BtreePager::get_parent(old_node.cast_const()));
            new_node = self.pager.get_or_create_page(new_page_index);
            BtreePager::initialize_internal_node(new_node);
        }

        let mut old_num_keys = BtreePager::get_internal_node_num_keys(old_node.cast_const());

        let mut cur_page_num = BtreePager::get_internal_node_right_child(old_node.cast_const());
        let mut cur = self.pager.get_or_create_page(cur_page_num);

        /*
        First put right child into new node and set right child of old node to invalid page number
        */
        self.internal_node_insert(new_page_index, cur_page_num);
        BtreePager::set_parent(cur, new_page_index);
        BtreePager::set_internal_node_right_child(old_node, INVALID_PAGE_NUM);
        /*
        For each key until you get to the middle key, move the key and the child to the new node
        */
        for i in (INTERNAL_NODE_MAX_KEYS / 2 + 1..INTERNAL_NODE_MAX_KEYS - 1).rev() {
            cur_page_num = BtreePager::get_internal_node_child(old_node, i);
            cur = self.pager.get_or_create_page(cur_page_num);

            self.internal_node_insert(new_page_index, cur_page_num);
            BtreePager::set_parent(cur, new_page_index);
            old_num_keys -= 1;
            BtreePager::set_internal_node_num_keys(old_node, old_num_keys);
        }

        /*
          Set child before middle key, which is now the highest key, to be node's right child,
          and decrement number of keys
        */
        BtreePager::set_internal_node_right_child(
            old_node,
            BtreePager::get_internal_node_child(old_node, old_num_keys - 1),
        );

        old_num_keys -= 1;
        BtreePager::set_internal_node_num_keys(old_node, old_num_keys);

        /*
        Determine which of the two nodes after the split should contain the child to be inserted,
        and insert the child
        */
        let max_after_split = self.pager.get_node_biggest_key(old_node, &self.key_type);

        let destination_page_index = if child_max < max_after_split {
            old_page_index
        } else {
            new_page_index
        };

        self.internal_node_insert(destination_page_index, child_page_index);
        BtreePager::set_parent(child, destination_page_index);

        let old_key_cell_index = self.internal_node_find_child(parent, &old_max);
        BtreePager::set_internal_node_cell_key(
            parent,
            old_key_cell_index,
            self.key_size,
            &self.pager.get_node_biggest_key(old_node, &self.key_type),
        );

        if !splitting_root {
            self.internal_node_insert(BtreePager::get_parent(old_node), new_page_index);
            BtreePager::set_parent(new_node, BtreePager::get_parent(old_node));
        }
    }

    pub fn internal_node_insert(&mut self, parent_index: usize, child_index: usize) {
        /*
        +  Add a new child/key pair to parent that corresponds to child
        +  */

        let parent = self.pager.get_or_create_page(parent_index);
        let child = self.pager.get_or_create_page(child_index);
        let child_max_key = self.pager.get_node_biggest_key(child, &self.key_type);

        let parent_const = parent.cast_const();
        // cell that contains the key -> position of the child in the parent cells
        let cell_index = self.internal_node_find_child(parent_const, &child_max_key);

        let original_num_keys = BtreePager::get_internal_node_num_keys(parent_const);

        /*
        An internal node with a right child of INVALID_PAGE_NUM is empty
        */
        if original_num_keys >= INTERNAL_NODE_MAX_KEYS {
            self.internal_node_split_and_insert(parent_index, child_index);
            return;
        }

        let right_child_page_index = BtreePager::get_internal_node_right_child(parent_const);
        /*
        An internal node with a right child of INVALID_PAGE_NUM is empty
        */
        if right_child_page_index == INVALID_PAGE_NUM {
            BtreePager::set_internal_node_right_child(parent, child_index);
            return;
        }

        let right_child = self.pager.get_or_create_page(right_child_page_index);

        /*
        If we are already at the max number of cells for a node, we cannot increment
        before splitting. Incrementing without inserting a new key/child pair
        and immediately calling internal_node_split_and_insert has the effect
        of creating a new key at (max_cells + 1) with an uninitialized value
        */
        BtreePager::set_internal_node_num_keys(parent, original_num_keys + 1);

        let biggest_key = self.pager.get_node_biggest_key(right_child, &self.key_type);
        if child_max_key > biggest_key {
            /* Replace right child */
            BtreePager::set_internal_node_child(parent, original_num_keys, right_child_page_index);
            BtreePager::set_internal_node_cell_key(
                parent,
                original_num_keys,
                self.key_size,
                &biggest_key,
            );
            BtreePager::set_internal_node_right_child(parent, child_index);
        } else {
            /* Make room for the new cell */
            copy(
                BtreePager::get_internal_node_cell(parent, cell_index),
                BtreePager::get_internal_node_cell(parent, cell_index + 1),
                INTERNAL_NODE_CELL_SIZE * (original_num_keys - cell_index),
            );
            BtreePager::set_internal_node_child(parent, cell_index, child_index);
            BtreePager::set_internal_node_cell_key(
                parent,
                cell_index,
                self.key_size,
                &child_max_key,
            );
        }
    }

    pub unsafe fn read_compare_value(
        &self,
        row_ptr: *const u8,
        buf: &mut Vec<u8>,
        condition_expr: &ConditionExpr,
    ) -> bool {
        let field_meta = self
            .table_metadata
            .get_field_metadata(&condition_expr.field)
            .unwrap();

        copy_nonoverlapping(
            row_ptr.add(field_meta.offset),
            buf.as_mut_ptr(),
            field_meta.size,
        );
        buf.set_len(field_meta.size);
        let value = Value::from_ptr(&field_meta.data_def.data_type, buf.as_ptr());
        buf.clear();

        condition_expr
            .operator
            .operate(&value, &condition_expr.value)
    }
}

pub struct SequentialTable {
    pub root_page_index: usize,
    pub cells_num_by_page: usize,
    pub pager: Box<SequentialPager>,
    table_metadata: Rc<TableStructureMetadata>,
}

impl SequentialTable {
    pub(crate) fn new(
        path: &PathBuf,
        table_metadata: Rc<TableStructureMetadata>,
    ) -> Result<SequentialTable, String> {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .open(path) {
            Ok(file) => {
                let pager = SequentialPager::open(file);
                Ok(SequentialTable {
                    root_page_index: 0,
                    cells_num_by_page: (PAGE_SIZE - SEQUENTIAL_NODE_HEADER_SIZE)
                        / table_metadata.row_size,
                    pager: Box::new(pager),
                    table_metadata,
                })
            }
            Err(_) => Err(format!(
                "Can not open index data file of table {}!",
                table_metadata.table_name
            )),
        }
    }

    pub(crate) fn insert_to_end(
        &mut self,
        page_index: usize,
        cell_index: usize,
        row: &RowToInsert,
    ) {
        let ptr = self.get_row_value_mut(page_index, cell_index);
        copy_nonoverlapping(row.raw_data.as_ptr(), ptr, self.table_metadata.row_size);
        self.pager.increment_cells_num(page_index);
    }

    pub unsafe fn read_compare_value(
        &self,
        row_ptr: *const u8,
        buf: &mut Vec<u8>,
        cluster: &ConditionCluster,
    ) -> bool {
        let mut matched: Option<bool> = None;
        for condition in &cluster.conditions {
            let logical_op: LogicalOperator;
            let compare_result = match condition {
                Condition::Cluster(c) => {
                    logical_op = c.logical_operator;
                    self.read_compare_value(row_ptr, buf, c)
                }
                Condition::Expr(expr) => {
                    let field_meta = self
                        .table_metadata
                        .get_field_metadata(&expr.field)
                        .unwrap();

                    copy_nonoverlapping(
                        row_ptr.add(field_meta.offset),
                        buf.as_mut_ptr(),
                        field_meta.size,
                    );
                    buf.set_len(field_meta.size);
                    let value = Value::from_ptr(&field_meta.data_def.data_type, buf.as_ptr());
                    buf.clear();
                    logical_op = expr.logical_operator;
                    expr.operator.operate(&value, &expr.value)
                }
            };

            if matched.is_none(){
                matched = Some(compare_result);
            } else {
                matched = Some(logical_op.operate(matched.unwrap(), compare_result));
            }
        }

        matched.unwrap()
    }
}

impl Table for SequentialTable {
    fn begin(&mut self) -> WriteReadCursor {
        let page_index = self.root_page_index;
        WriteReadCursor::at(self, page_index, 0)
    }

    fn insert(&mut self, row: &RowToInsert) -> Result<(), String> {
        let mut write_to_page = self.pager.get_total_page();
        let mut write_to_cell_index = self.get_num_cells(write_to_page);

        if write_to_cell_index >= self.cells_num_by_page {
            write_to_page += 1;
            write_to_cell_index = 0;
        }

        self.insert_to_end(write_to_page, write_to_cell_index, row);
        Ok(())
    }

    fn find_by_condition_clusters(
        &self,
        condition_clusters: &Vec<ConditionCluster>,
    ) -> Vec<RowBytes> {
        let mut result = vec![];
        let mut last_op = LogicalOperator::OR;
        for cluster in condition_clusters {
            let cluster_result = self.find_by_condition_cluster(cluster);
            if last_op == LogicalOperator::OR{
                result.extend(cluster_result);
                last_op = last_op.combine(cluster.logical_operator);
            } else {
                let mut set: HashSet<RowBytes> = HashSet::from_iter(result.into_iter());
                result = vec![];
                for r in cluster_result {
                    if set.contains(&r) {
                        result.push(r);
                    }
                }
            }
        }
        result
    }

    fn find_by_condition_cluster(&self, cluster: &ConditionCluster) -> Vec<RowBytes> {
        let row_size = self.table_metadata.row_size;
        let mut cursor = ReadCursor::at(self, 0, 0);
        let mut result = Vec::new();
        let mut global_max_field_size: usize = 0;

        for condition in &cluster.conditions {
          global_max_field_size = max(global_max_field_size, condition.get_field_max_size(&self.table_metadata));
        }

        let mut field_buf = Vec::<u8>::with_capacity(global_max_field_size);

        unsafe {
            while !cursor.is_end() {
                let row_ptr = cursor.cursor_value();
                let mut matched = true;
                matched &= self.read_compare_value(row_ptr, &mut field_buf, cluster);

                if matched {
                    result.push(RowBytes::deserialize_row(row_ptr, row_size));
                }

                cursor.cursor_advance();
            }
        }

        result
    }

    fn end(&mut self) -> WriteReadCursor {
        let total_cells = self.get_num_cells(self.pager.get_total_page());
        let total_pages = self.pager.get_total_page();
        WriteReadCursor::at(self, total_pages, total_cells)
    }

    fn is_btree(&self) -> bool {
        false
    }

    fn get_all(&self) -> Vec<RowBytes> {
        let row_size = self.table_metadata.row_size;
        let mut cursor = ReadCursor::at(self, 0, 0);
        let mut result = Vec::new();
        while !cursor.is_end() {
            result.push(RowBytes::deserialize_row(cursor.cursor_value(), row_size));
            cursor.cursor_advance();
        }
        result
    }

    fn get_row_size(&self) -> usize {
        self.table_metadata.row_size
    }

    fn get_num_cells(&self, page_index: usize) -> usize {
        let page = self.pager.get_page(page_index);
        SequentialPager::get_num_cells(page)
    }

    fn get_next_page_index(&self, page_index: usize) -> usize {
        if page_index == self.pager.get_total_page() - 1 {
            0
        } else {
            page_index + 1
        }
    }

    fn get_row_value(&self, page_index: usize, cell_index: usize) -> *const u8 {
        let page = self.pager.get_page(page_index);
        self.pager
            .get_row_value(page, cell_index, self.table_metadata.row_size)
    }

    fn get_row_value_mut(&mut self, page_index: usize, cell_index: usize) -> *mut u8 {
        let page = self.pager.get_or_create_page(page_index);
        self.pager
            .get_row_value_mut(page, cell_index, self.table_metadata.row_size)
    }

    fn flush_to_disk(&mut self) {
        for x in 0..TABLE_MAX_PAGES {
            if !self.pager.flush_page_to_disk(x) {
                break;
            }
        }
    }

    fn print_tree(&self, page_index: usize, cell_index: usize) {
        println!("{}", page_index);
        println!("{}", cell_index);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
