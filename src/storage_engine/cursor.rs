use crate::storage_engine::config::{LEAF_NODE_CELL_SIZE, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS, LEAF_NODE_RIGHT_SPLIT_COUNT};
use crate::storage_engine::core::{Pager, Row, Table};
use crate::utils::utils::copy;

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_index: usize,
    cell_index: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub(crate) fn at(table: &mut Table, page_index: usize, cell_index: usize) -> Cursor {
        let page = table.pager.get_page_or_create(page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);
        Cursor {
            table,
            page_index,
            cell_index,
            end_of_table: num_cells == 0,
        }
    }

    pub(crate) fn table_start(table: &mut Table) -> Cursor {
        //  if key 0 does not exist in the table, this method will return the position of the lowest id (the start of the left-most leaf node)
        table.table_find_by_key(0)
    }

    pub(crate) fn table_end(table: &mut Table) -> Cursor {
        let page_index = table.root_page_index;
        let page = table.pager.get_page_or_create(page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);
        Cursor {
            table,
            page_index,
            cell_index: num_cells,
            end_of_table: true,
        }
    }

    pub(crate) fn cursor_value(&mut self) -> *mut u8 {
        let page = self.table.pager.get_page_or_create(self.page_index);
        Pager::leaf_node_value(page, self.cell_index)
    }

    pub(crate) fn cursor_advance(&mut self) {
        let node = self.table.pager.get_page_or_create(self.page_index);
        self.cell_index += 1;

        if self.cell_index >= Pager::get_leaf_node_cells_num(node) {
            let next_page_index = Pager::get_leaf_node_next_leaf(node);
            if next_page_index == 0 {
                /* This was rightmost leaf */
                self.end_of_table = true;
            } else {
                self.page_index = next_page_index;
                self.cell_index = 0;
            }
        }
    }

    pub(crate) fn insert_row(&mut self, key: usize, row: &Row) {
        let page = self.table.pager.get_page_or_create(self.page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);

        if num_cells >= LEAF_NODE_MAX_CELLS {
            self.split_and_insert(key, row);
        } else {
            self.insert(page, num_cells, key, row);
        }
    }

    fn insert(&mut self, page: *mut u8, num_cells: usize, key: usize, row: &Row) {
        if self.cell_index < num_cells {
            copy(Pager::leaf_node_cell(page, self.cell_index),
                 Pager::leaf_node_cell(page, self.cell_index + 1),
                 LEAF_NODE_CELL_SIZE * (num_cells - self.cell_index))
        }
        Pager::set_leaf_node_cell_key(page, self.cell_index, key);
        Pager::increment_leaf_node_cells_num(page);
        self.table.pager.mark_page_as_updated(self.page_index);
        row.serialize_row(self.cursor_value());
    }

    fn split_and_insert(&mut self, key: usize, row: &Row) {
        /*
         Create a new node and move half the cells over.
         Insert the new value in one of the two nodes.
         Update parent or create a new parent.
       */
        let old_node = self.table.pager.get_page_or_create(self.page_index);
        let old_biggest_key = self.table.pager.get_node_biggest_key(old_node);
        let new_page_index = self.table.pager.get_unused_page_num();
        let new_node = self.table.pager.get_page_or_create(new_page_index);
        Pager::initialize_leaf_node(new_node);

        Pager::set_parent(new_node, Pager::get_parent(old_node));

        Pager::set_leaf_node_next_leaf(new_node, Pager::get_leaf_node_next_leaf(old_node));
        Pager::set_leaf_node_next_leaf(old_node, new_page_index);

        /*
          All existing keys plus new key should be divided
          evenly between old (left) and new (right) nodes.
          Starting from the right, move each key to correct position.
        */
        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            let destination_node;
            if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                // upper halves (right halves) will be stored in the new_node
                destination_node = new_node;
            } else {
                destination_node = old_node;
            }
            // index_within_node will always decrement until it arrives to 0, then destination_node will be switched to old_node
            let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
            let cell_pointer = Pager::leaf_node_cell(destination_node, index_within_node);

            if i == self.cell_index {
                // when this code executes, the value in the cell_pointer was already moved to position i + 1, if cell_pointer is old_node
                // if cell_pointer is new_node, position `index_within_node` is empty
                Pager::set_leaf_node_cell_key(destination_node, index_within_node, key);
                row.serialize_row(Pager::leaf_node_value(destination_node, index_within_node));
            } else if i > self.cell_index {
                // copy a node from old_node tail (position i - 1), to destination_node (index_within_node)
                copy(Pager::leaf_node_cell(old_node, i - 1), cell_pointer, LEAF_NODE_CELL_SIZE);
            } else {
                copy(Pager::leaf_node_cell(old_node, i), cell_pointer, LEAF_NODE_CELL_SIZE);
            }
        }

        Pager::set_leaf_node_cells_num(old_node, LEAF_NODE_LEFT_SPLIT_COUNT);
        Pager::set_leaf_node_cells_num(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT);

        if Pager::is_root_node(old_node) {
            self.table.create_new_root(new_page_index);
        } else {
            let parent_page_index = Pager::get_parent(old_node);
            let new_biggest = self.table.pager.get_node_biggest_key(old_node);
            let parent_page = self.table.pager.get_page_or_create(parent_page_index);

            let old_key_cell_index = self.table.internal_node_find_child(parent_page, old_biggest_key);
            // old_node is split and contains left halves rows (lower halves)
            // so it's necessary to replace old_biggest_key to new_biggest_key
            Pager::set_internal_node_cell_key(parent_page, old_key_cell_index, new_biggest);
            self.table.internal_node_insert(parent_page_index, new_page_index);
        }
    }

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }
}