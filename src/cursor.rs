use std::ptr;
use crate::config::{LEAF_NODE_CELL_SIZE, LEAF_NODE_LEFT_SPLIT_COUNT, LEAF_NODE_MAX_CELLS, LEAF_NODE_RIGHT_SPLIT_COUNT};
use crate::core::{Pager, Row, Table};

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_index: usize,
    cell_index: usize,
    total_cells: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub(crate) fn at(table: &mut Table, cell_index: usize) -> Cursor {
        let page_index = table.root_page_index;
        let page = table.pager.get_page(page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);
        Cursor {
            table,
            page_index,
            cell_index,
            total_cells: num_cells,
            end_of_table: num_cells == 0,
        }
    }

    pub(crate) fn table_start(table: &mut Table) -> Cursor {
        let page_index = table.root_page_index;
        let page = table.pager.get_page(page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);
        Cursor {
            table,
            page_index,
            cell_index: 0,
            total_cells: num_cells,
            end_of_table: num_cells == 0,
        }
    }

    pub(crate) fn table_end(table: &mut Table) -> Cursor {
        let page_index = table.root_page_index;
        let page = table.pager.get_page(page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);
        Cursor {
            table,
            page_index,
            cell_index: num_cells,
            total_cells: num_cells,
            end_of_table: true,
        }
    }

    pub(crate) fn cursor_value(&mut self) -> *mut u8 {
        let page = self.table.pager.get_page(self.page_index);
        Pager::leaf_node_value(page, self.cell_index)
    }

    pub(crate) fn cursor_advance(&mut self) {
        self.cell_index += 1;

        if self.cell_index >= self.total_cells {
            self.end_of_table = true;
        }
    }

    pub(crate) fn insert_row(&mut self, key: usize, row: &Row) {
        let page = self.table.pager.get_page(self.page_index);
        let num_cells = Pager::get_leaf_node_cells_num(page);

        if num_cells >= LEAF_NODE_MAX_CELLS {
            self.split_and_insert(key, row);
        } else {
            self.insert(page, num_cells, key, row);
        }
    }

    fn insert(&mut self, page:*mut u8, num_cells: usize, key: usize, row: &Row) {
        if self.cell_index < num_cells {
            for i in (self.cell_index + 1..=num_cells).rev() {
                unsafe {
                    ptr::copy_nonoverlapping(Pager::leaf_node_cell(page, i - 1),
                                             Pager::leaf_node_cell(page, i),
                                             LEAF_NODE_CELL_SIZE)
                }
            }
        }
        Pager::set_leaf_node_cell_key(page, self.cell_index, key);
        Pager::increment_leaf_node_cells_num(page);
        self.table.pager.mark_page_as_updated(self.page_index);
        row.serialize_row(self.cursor_value());
    }

    fn split_and_insert(&mut self, key: usize, row: &Row) {
        let old_node = self.table.pager.get_page(self.page_index);
        let new_page_index = self.table.pager.get_unused_page_num();
        let new_node = self.table.pager.get_page(new_page_index);

        for i in (0..=LEAF_NODE_MAX_CELLS).rev() {
            let destination_node;
            if i >= LEAF_NODE_LEFT_SPLIT_COUNT {
                destination_node = new_node;
            } else {
                destination_node = old_node;
            }
            let index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
            let cell_pointer = Pager::leaf_node_cell(destination_node, index_within_node);

            if i == self.cell_index {
                Pager::set_leaf_node_cell_key(destination_node, self.cell_index, key);
                row.serialize_row(cell_pointer);
            } else if (i > self.cell_index) {
                unsafe {
                    ptr::copy_nonoverlapping(Pager::leaf_node_cell(old_node, i - 1), cell_pointer, LEAF_NODE_CELL_SIZE);
                }
            } else {
                unsafe {
                    ptr::copy_nonoverlapping(Pager::leaf_node_cell(old_node, i), cell_pointer, LEAF_NODE_CELL_SIZE);
                }
            }
        }

        Pager::set_leaf_node_cells_num(old_node, LEAF_NODE_LEFT_SPLIT_COUNT);
        Pager::set_leaf_node_cells_num(new_node, LEAF_NODE_RIGHT_SPLIT_COUNT);

        /*if self.is_node_root(old_node) {
            return self.table.create_new_root(new_page_num);
        } else {
            println!("Need to implement updating parent after split\n");
            exit(1);
        }*/
    }

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }

    pub(crate) fn is_full(&self) -> bool {
        false
    }

    /*fn is_node_root(&self, ptr:&Page) -> bool {
        self.table.pager.get_node_type(ptr) == Internal
    }*/
}
