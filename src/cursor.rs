use std::ptr;
use crate::config::{LEAF_NODE_KEY_SIZE, LEAF_NODE_NUM_CELLS_OFFSET, LEAF_NODE_NUM_CELLS_SIZE, TABLE_MAX_PAGES};
use crate::core::{Row, Table};

pub struct Cursor<'a> {
    table: &'a mut Table,
    page_index: usize,
    cell_index: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub(crate) fn table_start(table: &mut Table) -> Cursor {
        let page_index = table.root_page_index;
        let num_cells = table.pager.leaf_node_cells_num(table.root_page_index);
        Cursor {
            table,
            page_index,
            cell_index: 0,
            end_of_table: num_cells == 0
        }
    }

    pub(crate) fn table_end(table: &mut Table) -> Cursor {
        let page_index = table.root_page_index;
        let num_cells = table.pager.leaf_node_cells_num(table.root_page_index);
        Cursor {
            table,
            page_index,
            cell_index: num_cells,
            end_of_table: true,
        }
    }

    pub(crate) fn cursor_value(&mut self) -> *mut u8 {
        self.table.pager.leaf_node_cell(self.page_index, self.cell_index)
    }

    pub(crate) fn cursor_advance(&mut self) {
        self.cell_index += 1;

        let cells_num = self.table.pager.leaf_node_cells_num(self.page_index);
        
        if self.cell_index >= cells_num{
            self.end_of_table = true;
        }
    }

    pub(crate) fn insert_row(&mut self, key: usize, row: &Row) {
        self.table.pager.increment_leaf_node_cells_num(self.page_index);
        self.table.pager.set_leaf_node_cell_key(self.page_index, self.cell_index, key);
        let ptr = self.cursor_value();

        row.serialize_row(ptr);
        self.cursor_advance()
    }

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }

    pub(crate) fn is_full(&self) -> bool{
        false
    }
}