use crate::storage_engine::tables::{Table};
use crate::storage_engine::common::{RowBytes};

pub struct Cursor<'a> {
    table: &'a mut dyn Table,
    page_index: usize,
    cell_index: usize,
    end_of_table: bool,
    pub row_size: usize
}

impl<'a> Cursor<'a> {
    pub(crate) fn at(table: &mut dyn Table, page_index: usize, cell_index: usize) -> Cursor {
        let num_cells = table.get_num_cells(page_index);
        let row_size = table.get_row_size();
        Cursor {
            table,
            page_index,
            cell_index,
            end_of_table: num_cells == 0,
            row_size
        }
    }

    pub(crate) fn table_start(table: &mut dyn Table) -> Cursor {
        //  if key 0 does not exist in the table, this method will return the position of the lowest id (the start of the left-most leaf node)
        table.begin()
    }

    pub(crate) fn table_end(table: &mut dyn Table) -> Cursor {
        table.end()
    }

    pub(crate) fn cursor_value(&mut self) -> *mut u8 {
        self.table.get_row_value(self.page_index, self.cell_index)
    }

    pub(crate) fn cursor_advance(&mut self) {
        self.cell_index += 1;

        if self.cell_index >= self.table.get_num_cells(self.page_index) {
            let next_page_index = self.table.get_next_page_index(self.page_index);
            if next_page_index == 0 {
                /* This is the last leaf */
                self.end_of_table = true;
            } else {
                self.page_index = next_page_index;
                self.cell_index = 0;
            }
        }
    }

    pub(crate) fn insert_row(&mut self, row: &RowBytes) {
        self.table.insert(self.page_index, self.cell_index, row)
    }

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }
}