use std::ops::{Deref, DerefMut};
use crate::storage_engine::tables::{Table};
use crate::storage_engine::common::{RowBytes};

pub struct Cursor {
    pub page_index: usize,
    pub cell_index: usize,
    pub end_of_table: bool,
    pub row_size: usize,
}

impl Cursor {
    fn new(page_index: usize, cell_index: usize, end_of_table: bool, row_size: usize) -> Cursor {
        Cursor {
            page_index,
            cell_index,
            end_of_table,
            row_size,
        }
    }
}

pub struct WriteReadCursor<'a> {
    cursor: Cursor,
    table: &'a mut dyn Table,
}

impl<'a> Deref for WriteReadCursor<'a> {
    type Target = Cursor;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl<'a> DerefMut for WriteReadCursor<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}

impl<'a> WriteReadCursor<'a> {
    pub(crate) fn at(table: &mut dyn Table, page_index: usize, cell_index: usize) -> WriteReadCursor {
        let num_cells = table.get_num_cells(page_index);
        let row_size = table.get_row_size();
        WriteReadCursor {
            cursor: Cursor::new(page_index, cell_index, num_cells == 0, row_size),
            table,
        }
    }

    pub(crate) fn cursor_value(&mut self) -> *mut u8 {
        self.table.get_row_value_mut(self.page_index, self.cell_index)
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

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }
}

pub struct ReadCursor<'a> {
    cursor: Cursor,
    table: &'a dyn Table,
}

impl<'a> Deref for ReadCursor<'a> {
    type Target = Cursor;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl<'a> DerefMut for ReadCursor<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}

impl<'a> ReadCursor<'a> {
    pub(crate) fn at(table: &dyn Table, page_index: usize, cell_index: usize) -> ReadCursor {
        let num_cells = table.get_num_cells(page_index);
        let row_size = table.get_row_size();
        ReadCursor {
            cursor: Cursor::new(page_index, cell_index, num_cells == 0, row_size),
            table,
        }
    }

    pub(crate) fn cursor_value(&self) -> *const u8 {
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

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }
}