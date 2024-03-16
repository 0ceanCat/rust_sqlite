use crate::core::{Row, Table};

pub struct Cursor<'a> {
    table: &'a mut Table,
    current_row_index: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub(crate) fn table_start(table: &mut Table) -> Cursor {
        Cursor {
            table,
            current_row_index: 0,
            end_of_table: false,
        }
    }

    pub(crate) fn table_end(table: &mut Table) -> Cursor {
        let row_index = table.num_rows;
        Cursor {
            table,
            current_row_index: row_index,
            end_of_table: true,
        }
    }

    pub(crate) fn cursor_value(&mut self) -> *mut u8 {
        let pointer = self.table.row_slot(self.current_row_index);
        pointer
    }

    pub(crate) fn cursor_advance(&mut self) {
        self.current_row_index += 1;

        if self.current_row_index >= self.table.num_rows {
            self.end_of_table = true;
        }
    }

    pub(crate) fn insert_row(&mut self, row: &Row) {
        row.serialize_row(self.cursor_value());
        self.table.num_rows += 1
    }

    pub(crate) fn is_end(&self) -> bool {
        self.end_of_table
    }

    pub(crate) fn is_full(&self) -> bool{
        self.table.is_full()
    }
}