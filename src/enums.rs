pub(crate) enum MetaCommandResult {
    MetaCommandSuccess,
    MetaCommandUnrecognizedCommand,
    MetaCommandExit,
}

pub(crate) enum PrepareResult {
    PrepareSuccess,
    PrepareUnrecognizedStatement,
}

pub(crate) enum ExecutionResult {
    ExecutionSuccess,
    ExecutionTableFull,
}

pub(crate) enum StatementType {
    StatementInsert,
    StatementSelect,
}