pub static SELECT: &str = "select";
pub static INSERT: &str = "insert";
pub static INTO: &str = "into";
pub static INSERT_INTO: &str = "insert into";
pub static FROM: &str = "from";
pub static WHERE: &str = "where";
pub static ORDER_BY: &str = "order by";
pub static NOT: &str = "not";
pub static VALUES: &str = "values";
pub static CREATE_TABLE: &str = "create table";
pub static PRIMARY: &str = "primary";
pub static TEXT: &str = "text";
pub static INTEGER: &str = "integer";
pub static FLOAT: &str = "float";
pub static BOOLEAN: &str = "boolean";

static KEY_WORDS: [&str; 15]  = [SELECT, INSERT, INTO, INSERT_INTO, FROM, WHERE, ORDER_BY, NOT, VALUES, CREATE_TABLE, PRIMARY, TEXT, INTEGER, FLOAT, BOOLEAN];

pub fn is_key_words(k: &str) -> bool {
    KEY_WORDS.contains(&k)
}