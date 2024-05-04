pub const SELECT: &str = "select";
pub const INSERT: &str = "insert";
pub const INTO: &str = "into";
pub const INSERT_INTO: &str = "insert into";
pub const FROM: &str = "from";
pub const WHERE: &str = "where";
pub const ORDER_BY: &str = "order by";
pub const NOT: &str = "not";
pub const VALUES: &str = "values";
pub const CREATE_TABLE: &str = "create table";
pub const PRIMARY: &str = "primary key";
pub const TEXT: &str = "text";
pub const INTEGER: &str = "integer";
pub const FLOAT: &str = "float";
pub const BOOLEAN: &str = "boolean";

const KEY_WORDS: [&str; 15] = [
    SELECT,
    INSERT,
    INTO,
    INSERT_INTO,
    FROM,
    WHERE,
    ORDER_BY,
    NOT,
    VALUES,
    CREATE_TABLE,
    PRIMARY,
    TEXT,
    INTEGER,
    FLOAT,
    BOOLEAN,
];

pub fn is_key_words(k: &str) -> bool {
    KEY_WORDS.contains(&k)
}
