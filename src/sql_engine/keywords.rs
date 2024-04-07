pub static SELECT: &str = "select";
pub static INSERT: &str = "insert";
pub static INTO: &str = "into";
pub static INSERT_INTO: &str = "insert into";
pub static FROM: &str = "from";
pub static WHERE: &str = "where";
pub static ORDER_BY: &str = "order by";
pub static NOT: &str = "not";
pub static VALUES: &str = "values";

static KEY_WORDS: [&str; 9]  = [SELECT, INSERT, INTO, INSERT_INTO, FROM, WHERE, ORDER_BY, NOT, VALUES];

pub fn is_key_words(k: &str) -> bool {
    KEY_WORDS.contains(&k)
}