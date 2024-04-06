pub static SELECT: &str = "select";
pub static INSERT: &str = "insert";
pub static FROM: &str = "from";
pub static WHERE: &str = "where";
pub static ORDER_BY: &str = "order by";
pub static NOT: &str = "not";

static KEY_WORDS: [&str; 6]  = [SELECT, INSERT, FROM, WHERE, ORDER_BY, NOT];

pub fn is_key_words(k: &str) -> bool {
    KEY_WORDS.contains(&k)
}