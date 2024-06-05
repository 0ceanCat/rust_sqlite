use std::collections::HashSet;

use lazy_static::lazy_static;
use regex::Regex;

pub const SELECT: &str = "SELECT";
pub const INSERT: &str = "INSERT";
pub const INTO: &str = "INTO";
pub const FROM: &str = "FROM";
pub const WHERE: &str = "WHERE";
pub const ORDER: &str = "ORDER";
pub const BY: &str = "BY";
pub const NOT: &str = "NOT";
pub const VALUES: &str = "VALUES";
pub const CREATE: &str = "CREATE";
pub const TABLE: &str = "TABLE";
pub const PRIMARY: &str = "PRIMARY";
pub const KEY: &str = "KEY";
pub const OR: &str = "OR";
pub const AND: &str = "AND";
pub const IN: &str = "IN";

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum TokenType {
    Keyword,
    AllColumn,
    Ident,
    Boolean,
    Number,
    StringLiteral,
    Operator,
    LogicalOperator,
    COMMA,
    Lparen,
    Rparen,
    LeftBracket,
    RightBracket,
    EOF,
    DataType,
    Skip,
    Mismatch,
}

#[derive(Debug)]
pub struct Token {
    token_type: TokenType,
    value: String,
}

impl Token {
    pub fn token_type(&self) -> TokenType {
        self.token_type
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

lazy_static! {
    static ref KEYWORDS: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("SELECT");
        set.insert("FROM");
        set.insert("WHERE");
        set.insert("INSERT");
        set.insert("INTO");
        set.insert("VALUES");
        set.insert("UPDATE");
        set.insert("SET");
        set.insert("DELETE");
        set.insert("ORDER");
        set.insert("BY");
        set.insert("PRIMARY");
        set.insert("KEY");
        set.insert("CREATE");
        set.insert("TABLE");
        set
    };
    static ref OPERATORS: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("=");
        set.insert(">");
        set.insert("<");
        set.insert(">=");
        set.insert("<=");
        set.insert("<>");
        set.insert("!=");
        set
    };
    static ref LOGICAL_OPERATORS: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("AND");
        set.insert("OR");
        set
    };
    static ref DATA_TYPE: HashSet<&'static str> = {
        let mut set = HashSet::new();
        set.insert("TEXT");
        set.insert("INT");
        set.insert("FLOAT");
        set.insert("BOOL");
        set
    };
    static ref TOKEN_REGEX: Regex = {
        let number = r"\b\d+(\.\d*)?\b";
        let all_column = r"\*";
        let ident = r"\b[a-zA-Z_][a-zA-Z0-9_]*\b";
        let boolean = r"true|false|True|False|TRUE|FALSE";
        let string_literal = r"'[^']*'";
        let operators = OPERATORS.iter().map(|&op| regex::escape(op)).collect::<Vec<_>>().join("|");
        let logical_ops = LOGICAL_OPERATORS.iter().map(|&op| regex::escape(op)).collect::<Vec<_>>().join("|");
        let data_types = DATA_TYPE.iter().map(|&op| regex::escape(op)).collect::<Vec<_>>().join("|");
        let comma = r",";
        let skip = r"[ \t]+";
        let lparen = r"\(";
        let rparen = r"\)";
        let left_bracket = r"\[";
        let right_bracket = r"\]";
        let mismatch = r"\.";
        let eof = r"\;";

        let regex_str = format!(
            "(?P<NUMBER>{})|(?P<ALL_COLUMN>{})|(?P<IDENT>{})|(?P<BOOLEAN>{})|(?P<STRING_LITERAL>{})|(?P<OPERATOR>{})|(?P<LOGICAL_OPERATOR>{})|(?P<DATA_TYPE>{})|(?P<COMMA>{})|(?P<LPAREN>{})|(?P<RPAREN>{})|(?P<LBRACKET>{})|(?P<RBRACKET>{})|(?P<SKIP>{})|(?P<MISMATCH>{})|(?P<EOF>{})",
            number, all_column, ident, boolean, string_literal, operators, logical_ops, data_types, comma, lparen, rparen,left_bracket, right_bracket, skip, mismatch, eof
        );

        Regex::new(&regex_str).unwrap()
    };
}

pub struct Tokenizer {
    current_token: Option<Token>,
    position: usize,
    sql: String,
}

impl Tokenizer {
    pub fn new(sql: String) -> Self {
        Tokenizer {
            current_token: None,
            position: 0,
            sql,
        }
    }
    pub fn next_token(&mut self) -> Result<&Token, String> {
        while self.position < self.sql.len() {
            if let Some(mat) = TOKEN_REGEX.find(&self.sql[self.position..]) {
                let mut token_str = mat.as_str();
                let typ = match TOKEN_REGEX.captures(&self.sql[self.position..]).unwrap() {
                    caps if caps.name("ALL_COLUMN").is_some() => TokenType::AllColumn,
                    caps if caps.name("NUMBER").is_some() => TokenType::Number,
                    caps if caps.name("IDENT").is_some() => {
                        if KEYWORDS.contains(&token_str.to_uppercase().as_str()) {
                            TokenType::Keyword
                        } else {
                            TokenType::Ident
                        }
                    }
                    caps if caps.name("BOOLEAN").is_some() => TokenType::Boolean,
                    caps if caps.name("STRING_LITERAL").is_some() => TokenType::StringLiteral,
                    caps if caps.name("OPERATOR").is_some() => TokenType::Operator,
                    caps if caps.name("LOGICAL_OPERATOR").is_some() => TokenType::LogicalOperator,
                    caps if caps.name("COMMA").is_some() => TokenType::COMMA,
                    caps if caps.name("LPAREN").is_some() => TokenType::Lparen,
                    caps if caps.name("RPAREN").is_some() => TokenType::Rparen,
                    caps if caps.name("LEFT_BRACKET").is_some() => TokenType::LeftBracket,
                    caps if caps.name("RIGHT_BRACKET").is_some() => TokenType::RightBracket,
                    caps if caps.name("DATA_TYPE").is_some() => TokenType::DataType,
                    caps if caps.name("SKIP").is_some() => {
                        self.position += mat.end();
                        continue;
                    }
                    caps if caps.name("EOF").is_some() => TokenType::EOF,
                    _ => TokenType::Mismatch,
                };

                if typ == TokenType::Mismatch {
                    return Err(format!("Unexpected character: {}", token_str));
                }

                self.position += mat.end();

                let token = Token {
                    token_type: typ,
                    value: if typ == TokenType::Keyword { token_str.to_uppercase() } else { token_str.to_string() },
                };
                self.current_token = Some(token);
                return Ok(self.current_token());
            } else {
                break;
            }
        }

        Ok(self.current_token())
    }

    pub fn current_token(&self) -> &Token {
        &self.current_token.as_ref().unwrap()
    }

    pub fn has_more(&self) -> bool {
        self.current_token.as_ref().unwrap().token_type != TokenType::EOF
    }
}