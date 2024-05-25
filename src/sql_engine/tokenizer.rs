use std::collections::HashSet;

use lazy_static::lazy_static;
use regex::Regex;

#[derive(Debug, PartialEq)]
enum TokenType {
    Keyword,
    Ident,
    Number,
    StringLiteral,
    Operator,
    LogicalOperator,
    COMMA,
    LPAREN,
    RPAREN,
    EOF,
    Skip,
    Mismatch,
}

#[derive(Debug)]
struct Token {
    token_type: TokenType,
    value: String,
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
    static ref TOKEN_REGEX: Regex = {
        let number = r"\b\d+(\.\d*)?\b";
        let ident = r"\b[a-zA-Z_][a-zA-Z0-9_]*\b";
        let string_literal = r"'[^']*'";
        let operators = OPERATORS.iter().map(|&op| regex::escape(op)).collect::<Vec<_>>().join("|");
        let logical_ops = LOGICAL_OPERATORS.iter().map(|&op| regex::escape(op)).collect::<Vec<_>>().join("|");
        let comma = r",";
        let skip = r"[ \t]+";
        let lparen = r"\(";
        let rparen = r"\)";
        let mismatch = r".";
        let eof = r";";

        let regex_str = format!(
            "(?P<NUMBER>{})|(?P<IDENT>{})|(?P<STRING_LITERAL>{})|(?P<OPERATOR>{})|(?P<LOGICAL_OPERATOR>{})|(?P<COMMA>{})|(?P<LPAREN>{})|(?P<RPAREN>{})|(?P<EOF>{})|(?P<SKIP>{})|(?P<MISMATCH>{})",
            number, ident, string_literal, operators, logical_ops, comma, lparen, rparen, eof, skip, mismatch
        );

        Regex::new(&regex_str).unwrap()
    };
}

fn tokenize(sql: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut pos = 0;

    while pos < sql.len() {
        if let Some(mat) = TOKEN_REGEX.find(&sql[pos..]) {
            let token_str = mat.as_str();
            let typ = match TOKEN_REGEX.captures(&sql[pos..]).unwrap() {
                caps if caps.name("NUMBER").is_some() => TokenType::Number,
                caps if caps.name("IDENT").is_some() => {
                    if KEYWORDS.contains(&token_str.to_uppercase().as_str()) {
                        TokenType::Keyword
                    } else {
                        TokenType::Ident
                    }
                }
                caps if caps.name("STRING_LITERAL").is_some() => TokenType::StringLiteral,
                caps if caps.name("OPERATOR").is_some() => TokenType::Operator,
                caps if caps.name("LOGICAL_OPERATOR").is_some() => TokenType::LogicalOperator,
                caps if caps.name("COMMA").is_some() => TokenType::COMMA,
                caps if caps.name("LPAREN").is_some() => TokenType::LPAREN,
                caps if caps.name("RPAREN").is_some() => TokenType::RPAREN,
                caps if caps.name("EOF").is_some() => TokenType::EOF,
                caps if caps.name("SKIP").is_some() => {
                    pos += mat.end();
                    continue;
                }
                _ => TokenType::Mismatch,
            };

            if typ == TokenType::Mismatch {
                panic!("Unexpected character: {}", token_str);
            }

            tokens.push(Token {
                token_type: typ,
                value: token_str.to_string(),
            });

            pos += mat.end();
        } else {
            break;
        }
    }

    tokens
}