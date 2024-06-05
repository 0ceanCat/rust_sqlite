use crate::sql_engine::sql_structs::{Condition, ConditionCluster, ConditionExpr, CreateStmt, DataType, FieldDefinition, InsertStmt, LogicalOperator, Operator, Order, OrderByCluster, OrderByExpr, SelectStmt, SqlStmt, Value, WhereExpr};
use crate::sql_engine::tokenizer::*;
use crate::storage_engine::config::FIELD_NAME_SIZE;

#[derive(Clone)]
pub struct SqlParser {}

impl SqlParser {
    pub fn parse_sql(input_stream: String) -> Result<SqlStmt, String> {
        SqlParser {}
            .parse(input_stream)
    }

    fn parse(&mut self, input: String) -> Result<SqlStmt, String> {
        let mut tokenizer = Tokenizer::new(input);
        let first_token = tokenizer.next_token()?;
        if first_token.token_type() != TokenType::Keyword {
            return Err(String::from("Unknown sql statement."));
        }
        if first_token.value() == SELECT {
            let mut select_stmt_parser = SelectStmtParser { tokenizer };
            let select_stmt = select_stmt_parser.parse()?;
            Ok(SqlStmt::SELECT(select_stmt))
        } else if first_token.value() == INSERT {
            let mut insert_stmt_parser = InsertStmtParser { tokenizer };
            let insert_stmt = insert_stmt_parser.parse()?;
            Ok(SqlStmt::INSERT(insert_stmt))
        } else if first_token.value() == CREATE {
            let mut create_stmt_parser = CreateStmtParser { tokenizer };
            let create_stmt = create_stmt_parser.parse()?;
            Ok(SqlStmt::CREATE(create_stmt))
        } else {
            Err(String::from("Unknown sql statement."))
        }
    }
}

struct SelectStmtParser {
    tokenizer: Tokenizer,
}

impl SelectStmtParser {
    fn parse(&mut self) -> Result<SelectStmt, String> {
        let from = self.tokenizer.next_token()?;

        if from.token_type() == TokenType::Keyword && from.value() == FROM {
            return Err(String::from("Syntax error, no selected columns found."));
        }

        let selected_fields = if self.tokenizer.current_token().token_type() == TokenType::AllColumn {
            self.tokenizer.next_token()?; // skip FROM
            vec![String::from("*")]
        } else {
            self.parse_selected_fields()?
        };

        let table = self.tokenizer.next_token()?.value().into();
        self.tokenizer.next_token()?;
        let where_stmt: Option<WhereExpr> =
            if !self.tokenizer.has_more() || self.tokenizer.current_token().value() == ORDER {
                None
            } else {
                Some(
                    WhereStmtParser {
                        tokenizer: &mut self.tokenizer
                    }.parse()?,
                )
            };

        let order_by_stmt: Option<OrderByCluster> = if !self.tokenizer.has_more() {
            None
        } else {
            Some(
                OrderByExprParser {
                    tokenizer: &mut self.tokenizer,
                }.parse()?,
            )
        };

        Ok(SelectStmt::new(
            selected_fields,
            table,
            where_stmt,
            order_by_stmt,
        ))
    }

    fn parse_selected_fields(&mut self) -> Result<Vec<String>, String> {
        let mut fields = Vec::<String>::new();
        while self.tokenizer.has_more() && self.tokenizer.current_token().value() != FROM {
            let field_token = self.tokenizer.current_token();
            let field_name = field_token.value().to_string();

            if fields.contains(&field_name) {
                return Err(format!("Column `{field_name}` has already be selected."));
            }

            fields.push(field_name);

            let next = self.tokenizer.next_token()?;

            if next.token_type() == TokenType::COMMA {
                self.tokenizer.next_token()?; // skip ','
            } else if next.value() != FROM {
                return Err(String::from(
                    "Syntax error, there must be a ',' between two selected fields.",
                ));
            }
        }

        Ok(fields)
    }
}

struct WhereStmtParser<'a> {
    tokenizer: &'a mut Tokenizer,
}

impl<'a> WhereStmtParser<'a> {
    fn parse(&mut self) -> Result<WhereExpr, String> {
        if self.tokenizer.current_token().value() != WHERE {
            return Err(format!(
                "Syntax error, expected a Where statement, but a token `{}` was found.",
                self.tokenizer.current_token().value()
            ));
        }
        self.tokenizer.next_token()?;

        let mut condition_exprs = Vec::<ConditionCluster>::new();
        let mut logical_op = Some(LogicalOperator::AND);

        while self.tokenizer.has_more() {
            let condition_expr = self.parse_condition_cluster(logical_op.unwrap())?;
            condition_exprs.push(condition_expr);
            logical_op = None;

            if self.tokenizer.current_token().value() == ORDER {
                break;
            }

            logical_op = Some(LogicalOperator::try_from(
                self.tokenizer.current_token().value(),
            )?);
        }

        if logical_op.is_some() {
            return Err(String::from(
                "Syntax error, Where statement is not complete.",
            ));
        }

        if condition_exprs.is_empty() {
            return Err(String::from(
                "Syntax error, empty Where statement detected.",
            ));
        }

        Ok(WhereExpr::new(condition_exprs))
    }

    fn parse_condition_cluster(&mut self, cluster_operator: LogicalOperator) -> Result<ConditionCluster, String> {
        let mut conditions = Vec::<Condition>::new();
        let mut more_than_one = false;
        let mut logical_op = LogicalOperator::AND;
        let mut par = false;

        if self.tokenizer.current_token().token_type() == TokenType::Lparen {
            self.tokenizer.next_token()?; //skip '('
            par = true;
        }

        while self.tokenizer.has_more() && self.tokenizer.current_token().value() != ORDER {
            if more_than_one {
                logical_op = LogicalOperator::try_from(self.tokenizer.current_token().value())?;
                self.tokenizer.next_token()?;
            }
            conditions.push(Condition::Expr(self.parse_expr(logical_op)?));

            if par && self.tokenizer.current_token().value() == OR {
                conditions.push(Condition::Cluster(self.parse_condition_cluster(LogicalOperator::OR)?));
            } else if self.tokenizer.current_token().value() == OR  {
                break;
            }

            if self.tokenizer.current_token().token_type() == TokenType::Rparen {
                break;
            }

            more_than_one = true;
        }

        if  self.tokenizer.has_more() && self.tokenizer.current_token().value() != ORDER {
            return Err(String::from("Do you mean ORDER BY?"))
        }

        if par && self.tokenizer.current_token().token_type() != TokenType::Rparen {
            return Err(format!("Syntax error, Where statement is incorrectly formatted, expected a ')' but found {}", self.tokenizer.current_token().value()));
        } else if par {
            self.tokenizer.next_token()?; //skip ')'
        }

        Ok(ConditionCluster::new(cluster_operator, conditions))
    }

    fn parse_expr(
        &mut self,
        logical_operator: LogicalOperator,
    ) -> Result<ConditionExpr, String> {
        let field = self.tokenizer.current_token().value().to_string();
        self.tokenizer.next_token()?;
        let op = {
            OperatorParser {
                tokenizer: &mut self.tokenizer,
            }.parse()?
        };
        self.tokenizer.next_token()?;
        let v = ValueParser {
            tokenizer: &mut self.tokenizer,
        }.parse()?;
        self.tokenizer.next_token()?;
        Ok(ConditionExpr::new(logical_operator, field, op, v))
    }
}

struct InsertStmtParser {
    tokenizer: Tokenizer,
}

impl InsertStmtParser {
    fn parse(&mut self) -> Result<InsertStmt, String> {
        self.tokenizer.next_token()?;
        if self.tokenizer.next_token()?.value() != "INTO" {
            return Err(String::from("Do you mean INERT INTO?"))
        }

        let table_name = self.tokenizer.next_token()?.value().to_string();

        let fields = self.parse_inserted_fields()?;

        let values = self.parse_values()?;

        if self.tokenizer.current_token().token_type() != TokenType::COMMA {
            return Err(format!(
                "Syntax error, `;` expected but `{}` was found.",
                self.tokenizer.current_token().value()
            ));
        }

        if fields.first().unwrap() != "*" && values.len() != fields.len() {
            return Err(String::from(
                "Number of inserted rows and row values are not the same.",
            ));
        }

        Ok(InsertStmt::new(table_name, fields, values))
    }

    fn parse_inserted_fields(&mut self) -> Result<Vec<String>, String> {
        let mut fields = Vec::<String>::new();

        if self.tokenizer.next_token()?.token_type() == TokenType::Lparen {
            self.tokenizer.next_token()?;
            while self.tokenizer.has_more() {
                let field = self.tokenizer.current_token().value();
                fields.push(field.to_string());
                self.tokenizer.next_token()?;
                if self.tokenizer.current_token().token_type() == TokenType::COMMA {
                    self.tokenizer.next_token()?;
                } else if self.tokenizer.current_token().token_type() == TokenType::Rparen {
                    break;
                }
            }
            self.tokenizer.next_token()?;
            if self.tokenizer.current_token().token_type() != TokenType::Rparen {
                return Err(String::from(
                    "Syntax error, inserted fields is not closed, expected a ')'",
                ));
            }
            self.tokenizer.next_token()?;
        } else {
            fields.push(String::from("*"))
        }

        Ok(fields)
    }
    fn parse_values(&mut self) -> Result<Vec<Value>, String> {
        if self.tokenizer.current_token().value() != VALUES {
            return Err(String::from("Syntax error, `values` is missing."));
        }
        self.tokenizer.next_token()?;
        if self.tokenizer.current_token().token_type() == TokenType::Lparen {
            self.tokenizer.next_token()?; // skip '('
            let mut values = Vec::<Value>::new();
            while self.tokenizer.has_more() {
                let value = ValueParser {
                    tokenizer: &mut self.tokenizer,
                }
                    .parse()?;
                values.push(value);
                if self.tokenizer.current_token().token_type() == TokenType::COMMA {
                    self.tokenizer.next_token()?;
                } else if self.tokenizer.current_token().token_type() == TokenType::Rparen {
                    break;
                }
            }
            if self.tokenizer.current_token().token_type() != TokenType::Rparen {
                return Err(String::from(
                    "Syntax error, `values` is not closed, expected a ')'",
                ));
            }
            self.tokenizer.next_token()?;
            return Ok(values);
        } else {
            return Err(String::from("Syntax error, `values` is uncompleted."));
        }
    }
}

struct CreateStmtParser {
    tokenizer: Tokenizer,
}

impl CreateStmtParser {
    fn parse(&mut self) -> Result<CreateStmt, String> {
        if self.tokenizer.next_token()?.value() != TABLE {
            return Err(String::from("Do you mean Create Table?"))
        };
        let table_name = self.tokenizer.next_token()?.value().to_string();
        let field_definitions = self.parse_field_definitions()?;

        Ok(CreateStmt::new(table_name, field_definitions))
    }

    fn parse_field_definitions(&mut self) -> Result<Vec<FieldDefinition>, String> {
        if self.tokenizer.next_token()?.token_type() == TokenType::Lparen {
            let mut field_definitions = Vec::<FieldDefinition>::new();

            while self.tokenizer.has_more() {
                let field = self.tokenizer.next_token()?.value().to_string();

                if field.len() > FIELD_NAME_SIZE {
                    return Err(format!("Field name can not exceed {FIELD_NAME_SIZE}"));
                }

                let data_type = DataTypeParser {
                    tokenizer: &mut self.tokenizer,
                }.parse()?;

                let primary = self.tokenizer.next_token()?.value() == PRIMARY;

                if primary {
                    self.tokenizer.next_token()?;
                }

                field_definitions.push(FieldDefinition::new(field, data_type, primary));

                if self.tokenizer.current_token().token_type() == TokenType::COMMA {
                    self.tokenizer.next_token()?;
                } else if self.tokenizer.current_token().token_type() == TokenType::Rparen {
                    break;
                } else {
                    return Err(String::from(
                        "Syntax error, `,` expected between defined fields.",
                    ));
                }
            }

            if field_definitions.is_empty() {
                return Err(String::from(
                    "Syntax error, Create statement has no defined values",
                ));
            }

            if field_definitions.iter().filter(|d| d.is_primary()).count() > 1 {
                return Err(String::from("Each table can only have ONE primary key."));
            }
            Ok(field_definitions)
        } else {
            Err(String::from("Syntax error, Create statement has no defined values. `(` expected after the table name."))
        }
    }
}

struct OrderByExprParser<'a> {
    tokenizer: &'a mut Tokenizer,
}

impl<'a> OrderByExprParser<'a> {
    pub(crate) fn parse(&mut self) -> Result<OrderByCluster, String> {
        if self.tokenizer.current_token().value() != ORDER && self.tokenizer.next_token()?.value() != BY {
            return Err(format!(
                "Syntax error, expect `order by`, but found {}",
                self.tokenizer.current_token().value()
            ));
        }
        self.tokenizer.next_token()?;

        let mut order_bys = Vec::<OrderByExpr>::new();

        while self.tokenizer.has_more() {
            let field = self.tokenizer.next_token()?.value().to_string();
            let order: Order;
            if !self.tokenizer.has_more() || self.tokenizer.next_token()?.token_type() == TokenType::COMMA {
                order = Order::ASC;
            } else {
                order = Order::try_from(self.tokenizer.current_token().value())?;
                self.tokenizer.next_token()?;
            }
            order_bys.push(OrderByExpr::new(field, order));
        }

        Ok(OrderByCluster {
            order_by_exprs: order_bys,
        })
    }
}

struct ValueParser<'a> {
    tokenizer: &'a mut Tokenizer
}

impl<'a> ValueParser<'a> {
    fn parse(&mut self) -> Result<Value, String> {
        let v = self.tokenizer.current_token().value();
        match self.tokenizer.current_token().token_type() {
            TokenType::LeftBracket => self.parse_array(),
            TokenType::StringLiteral => Ok(Value::TEXT(v.to_string())),
            TokenType::Number => {
                let number_str = v;
                if number_str.contains('.') {
                    return Ok(Value::FLOAT(number_str.parse().unwrap()))
                }
                Ok(Value::INT(number_str.parse().unwrap()))
            },
            TokenType::Boolean => Ok(Value::BOOL(v.to_lowercase() == "true")),
            _ => {
                return Err(format!(
                    "Unknown type of value `{}` detected.",
                    v
                ));
            }
        }
    }

    fn parse_array(&mut self) -> Result<Value, String> {
        let mut array = Vec::<Value>::new();
        self.tokenizer.next_token()?; // skip '['

        while self.tokenizer.has_more() && self.tokenizer.current_token().token_type() != TokenType::RightBracket {
            let value = self.parse()?;
            match value {
                Value::ARRAY(_) => {
                    return Err(String::from(
                        "An array must contain only primitive values. But array detected.",
                    ));
                }
                _ => {}
            }

            if !array.is_empty() && !Value::are_same_variant(&array[0], &value) {
                return Err(String::from(
                    "All element of an array must be the same type.",
                ));
            }

            array.push(value);

            if self.tokenizer.current_token().token_type() != TokenType::COMMA {
                self.tokenizer.next_token()?;
            }
        }

        if !self.tokenizer.has_more() || self.tokenizer.current_token().token_type() != TokenType::RightBracket {
            return Err(String::from(
                "Detected an array value, but it is not closed. ']' is expected.",
            ));
        }

        self.tokenizer.next_token()?;
        Ok(Value::ARRAY(array))
    }
}

struct OperatorParser<'a> {
    tokenizer: &'a mut Tokenizer,
}

impl<'a> OperatorParser<'a> {
    fn parse(&mut self) -> Result<Operator, String> {
        let mut operator = String::new();

        if self.tokenizer.has_more() && self.tokenizer.current_token().value() == NOT {
            operator.push_str("not ");
            self.tokenizer.next_token()?;
        }

        if self.tokenizer.has_more() && self.tokenizer.current_token().value() == IN {
            operator.push_str("in");
            self.tokenizer.next_token()?;
        } else {
            operator.push_str(self.tokenizer.current_token().value());
        }
        Operator::try_from(operator)
    }
}

struct DataTypeParser<'a> {
    tokenizer: &'a mut Tokenizer,
}

impl<'a> DataTypeParser<'a> {
    fn parse(&mut self) -> Result<DataType, String> {
        let data_type = self.tokenizer.next_token()?.value();

        if data_type == "text" {
            let mut size: usize = 255;
            if self.tokenizer.next_token()?.token_type() == TokenType::Lparen {
                size = self.tokenizer.next_token()?.value().parse().unwrap_or(255);
                if self.tokenizer.next_token()?.token_type() == TokenType::Rparen {
                    return Err(String::from("Syntax error, expected a ')'."));
                }
            }

            Ok(DataType::TEXT(size))
        } else {
            match data_type {
                "integer" => Ok(DataType::INTEGER),
                "float" => Ok(DataType::FLOAT),
                "boolean" => Ok(DataType::BOOLEAN),
                _ => Err(format!("Unknown data type `{}` was found.", data_type)),
            }
        }
    }
}