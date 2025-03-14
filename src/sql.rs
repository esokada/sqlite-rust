#![allow(dead_code)]
#![allow(unused_imports)]


use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case},
    character::complete::{alpha1, alphanumeric1, multispace0, multispace1},
    combinator::recognize,
    multi::{many0_count, separated_list0, separated_list1},
    sequence::{delimited, pair},
    IResult, Parser,
};

// CREATE TABLE apples
// (
// 	id integer primary key autoincrement,
// 	name text,
// 	color text
// )

//get table name and columns from CREATE TABLE statement
pub fn create_table(i: &str) -> nom::IResult<&str,(&str,Vec<Vec<&str>>)> {
    // let my_result = (
    let (remaining, (_,_,table_name,_,table_columns)) = (
        tag_no_case("create table"), 
        multispace1,
        alphanumeric1,
        multispace1,
        columns
    ).parse(i).unwrap();

    Ok((remaining,(table_name,table_columns)))
}

// get the comma-separated string inside parens from the SQL query
fn columns(i: &str) -> IResult<&str,Vec<Vec<&str>>> {
    let list_items = separated_list0(space_comma, column_items);
    let mut inside_parens = delimited((tag("("), multispace0), list_items, (multispace0, tag(")")));

    inside_parens.parse(i)
    
}

//get the space comma combination that appears between the columns
fn space_comma(i: &str) -> IResult<&str, &str> {
    delimited(multispace0, tag(","), multispace0).parse(i)
}

//get the individual items (column name, column data type, etc) from the comma-separated string
fn column_items(i: &str) -> IResult<&str,Vec<&str>> {
    separated_list0(multispace1, alphanumeric1).parse(i)
}

#[cfg(test)]
#[test]
fn test_create_table() {
    let input = "CREATE TABLE apples
(
	id integer primary key autoincrement,
	name text,
	color text
)";
    let (remaining, result) = create_table(input).unwrap();
    assert_eq!(remaining, "");
    assert_eq!(result, ("apples",vec![vec!["id", "integer", "primary", "key", "autoincrement"], vec!["name", "text"], vec!["color","text"]]));
}

#[cfg(test)]
#[test]
fn test_columns() {
    let input = "(id integer primary key autoincrement, name text, color text)";
    let (remaining, result) = columns(input).unwrap();
    assert_eq!(remaining, "");
    assert_eq!(result, vec![vec!["id", "integer", "primary", "key", "autoincrement"], vec!["name", "text"], vec!["color","text"]]);
}

#[cfg(test)]
#[test]
fn test_space_comma() {
    let input = ", ";
    let (remaining, result) = space_comma(input).unwrap();
    assert_eq!(remaining, "");
    assert_eq!(result, ",");
}

#[cfg(test)]
#[test]
fn test_column_items() {
    let input = "id integer primary key autoincrement,";
    let (remaining, result) = column_items(input).unwrap();
    assert_eq!(remaining, ",");
    assert_eq!(result, vec!["id", "integer", "primary", "key", "autoincrement"]);
}