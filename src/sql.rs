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

// ***SELECT***

    // "SELECT COUNT(*) FROM apples"
    // "SELECT name FROM apples"
    // "SELECT name, color FROM apples"

pub fn select(i:&str) -> nom::IResult<&str, (&str, Vec<&str>)> {
    let (remaining, (_, _, selected, _, _, _, table_name)) = (
        tag_no_case("select"), 
        multispace1,
        selection,
        multispace1,
        tag_no_case("from"),
        multispace1,
        alphanumeric1
    ).parse(i).unwrap();

    Ok((remaining,(table_name,selected)))
}

//object of select statement
fn selection(i:&str) -> nom::IResult<&str, Vec<&str>> {
    //get count(*) or list of columns
    //essential that count(*) is first because alt tries parsers in order, 
    //and if alphanumeric1 were first, it would consume "count" and reject "(*)"
    separated_list1(space_comma, alt((tag_no_case("count(*)"), alphanumeric1))).parse(i)
}

#[cfg(test)]
#[test]
fn test_select() -> () {
let input = "SELECT name, color FROM apples";
let (remaining, result) = select(input).unwrap();
assert_eq!(remaining, "");
assert_eq!(result, ("apples", vec!["name", "color"]));
}

#[cfg(test)]
#[test]
fn test_selection() -> () {
let input = "name, color";
let (remaining, result) = selection(input).unwrap();
assert_eq!(remaining, "");
assert_eq!(result, vec!["name", "color"]);
}

// ***CREATE TABLE***

// CREATE TABLE apples
// (
// 	id integer primary key autoincrement,
// 	name text,
// 	color text
// )

//get table name and columns from CREATE TABLE statement
pub fn create_table(i: &str) -> nom::IResult<&str,(&str,Vec<Vec<&str>>)> {
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