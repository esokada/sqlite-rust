#![allow(dead_code)]


use anyhow::{bail, Ok, Result};
use std::env::VarError;
// use core::num;
// use std::collections::btree_map::Range;
use std::fs::File;
use std::io::prelude::*;
use std::vec;

#[derive(Debug)]
struct Database {
    page_size: u16,
    num_pages: u16,
    file: File
}

impl Database {
    fn new(file_name: &str) -> Result<Self> {
        let mut file = File::open(file_name)?;
        let mut header = [0; 100];
        file.read_exact(&mut header).unwrap(); 
        let page_size = u16::from_be_bytes([header[16], header[17]]);
        let mut b_tree_header = [0;12];
        file.read_exact(&mut b_tree_header).unwrap();
        let num_pages = u16::from_be_bytes([b_tree_header[3],b_tree_header[4]]);
        Ok(Self {
            page_size,
            num_pages,
            file
        }) 
    }

    fn read_page(&mut self, page_index:u16) -> Result<Page> {
        //pages are one-indexed and so need to be rolled back by 1
        let page_offset = (page_index-1) * self.page_size;
        //for schema table
        if page_index == 1 {
            self.file.seek(std::io::SeekFrom::Start(page_offset as u64 + 100))?;
        }
        //all other tables
        else {
            self.file.seek(std::io::SeekFrom::Start(page_offset as u64))?;
        }
        //The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages. 
        //TODO: fix this for interior pages later
        // let page_header_size = 8;

        let mut page_header = [0u8;8];
        self.file.read_exact(&mut page_header)?;
        let page_type = u8::from_be_bytes([page_header[0]]);
        let num_cells = u16::from_be_bytes([page_header[3],page_header[4]]);
        // 2 (0x02) means the page is an interior index b-tree page, 5 (0x05): interior table b-tree page, 10 (0x0a): leaf index b-tree page, 13 (0x0d): leaf table b-tree page. 
        match page_type {
            0x0d => {
                //get the cell pointer array
                // let cpa_start = 8;
                let cpa_size = 2*num_cells;
                // let mut page_1 = vec![100;page_size];
                // file.read_exact(&mut page_1).unwrap();
                //chain these later
                let mut cell_pointer_span = vec![0u8;cpa_size as usize];
                self.file.read_exact(&mut cell_pointer_span)?;
                //using chunks_exact(2) because these are 2-byte values
                let cell_pointers = cell_pointer_span.chunks_exact(2);
                // get usize from be_bytes
                let cell_pointers_bytes = cell_pointers.map(|i| u16::from_be_bytes([i[0],i[1]]) as usize);
                //collect into array
                let cell_pointer_array: Vec<usize> = cell_pointers_bytes.collect();
                let mut cells: Vec<TableLeafCell> = Vec::with_capacity(num_cells as usize);
                for cell_pointer in cell_pointer_array {
                    let cell = self.read_table_leaf_cell(page_index as u32, cell_pointer as u16)?;
                    cells.push(cell);
                }
                return Ok(Page::TableLeaf { cells });
            }
            _ => {bail!("Haven't implemented this page type yet")}
        }
    }
    fn read_table_leaf_cell(&mut self, page_index:u32, cell_pointer:u16) -> Result<TableLeafCell> {
        let page_offset = (page_index-1) * self.page_size as u32;
        let offset = page_offset + cell_pointer as u32;
        self.file.seek(std::io::SeekFrom::Start(offset as u64))?;

        //TODO: reorganize this varint reading + cursor adjustment into a method
        //get payload size
        // always pass in 9 bytes
        let mut possible_bytes = [0u8;9];
        self.file.read_exact(&mut possible_bytes)?;
        let (_payload_size, ps_len) = handle_varint(&possible_bytes).unwrap();
        //need to adjust cursor based on length of varint
        let mut new_offset = offset + ps_len as u32;
        self.file.seek(std::io::SeekFrom::Start(new_offset as u64))?;

        //get row id
        self.file.read_exact(&mut possible_bytes)?;
        let (row_id, row_id_len) = handle_varint(&possible_bytes).unwrap();
        new_offset += row_id_len as u32;
        self.file.seek(std::io::SeekFrom::Start(new_offset as u64))?;  

        //get payload header size (varint)
        self.file.read_exact(&mut possible_bytes)?;
        let (payload_header_size, phs_len) = handle_varint(&possible_bytes).unwrap();
        new_offset += phs_len as u32;
        self.file.seek(std::io::SeekFrom::Start(new_offset as u64))?;  

        //collect serial types for the columns
        //payload_header_size - phs_len = len of serial type span
        let serial_types_len = payload_header_size - phs_len as u64;
        let mut serial_types: Vec<u64> = Vec::new();
        let mut serial_type_buffer = [0u8;9];
        //keep track of bytes taken so far
        let mut byte_tally = 0;
        while byte_tally < serial_types_len {
            self.file.read_exact(&mut serial_type_buffer)?;
            let (stype, stype_len) = handle_varint(&serial_type_buffer).unwrap();
            serial_types.push(stype);
            byte_tally += stype_len as u64;
            new_offset += stype_len as u32;
            self.file.seek(std::io::SeekFrom::Start(new_offset as u64))?;
        }
        //collect values of each column
        let mut values: Vec<RecordValue> = Vec::new();
        for stype in serial_types {
            let value = self.read_record_value(stype)?;
            values.push(value);
        }

        Ok(TableLeafCell{row_id, payload: Record {values}})
      

    }

    fn read_record_value(&mut self, serial_type: u64) -> Result<RecordValue> {
        match serial_type {
            //string
            0 => Ok(RecordValue::Null),
            1 => { 
                let mut record_buffer = [0u8;1];
                self.file.read_exact(&mut record_buffer)?;
                let value = u8::from_be_bytes(record_buffer);
                Ok(RecordValue::Int8 { val: value })
            },
            2 => { 
                let mut record_buffer = [0u8;2];
                self.file.read_exact(&mut record_buffer)?;
                let value = u16::from_be_bytes(record_buffer);
                Ok(RecordValue::Int16 { val: value })
            },
            3 => todo!(), //i24
            4 => { 
                let mut record_buffer = [0u8;4];
                self.file.read_exact(&mut record_buffer)?;
                let value = u32::from_be_bytes(record_buffer);
                Ok(RecordValue::Int32 { val: value })
            },
            5 => todo!(), //i48
            6 => { 
                let mut record_buffer = [0u8;8];
                self.file.read_exact(&mut record_buffer)?;
                let value = u64::from_be_bytes(record_buffer);
                Ok(RecordValue::Int64 { val: value })
            },
            7 => { 
                let mut record_buffer = [0u8;8];
                self.file.read_exact(&mut record_buffer)?;
                let value = f64::from_be_bytes(record_buffer);
                Ok(RecordValue::Double { val: value })
            }, //this may be wrong
            8 => Ok(RecordValue::Fake0),
            9 => Ok(RecordValue::Fake1),
            10 | 11 => Ok(RecordValue::Null),
            x if x >= 12 && x % 2 == 0 => {
                let size = (serial_type - 12)/2;
                let mut record_buffer = vec![0u8;size as usize];
                //todo: make sure the cursor is at the right place here
                //or this gets done in the calling function?
                self.file.read_exact(&mut record_buffer)?;
                let mut value = Vec::new();
                for item in record_buffer {
                    value.push(item);
                }
                Ok(RecordValue::Blob { val:value })
            },
            x if x >= 13 && x % 2 == 1 => {
                let size = (serial_type - 13)/2;
                let mut record_buffer = vec![0u8;size as usize];
                //todo: make sure the cursor is at the right place here
                //or this gets done in the calling function?
                self.file.read_exact(&mut record_buffer)?;
                let value = String::from_utf8(record_buffer)?;
                Ok(RecordValue::VarChar { val:value })
            },
            _ => bail!("Invalid serial type")
        }
    }

    fn get_schema_table(&mut self) -> Result<Vec<Schema>> {
        let mut db_tables = Vec::new();
        let db_first_page = self.read_page(1)?;
        match db_first_page {
            Page::TableLeaf {cells} => {
                for cell in cells {
                    db_tables.push(Schema::from_cell(&cell)?);             
            }
        }
        _ => bail!("something wrong with first page")
        }
        Ok(db_tables)
    }
}


fn handle_varint(bytes:&[u8]) -> Result<(u64,usize)> {
    //initialize incrementor to count size of varint
    let mut i: usize = 1;
    //we know we always need the first byte
    //bitwise AND here gets rid of the initial flag bit to store into the value
    let mut val: u64 = (bytes[0] & 0x7f).into();

    //looping through bytes as long as the previous byte value is >= 128 
    // (because if it's greater than 128, the first bit is 1, which means that more bytes are coming)
    // for up to 7 bytes (we've already taken the first one, and we handle the 9th byte later)
    while bytes[i-1] >= 0x80 && i < 8 {
        // left shift the value by 7 because we need to add new bits on the right
        val <<= 7;
        // assign the 7 bits of the current byte
        //bitwise OR assign here to combine the existing content of the value and the new value
        val |= (bytes[i] & 0x7f) as u64;
        i += 1;
    }

    //handle 9th byte
    // if the previous byte is >= 128, which means that more bytes are coming
    if bytes[i-1] >= 0x80 {
        // left shift val by 8 to add all 8 of the new bits to the right
        //(don't need to remove first bit of the 9th byte)
        val <<= 8;
        //add all 8 bits
        val |= bytes[i] as u64;
        i += 1;
    }

    Ok((val, i))

}

struct Schema {
    // schema_type: String,
    name: String,
    tbl_name: String,
    root_page: u32,
    sql: String,
}

impl Schema {
    fn from_cell(cell: &TableLeafCell) -> Result<Self> {
        let values = &cell.payload.values;
        // let schema_type = values[0]
        let name = match values[1] {
            RecordValue::VarChar { ref val } => Ok(val.clone()),
            _ => bail!("something wrong with schema name")
        }?;
        let tbl_name = match values[2] {
            RecordValue::VarChar { ref val } => Ok (val.clone()),
            _ => bail!("something wrong with schema table name")
        }?;
        let root_page = match values[3] {
            RecordValue::Int8 { val } => Ok(val as u32),
            RecordValue::Int16 { val } => Ok(val as u32),
            RecordValue::Int32 { val } => Ok (val as u32),
            RecordValue::Int64 {val} => Ok(val as u32),
            _ => bail!("something wrong with schema root page")
        }?;
        let sql = match values[4] {
            RecordValue::VarChar { ref val } => Ok(val.clone()),
            _ => bail!("something wrong with schema sql")
        }?;
        Ok(Schema {name, tbl_name,root_page, sql})
    }
}

//ignore the other types of pages for now
enum Page {
    TableInterior {},
    TableLeaf {cells: Vec<TableLeafCell>},
    IndexInterior {},
    IndexLeaf{}
}

struct TableLeafCell {
    row_id: u64,
    payload:Record
}

struct Record {
    values: Vec<RecordValue>
}

//add the rest of the value types later
enum RecordValue {
    Null,
    Int8 { val: u8 },
    Int16 { val: u16 },
    Int24 { val: u32 },
    Int32 { val: u32 },
    Int48 { val: u64 },
    Int64 { val: u64 },
    Double { val: f64 },
    Blob {val: Vec<u8>},
    Fake0,
    Fake1,
    Blog {val: Vec<u8>},
    VarChar {val:String}, 

}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    // debug args
    // let args = vec!["".to_string(),"sample.db".to_string(), ".dbinfo".to_string()];
    // let args = vec!["".to_string(),"sample.db".to_string(), ".tables".to_string()];
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            // dbinfo(args);
            let mut database = Database::new(&args[1])?;
            println!("database page size: {}", database.page_size);
            let schema_tables = database.get_schema_table().unwrap();
            println!("number of tables: {}",schema_tables.len());
        },
        ".tables" => {
            // tables(args);
            let mut database = Database::new(&args[1])?;
            let schema_tables = database.get_schema_table().unwrap();
            let mut table_names:Vec<String> = Vec::new();
            for table in schema_tables {
                table_names.push(table.tbl_name);
            }
            println!("{}",table_names.join(" "));
        }
        // _ => bail!("Missing or invalid command passed: {}", command),
        _ => {
            // parse_sql(args);
        }
    }

    Ok(())
}
