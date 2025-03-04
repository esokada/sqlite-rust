use anyhow::{bail, Result};
// use core::num;
// use std::collections::btree_map::Range;
use std::fs::File;
use std::io::prelude::*;

fn handle_varint() -> usize {
    todo!()
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    // debug args
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
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            let mut b_tree_header = [0;12];
            file.read_exact(&mut b_tree_header).unwrap();
            let num_tables = u16::from_be_bytes([b_tree_header[3],b_tree_header[4]]);

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            eprintln!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
            println!("number of tables: {}", num_tables);
        },
        ".tables" => {
            let mut file = File::open(&args[1])?;   
            //get database header to learn page size
            let mut database_header = [0;100];
            file.read_exact(&mut database_header).unwrap();
            let page_size: usize = u16::from_be_bytes([database_header[16],database_header[17]]).into();

            //get the first page
            // after the 100-byte database header
            let mut page_1 = vec![100;page_size];
            file.read_exact(&mut page_1).unwrap();

            //get number of cells on the page
            //number of cells is located at offset 3 of page header
            //turning this "into()" usize allows it to be used in range below
            let num_cells:usize = u16::from_be_bytes([page_1[3],page_1[4]]).into();

            //get the cell pointer array
            let cpa_start = 8;
            let cpa_end = 8+(2*num_cells);
            //chain these later
            let cell_pointer_span = &page_1[cpa_start..cpa_end];
            //using chunks_exact(2) because these are 2-byte values
            let cell_pointers = cell_pointer_span.chunks_exact(2);
            // get usize from be_bytes
            let cell_pointers_bytes = cell_pointers.map(|i| u16::from_be_bytes([i[0],i[1]]) as usize);
            //collect into array
            let cell_pointer_array: Vec<usize> = cell_pointers_bytes.collect();

            //store table names
            let mut table_names: Vec<String> = vec![];

            //iterate over cell pointer array
            for cell_pointer in cell_pointer_array {
                //cell header
                let mut cell_header = [0u8;3];
                file.seek(std::io::SeekFrom::Start(cell_pointer as u64)).ok();
                file.read_exact(&mut cell_header).ok();
                //don't need to use these yet
                // let record_size = u8::from_be_bytes([cell_header[0]]);              
                // let row_id = u8::from_be_bytes([cell_header[1]]);
                //get size of record header (includes 1 byte for this item)   
                let record_header_size = u8::from_be_bytes([cell_header[2]]);
                let mut record_header = vec![0u8;(record_header_size -1) as usize];
                file.read_exact(&mut record_header).ok();

                // iterate over the items in the record header
                // calling read_exact() repeatedly to advance the cursor to table name
                for (index, item) in record_header.iter().enumerate() {
                    let item_stype = u8::from_be_bytes([*item]);
                    // size = (stype-13)/2
                    let mut item_buffer = vec![0u8;((item_stype -13)/2) as usize];
                    file.read_exact(&mut item_buffer).ok();
                    // collect table name and break here for now
                    if index  == 2 {
                        let item_name = String::from_utf8(item_buffer).unwrap();
                        table_names.push(item_name);
                        break
                    }
                }            
            }
            let table_names_joined = table_names.join(" ");
            println!("{}",table_names_joined);
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
