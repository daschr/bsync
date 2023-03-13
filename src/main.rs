mod bdiff;

use bdiff::bsyncReader;
use std::{env, process::exit};

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} [file]", args[0]);
        exit(1);
    }

    let mut reader = bsyncReader::new(&args[1], 1024).expect("could not create reader");

    let mut hashbuf: Vec<u8> = vec![0u8; 64];

    while matches!(reader.next_blockhash(&mut hashbuf), Some(_)) {
        println!("{}", core::str::from_utf8(&hashbuf).unwrap());
    }
}
