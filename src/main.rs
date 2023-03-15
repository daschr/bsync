mod bsync;

use bsync::BlockFile;
use std::io;
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::{env, process::exit};

const BLOCKSIZE: u64 = 64;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        help(args[0].as_str());
    }

    match args[1].as_str() {
        "-rx" | "-c" => receiver(args[0].as_str(), args[2].as_str()),
        "-tx" | "-s" => transmitter(args[2].as_str()),
        _ => help(args[0].as_str()),
    }

    let mut reader = BlockFile::new(&args[1], 1024, false).expect("could not create reader");

    let mut hashbuf: Vec<u8> = vec![0u8; 64];

    while matches!(reader.next_blockhash(&mut hashbuf), Some(_)) {
        println!("{}", core::str::from_utf8(&hashbuf).unwrap());
    }
}

fn help(name: &str) {
    eprintln!("Usage: {name} [-rx|-tx|-c|s] [file]");
    exit(1);
}

/*
    receiver commands:

    <get_len>
    CmdFormat: <u8> <u64>
    ReturnFormat: <u64>

    <get_blockhashes> <num_hashes>
    <get_block> <blocknumber>
    CmdFormat: <u8> <u64>

    transmitter responses:

    get_blockhashes:
    <32 byte> x num_hashes

    get_block:

    <u64 (blocksize in bytes)> <blockdata>
*/

const GET_BLOCKHASH: u8 = 1u8;
const GET_BLOCK: u8 = 2u8;
const GET_LEN: u8 = 3u8;

fn receiver(bsync_name: &str, file: &str) {
    println!("receiver: {file} ");

    let mut child = if file.contains(":") {
        // Split.remainder is still experimental
        let pos = file.find(":").unwrap();
        let host: &str = &file[0..pos];
        let rest: &str = &file[(pos + 1)..];
        Command::new("ssh")
            .arg(host)
            .arg(bsync_name)
            .arg("-tx")
            .arg(rest)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed to start transmitter")
    } else {
        Command::new(bsync_name)
            .arg("-tx")
            .arg(file)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed to start transmitter")
    };

    let mut stdin = child
        .stdin
        .take()
        .expect("could not connect to stdin of child");
    let mut stdout = child
        .stdout
        .take()
        .expect("could not conenct to stdout of child");

    let mut bsync = BlockFile::new(file, BLOCKSIZE, true).expect("Could not start receiver");
}

fn transmitter(file: &str) {
    let mut bsync = BlockFile::new(file, BLOCKSIZE, false).expect("Could not start transmitter");

    let mut stdin = io::stdin();
    let mut stdout = io::stdout();

    let mut command = [0u8; 9];
    let mut blockhash = [0u8; 32];
    let mut block = [0u8; BLOCKSIZE as usize];

    loop {
        {
            let mut read_bytes = 0;
            while read_bytes != command.len() {
                read_bytes += stdin
                    .read(&mut command[read_bytes..9])
                    .expect("could not read bytes");
            }
        }

        match command[0] {
            GET_BLOCKHASH => {
                let num_blocks = read_be_u64(&command, 1);
                eprintln!("parsed num_blocks: {num_blocks}");

                for _ in 0..num_blocks {
                    if let Some(_) = bsync.next_blockhash(&mut blockhash) {
                        stdout.write(&blockhash).expect("could not write blockhash");
                    }
                }
            }
            GET_BLOCK => {
                let blocknumber = read_be_u64(&command, 1);
                eprintln!("parsed blocknumber: {blocknumber}");

                let blocksize = bsync
                    .get_block(blocknumber, &mut block)
                    .expect("could not read block");
                stdout
                    .write(&u64::to_be_bytes(blocksize as u64))
                    .expect("could not write blocksize");
                stdout
                    .write(&block[0..blocksize])
                    .expect("could not write block data");
            }
            GET_LEN => {
                let length: u64 = bsync.get_len().expect("could not get length of file");
                eprintln!("length: {length}");
                stdout
                    .write(&u64::to_be_bytes(length))
                    .expect("could not write file length");
            }
            _ => {
                eprintln!("invalid command! {command:?}");
                exit(1);
            }
        }
        stdout.flush().unwrap();
    }
}

fn read_be_u64(input: &[u8], splitpos: usize) -> u64 {
    let (_, int_bytes) = input.split_at(splitpos);
    u64::from_be_bytes(int_bytes.try_into().unwrap())
}
