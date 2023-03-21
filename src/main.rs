mod bsync;

use bsync::BlockFile;
use std::io;
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::Instant;
use std::{env, process::exit};

const HASHES_PER_ITERATION: u64 = 1024;
const BLOCKSIZE: u64 = 1024 * 1024;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        help(args[0].as_str());
    }

    match args[1].as_str() {
        "-rx" | "-c" => receiver(args[0].as_str(), args[2].as_str(), args[3].as_str()),
        "-tx" | "-s" => transmitter(args[2].as_str()),
        _ => help(args[0].as_str()),
    }
}

fn help(name: &str) {
    eprintln!("Usage: {name} [-rx|-tx|-c|s] [local_file] [remote_file]");
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
const QUIT: u8 = 4u8;

fn receiver(bsync_name: &str, local_file: &str, remote_file: &str) {
    println!("receiver: {local_file} {remote_file}");

    let mut bsync = BlockFile::new(local_file, BLOCKSIZE, true).expect("Could not start receiver");

    let mut child = if remote_file.contains(":") {
        // Split.remainder is still experimental
        let pos = remote_file.find(":").unwrap();
        let host: &str = &remote_file[0..pos];
        let rest: &str = &remote_file[(pos + 1)..];
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
            .arg(remote_file)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("failed to start transmitter")
    };

    let mut child_stdin = child
        .stdin
        .take()
        .expect("could not connect to stdin of child");
    let mut child_stdout = child
        .stdout
        .take()
        .expect("could not connect to stdout of child");

    let local_len: u64 = bsync.get_len().expect("could not get local length");

    eprintln!("local_len: {local_len}");

    child_stdin
        .write_all(&[GET_LEN, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8])
        .expect("could not write to remote process");
    child_stdin
        .flush()
        .expect("could not flush stdin of remote process");

    let remote_len: u64 = {
        let mut buf = [0u8; 8];
        read_exact(&mut child_stdout, &mut buf[0..8]).expect("could not read remote length");
        u64::from_be_bytes(buf)
    };

    println!("local len {local_len} remote len: {remote_len}");

    if local_len > remote_len {
        bsync
            .set_len(remote_len)
            .expect("could not set length of local file");
    }

    let num_blocks = remote_len / BLOCKSIZE + ((remote_len % BLOCKSIZE != 0) as u64);

    let mut blocks_to_be_read: Vec<u64> = Vec::new();
    let mut block_buf = vec![0u8; 8usize + BLOCKSIZE as usize];

    let mut hashes_per_iteration: u64 = HASHES_PER_ITERATION;

    let mut local_blockhash = [0u8; 32];
    let mut remote_blockhash = [0u8; 32];

    let mut current_block: u64 = 0u64;
    while current_block < num_blocks {
        let ts = Instant::now();
        blocks_to_be_read.clear();

        if num_blocks - current_block < hashes_per_iteration {
            hashes_per_iteration = num_blocks - current_block;
        }

        child_stdin.write_all(&[GET_BLOCKHASH]).unwrap();
        child_stdin
            .write_all(&u64::to_be_bytes(hashes_per_iteration))
            .expect("failed writing get blockhash command");
        child_stdin.flush().unwrap();

        for _ in 0..hashes_per_iteration {
            read_exact(&mut child_stdout, &mut remote_blockhash)
                .expect("failed reading remote blockhash");
            bsync.next_blockhash(&mut local_blockhash);
            if local_blockhash != remote_blockhash {
                blocks_to_be_read.push(current_block);
            }

            current_block += 1;
        }

        for block in &blocks_to_be_read {
            child_stdin.write_all(&[GET_BLOCK]).unwrap();
            child_stdin.write_all(&u64::to_be_bytes(*block)).unwrap();
            child_stdin.flush().unwrap();

            read_exact(&mut child_stdout, &mut block_buf[0..8]).expect("cannot read blocksize");
            let bufsize = {
                let (ib, _) = block_buf.split_at(8);
                u64::from_be_bytes(ib.try_into().unwrap())
            };
            read_exact(&mut child_stdout, &mut block_buf[8..(bufsize + 8) as usize])
                .expect("cannot read block");
            let written = bsync
                .write_block(*block, &block_buf[8..(8 + bufsize) as usize])
                .expect("could not write block") as u64;
            if written != bufsize {
                panic!("written != bufsize: {written} != {bufsize}");
            }
        }
        let elapsed = ts.elapsed();
        println!(
            "{} Blocks/s",
            (hashes_per_iteration as f64 / elapsed.as_secs() as f64) as f64
        );
    }

    eprintln!("quitting child");
    child_stdin
        .write_all(&[QUIT, 0, 0, 0, 0, 0, 0, 0, 0])
        .unwrap();
    child_stdin.flush().unwrap();
    child.wait().ok();
}

fn read_exact<A: Read>(r: &mut A, buf: &mut [u8]) -> std::io::Result<()> {
    let mut read_bytes = 0;
    while read_bytes != buf.len() {
        read_bytes += r.read(&mut buf[read_bytes..])?;
    }
    Ok(())
}

fn transmitter(file: &str) {
    let mut bsync = BlockFile::new(file, BLOCKSIZE, false).expect("Could not start transmitter");

    let mut stdin = io::stdin();
    let mut stdout = io::stdout();

    let mut command = [0u8; 9];
    let mut blockhash = [0u8; 32];
    let mut block = vec![0u8; BLOCKSIZE as usize]; // [0u8; BLOCKSIZE as usize];

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
                        stdout
                            .write_all(&blockhash)
                            .expect("could not write blockhash");
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
                    .write_all(&u64::to_be_bytes(blocksize as u64))
                    .expect("could not write blocksize");
                stdout
                    .write_all(&block[0..blocksize])
                    .expect("could not write block data");
            }
            GET_LEN => {
                let length: u64 = bsync.get_len().expect("could not get length of file");
                eprintln!("length: {length}");
                stdout
                    .write_all(&u64::to_be_bytes(length))
                    .expect("could not write file length");
            }
            QUIT => {
                break;
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
