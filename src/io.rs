use std::io;
use anyhow::Error;

pub(crate) fn read_from_input() -> Result<String, Error> {
    let mut input = String::new();
    let mut line = String::new();
    let mut has_read_to_end = false;
    while !has_read_to_end && io::stdin().read_line(&mut line)? > 0 {
        if line.trim().is_empty() {
            has_read_to_end = true;
        } else {
            input.push_str(&line);
            line.clear();
        }
    }
    Ok(input)
}