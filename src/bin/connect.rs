use std::error::Error;
use std::net::TcpStream;

use batcher::handle_connection;

fn main() -> Result<(), Box<dyn Error>> {
    let address = "localhost:8100";
//    let address = "localhost:8090";
//    let address = "int01.lw.eu.prod.appmetr.pix.team:8100";
//    let address = "appmetr.com:80";
    println!("Connecting to {}...", address);
    let stream = TcpStream::connect(address)?;
    handle_connection(&stream)?;
    Ok(())
}
