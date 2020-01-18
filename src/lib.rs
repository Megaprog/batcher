use std::net::TcpStream;
use std::io;
use miniz_oxide::deflate::{compress_to_vec, CompressionLevel};
use std::io::Read;

mod http_client;
mod waiter;
mod batch_storage;
mod memory_storage;
mod file_storage;
mod batch_sender;
mod chained_error;
mod batch_records;
mod batcher;

pub fn handle_connection(mut stream: &TcpStream) -> io::Result<()> {
    let body = r#"{"serverId":"meta", "batchId":"0", "batch":[{
"action":"trackEvent",
"timestamp":1564650518482,
"event":"rustOk",
"properties":{
    "test":"true",
    "numtest":"12345"
}}]}"#;
    let compressed = compress_to_vec(body.as_bytes(), CompressionLevel::BestCompression as u8);

    http_client::write_request(
        &mut stream,
        "POST",
        "/api?method=server.trackS2S&token=2dd19ae6-3d0e-4004-8cd4-bdb961829040&timestamp=1564059121842",
        [
            ("Host", "appmetr.com"),
            ("Content-Type", "application/octet-stream")
        ].iter(),
        &compressed)?;

    let mut buffer = vec![0; 2048];
    stream.read(&mut buffer)?;
    println!("{}", String::from_utf8(buffer).unwrap());
//    println!("{:?}", String::from_utf8_lossy(&buffer).chars().collect::<Vec<char>>());

//    let response = http_client::read_response(&mut stream)?;
////    stream.shutdown(Shutdown::Both);
//
//    println!("{:?}", response);
//    println!("{}", String::from_utf8_lossy(&response.body));
////    println!("{:?}", response.body.chars().collect::<Vec<char>>());
    Ok(())
}

