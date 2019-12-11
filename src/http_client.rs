use std::io;
use std::io::{Write, BufReader, Error, ErrorKind, BufRead, Read};
use std::collections::HashMap;

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Status {
    pub code: i32,
    pub message: String
}

#[derive(PartialEq, Eq, Debug)]
pub struct Response {
    pub status: Status,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>
}

pub fn write_request<'c, 'a: 'c, 'b: 'c>(write: &mut impl Write, method: &str, uri: &str,
                                           headers: impl Iterator<Item=&'c (&'a str, &'b str)>, body: &[u8]) -> io::Result<()> {
    let mut head = String::new();
    head.push_str(method);head.push(' ');head.push_str(uri);head.push(' ');head.push_str("HTTP/1.1\r\n");
    for &(h, v) in headers {
        head.push_str(h);head.push(':');head.push(' ');head.push_str(v);head.push('\r');head.push('\n');
    }
    head.push_str("Content-Length: ");head.push_str(&body.len().to_string());head.push_str("\r\n");
    head.push_str("\r\n");

    print!("Writing headers...\n{}", head);
    write.write(head.as_bytes())?;

    println!("Writing body...\n{}", String::from_utf8_lossy(body));
    write.write_all(&body)?;
    write.flush()//?;
//    stream.shutdown(Shutdown::Write)
}

pub fn read_response(read: &mut impl Read) -> io::Result<Response> {
    println!("\nReading..."); 

    let mut reader = BufReader::with_capacity(128, read);

    let status = read_status(&mut reader)?;
    let headers = read_headers(&mut reader, HashMap::new())?;
    let body = match headers.get("content-length") {
        Some(content_length) => {
            read_body(&mut reader, content_length.parse().map_err(|parse_int_error|
                Error::new(ErrorKind::InvalidData, format!("Can't parse body length {} with {}", content_length, parse_int_error)))?)?
        }
        None => match headers.get("transfer-encoding").map(String::as_str) {
            Some("chunked") => read_chunked(&mut reader)?,
            _ => Vec::new()
        }
    };

    Ok(Response {status, headers, body})
}

fn read_status(reader: &mut impl BufRead) -> io::Result<Status> {
    let s = reader.lines().next().transpose()?;
    let s = s.ok_or_else(|| Error::new(ErrorKind::InvalidInput, format!("Missing status line of response")))?;
    let status: Vec<&str> = s.split(' ').collect();

    if status.len() < 3 {
        Err(Error::new(ErrorKind::InvalidData, format!("Invalid status line of response {:?}", status)))
    } else {
        Ok(Status {
            code: status[1].parse().map_err(|parse_int_error|
                Error::new(ErrorKind::InvalidData, format!("Can't parse status {} with {}", status[1], parse_int_error)))?,
            message: status[2..].join(" "),
        })
    }
}

fn read_headers(reader: &mut impl BufRead, mut headers: HashMap<String, String>) -> io::Result<HashMap<String, String>> {
    for line in reader.lines() {
        let header = line?;
        if header.is_empty() {
            return Ok(headers)
        } else {
            let kv: Vec<&str> = header.split(": ").collect();
            if kv.len() != 2 {
                return Err(Error::new(ErrorKind::InvalidData, format!("Wrong header {}", header)))
            }
            headers.insert(kv[0].to_ascii_lowercase(), kv[1].to_string());
        }
    }

    Err(Error::new(ErrorKind::InvalidInput, format!("Can't read headers - empty line not found {:?}", headers)))
}

fn read_body(reader: &mut impl BufRead, length: usize) -> io::Result<Vec<u8>> {
    let mut body = vec![0; length];
    reader.read_exact(&mut body)?;
    Ok(body)
}

fn read_chunked(reader: &mut impl BufRead) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        match reader.lines().next() {
            Some(line) => {
                let chunk_size = line?;
                let chunk_size = usize::from_str_radix(&chunk_size, 16).map_err(|parse_int_error|
                    Error::new(ErrorKind::InvalidData, format!("Can't parse chunk size {} with {}", chunk_size, parse_int_error)))?;
                let mut chunk = vec![0; chunk_size];
                reader.read_exact(&mut chunk)?;
                body.extend_from_slice(&chunk);
                reader.read_line(&mut String::with_capacity(2))?; //skip \r\n
                if chunk_size == 0 {
                    break Ok(body);
                }
            }
            None => return Err(Error::new(ErrorKind::InvalidInput, format!("Wanted chunk length but no more data", )))
        }
    }
}

#[cfg(test)]
mod test {
    use std::io;
    use crate::http_client::*;
    use std::collections::HashMap;
    use std::ops::Index;

    #[test]
    #[should_panic]
    fn status_line_is_wrong() {
        let mut cursor = io::Cursor::new(b"foo\nbar");
        let _status = read_status(&mut cursor).unwrap();
    }

    #[test]
    fn status_200() {
        let mut cursor = io::Cursor::new(b"HTTP/1.1 200 OK\r\n");
        let status = read_status(&mut cursor).unwrap();
        assert_eq!(Status { code: 200, message: "OK".to_string() }, status)
    }

    #[test]
    fn status_404() {
        let mut cursor = io::Cursor::new(b"HTTP/1.1 404 Not Found\r\n");
        let status = read_status(&mut cursor).unwrap();
        assert_eq!(Status { code: 404, message: "Not Found".to_string() }, status)
    }

    #[test]
    fn header_without_colon() -> Result<(), String> {
        let mut cursor = io::Cursor::new(b"Wrong header\r\n");
        match read_headers(&mut cursor, HashMap::new()) {
            Ok(_) => Err("Should fail".to_string()),
            Err(_) => Ok(())
        }
    }

    #[test]
    fn header_without_empty_line() -> Result<(), String> {
        let mut cursor = io::Cursor::new(
r"Content-Type: application/json
Content-Length: 1024
");
        match read_headers(&mut cursor, HashMap::new()) {
            Ok(_) => Err("Should fail".to_string()),
            Err(_) => Ok(())
        }
    }

    #[test]
    fn headers() -> Result<(), String> {
        let mut cursor = io::Cursor::new(
r"Content-Type: application/json
Content-Length: 1024

");
        match read_headers(&mut cursor, HashMap::new()) {
            Ok(headers) => {
                assert_eq!(2, headers.len());
                assert_eq!("application/json", headers.index("content-type"));
                assert_eq!("1024", headers.index("content-length"));
                Ok(())
            },
            Err(e) => Err(e.to_string())
        }
    }

    #[test]
    fn chunked_body() -> Result<(), String> {
        let mut cursor = io::Cursor::new(
"4\r\n\
Wiki\r\n\
5\r\n\
pedia\r\n\
B\r\n\
\x20in\nchunks.\r\n\
0\r\n
\r\n");
        match read_chunked(&mut cursor) {
            Ok(body) => {
                assert_eq!(b"Wikipedia in\nchunks.", &*body);
                Ok(())
            },
            Err(e) => Err(e.to_string())
        }
    }
}
