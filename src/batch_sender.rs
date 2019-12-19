use std::io;

pub trait BatchSender: Clone + Send + 'static {
    fn send_batch(&self, batch: &[u8]) -> io::Result<Option<io::Error>>;
}

impl BatchSender for fn(&[u8]) -> io::Result<Option<io::Error>> {
    fn send_batch(&self, batch: &[u8]) -> io::Result<Option<io::Error>> {
        self(batch)
    }
}
