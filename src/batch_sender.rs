use std::io;

pub trait BatchSender: Clone + Send + 'static {
    fn send_batch(&self, batch: &[u8]) -> io::Result<()>;
}

impl BatchSender for fn(&[u8]) -> io::Result<()> {
    fn send_batch(&self, batch: &[u8]) -> io::Result<()> {
        self(batch)
    }
}
