use std::io;

pub trait BatchSender: Clone {
    fn send_batch(&self, batch: &[u8]) -> io::Result<()>;
}

impl BatchSender for fn(&[u8]) -> io::Result<()> {
    fn send_batch(&self, batch: &[u8]) -> io::Result<()> {
        self(batch)
    }
}
