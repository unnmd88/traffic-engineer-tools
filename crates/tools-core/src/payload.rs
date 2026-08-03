use std::fmt::{self, Display, Formatter};

#[derive(Debug)]
pub struct Payload<T> {
    pub payload: T,
}

impl<T: Display> Display for Payload<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.payload)
    }
}
