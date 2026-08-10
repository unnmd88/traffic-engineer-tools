use derive_more::{Constructor, Display};

#[derive(Debug, Clone, Hash, Eq, PartialEq, Display, Constructor, Copy)]
pub struct Uid(pub u64);
