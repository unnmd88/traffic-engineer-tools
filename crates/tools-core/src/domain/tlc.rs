use derive_more::{AsRef, Constructor, Deref, Display, From, Into};

#[derive(Debug, Clone, From, Into, AsRef, Deref, Constructor)]
pub struct Stage(pub u8);

#[derive(Debug, Clone, From, Into, AsRef, Deref, Constructor)]
pub struct Plan(pub u8);

#[derive(Debug, Clone, From, Into, AsRef, Deref, Constructor)]
pub struct NumDetector(pub u8);

#[derive(Debug, Clone, From, Into, AsRef, Deref, Constructor)]
pub struct DetectorLogic(pub u8);
