pub mod error;
pub mod messages;
pub mod models;
pub mod presentation;
pub mod primitives;
mod utils;
pub mod values;

pub use utils::get_timestamp_fmt;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
