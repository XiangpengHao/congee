mod node;
mod tree;
mod utils;

pub(crate) type Tid = usize;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
