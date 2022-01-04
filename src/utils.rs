use crate::node::NodeType;

pub(crate) fn convert_type_to_version(n_type: NodeType) -> usize{
    (n_type as usize) << 62
}
