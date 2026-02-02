use crate::structures::NodeID;

#[derive(Clone, Debug)]
pub enum EdgeData {
    Street(StreetEdgeData),
}

#[derive(Debug, Clone)]
pub struct StreetEdgeData {
    pub origin: NodeID,
    pub destination: NodeID,
    pub partial: bool,
    pub length: usize,
    pub foot: bool,
    pub bike: bool,
    pub car: bool,
}
