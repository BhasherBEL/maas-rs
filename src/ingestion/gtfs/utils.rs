use std::collections::HashMap;

pub struct IdMapper<T> {
    to_index: HashMap<String, T>,
    to_string: Vec<String>,
}

impl IdMapper<usize> {
    pub fn new() -> Self {
        Self {
            to_index: HashMap::new(),
            to_string: Vec::new(),
        }
    }

    pub fn get_or_insert(&mut self, gtfs_id: String) -> usize {
        if let Some(&idx) = self.to_index.get(&gtfs_id) {
            return idx;
        }
        let idx = self.to_string.len() as usize;
        self.to_string.push(gtfs_id.clone());
        self.to_index.insert(gtfs_id, idx);
        idx
    }

    pub fn get(&mut self, gtfs_id: String) -> Option<usize> {
        if let Some(&idx) = self.to_index.get(&gtfs_id) {
            return Some(idx);
        }
        None
    }

    pub fn to_gtfs_id(&self, idx: u32) -> &str {
        &self.to_string[idx as usize]
    }
}
