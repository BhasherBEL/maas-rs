use super::Graph;

impl Graph {
    /// Store the railway topology extracted from the OSM PBF.
    /// Called by `prepare_sncb` during the OSM snapshot phase.
    pub fn store_railway_graph(&mut self, nodes: Vec<(f64, f64)>, adj: Vec<Vec<(usize, u32)>>) {
        self.raptor.railway_nodes = nodes;
        self.raptor.railway_adj = adj;
    }

    /// Return a copy of the cached railway data, or `None` if not yet built.
    pub fn get_railway_graph_data(&self) -> Option<(Vec<(f64, f64)>, Vec<Vec<(usize, u32)>>)> {
        if self.raptor.railway_nodes.is_empty() {
            None
        } else {
            Some((self.raptor.railway_nodes.clone(), self.raptor.railway_adj.clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::structures::Graph;

    #[test]
    fn store_and_get_railway_graph_data() {
        let mut g = Graph::new();
        let nodes = vec![(50.0, 4.0), (50.001, 4.0)];
        let adj = vec![vec![(1usize, 111u32)], vec![(0usize, 111u32)]];
        g.store_railway_graph(nodes.clone(), adj.clone());
        let result = g.get_railway_graph_data();
        assert!(result.is_some());
        let (got_nodes, got_adj) = result.unwrap();
        assert_eq!(got_nodes, nodes);
        assert_eq!(got_adj, adj);
    }

    #[test]
    fn get_railway_graph_data_empty() {
        let g = Graph::new();
        assert!(g.get_railway_graph_data().is_none());
    }
}
