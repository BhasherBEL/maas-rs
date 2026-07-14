//! Bike-routing attributes classified once at OSM ingest, stored per directed edge.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HighwayClass {
    Motorway,
    MotorwayLink,
    Trunk,
    TrunkLink,
    Primary,
    PrimaryLink,
    Secondary,
    SecondaryLink,
    Tertiary,
    TertiaryLink,
    Unclassified,
    Residential,
    LivingStreet,
    Service,
    Cycleway,
    Footway,
    Path,
    Track,
    Bridleway,
    Pedestrian,
    Steps,
    Road,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Surface {
    Paved,
    Unpaved,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BikeAttrs {
    pub highway: HighwayClass,
    pub surface: Surface,
    /// tracktype grade 1..=5, or 0 for none/unknown.
    pub tracktype: u8,
    pub isbike: bool,
    pub cycleroute: bool,
    pub bikeaccess: bool,
    pub footaccess: bool,
    /// True when this directed edge goes against a bike-relevant oneway.
    pub wrong_way: bool,
}

impl BikeAttrs {
    /// `probablyGood` from BRouter: paved-or-bike-friendly and not explicitly unpaved.
    pub fn probably_good(&self) -> bool {
        let ispaved = matches!(self.surface, Surface::Paved);
        let isunpaved = matches!(self.surface, Surface::Unpaved);
        (ispaved || self.isbike || matches!(self.highway, HighwayClass::Footway)) && !isunpaved
    }

    pub fn is_residential_or_living(&self) -> bool {
        matches!(
            self.highway,
            HighwayClass::Residential | HighwayClass::LivingStreet
        )
    }

    /// Neutral default (plain road) for non-OSM-built test graphs.
    pub fn road_default() -> Self {
        BikeAttrs {
            highway: HighwayClass::Road,
            surface: Surface::Unknown,
            tracktype: 0,
            isbike: false,
            cycleroute: false,
            bikeaccess: true,
            footaccess: true,
            wrong_way: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probably_good_logic() {
        let mut a = BikeAttrs::road_default();
        a.surface = Surface::Paved;
        assert!(a.probably_good());
        a.surface = Surface::Unpaved;
        assert!(!a.probably_good());
        a.surface = Surface::Unknown;
        a.isbike = true;
        assert!(a.probably_good());
    }

    #[test]
    fn residential_helper() {
        let mut a = BikeAttrs::road_default();
        a.highway = HighwayClass::LivingStreet;
        assert!(a.is_residential_or_living());
        a.highway = HighwayClass::Service;
        assert!(!a.is_residential_or_living());
    }
}
