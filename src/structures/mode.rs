use async_graphql::Enum;
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Enum, Serialize, Deserialize,
)]
pub enum Mode {
    Walk,
    Bike,
    Car,
    WalkTransit,
    BikeTransit,
    BikeToTransit,
    BikeOnTransit,
    CarDropOff,
    CarPickup,
    BikePickup,
}

impl Mode {
    /// Burden 0=foot, 1=bike, 2=car; a heavier mode must strictly beat lighter on some Pareto axis.
    pub fn burden(self) -> u8 {
        match self {
            Mode::Walk | Mode::WalkTransit => 0,
            Mode::Bike
            | Mode::BikeTransit
            | Mode::BikeToTransit
            | Mode::BikeOnTransit
            | Mode::BikePickup => 1,
            Mode::Car | Mode::CarDropOff | Mode::CarPickup => 2,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VehicleState {
    Walked,
    BikeInHand,
    BikeDropped,
    CarParked,
    CarEgress,
    BikeEgress,
}

impl VehicleState {
    /// Must match `Mode::burden`; RAPTOR arrival cutoff is per burden so a heavier state never prunes a lighter one.
    pub fn burden(self) -> u8 {
        match self {
            VehicleState::Walked => 0,
            VehicleState::BikeInHand | VehicleState::BikeDropped | VehicleState::BikeEgress => 1,
            VehicleState::CarParked | VehicleState::CarEgress => 2,
        }
    }
}

pub const ALL_STATES: [VehicleState; 6] = [
    VehicleState::Walked,
    VehicleState::BikeInHand,
    VehicleState::BikeDropped,
    VehicleState::CarParked,
    VehicleState::CarEgress,
    VehicleState::BikeEgress,
];

#[derive(Debug, Clone)]
pub struct ActiveModes {
    modes: Vec<Mode>,
    state_idx: [u8; 6],
    n_states: u8,
}

impl ActiveModes {
    pub fn new(modes: &[Mode]) -> Self {
        let mut deduped: Vec<Mode> = Vec::new();
        for &m in modes {
            if !deduped.contains(&m) {
                deduped.push(m);
            }
        }

        let mut active = [false; 6];
        for &m in &deduped {
            match m {
                Mode::WalkTransit => active[VehicleState::Walked as usize] = true,
                Mode::BikeTransit => {
                    active[VehicleState::BikeInHand as usize] = true;
                    active[VehicleState::BikeDropped as usize] = true;
                }
                Mode::BikeToTransit => active[VehicleState::BikeDropped as usize] = true,
                Mode::BikeOnTransit => active[VehicleState::BikeInHand as usize] = true,
                Mode::CarDropOff => active[VehicleState::CarParked as usize] = true,
                Mode::CarPickup => active[VehicleState::CarEgress as usize] = true,
                Mode::BikePickup => active[VehicleState::BikeEgress as usize] = true,
                Mode::Walk | Mode::Bike | Mode::Car => {}
            }
        }

        let mut state_idx = [u8::MAX; 6];
        let mut n_states = 0u8;
        for (i, &on) in active.iter().enumerate() {
            if on {
                state_idx[i] = n_states;
                n_states += 1;
            }
        }

        ActiveModes {
            modes: deduped,
            state_idx,
            n_states,
        }
    }

    pub fn n_states(&self) -> usize {
        self.n_states as usize
    }

    pub fn state_of(&self, s: VehicleState) -> Option<usize> {
        let idx = self.state_idx[s as usize];
        (idx != u8::MAX).then_some(idx as usize)
    }

    pub fn states(&self) -> impl Iterator<Item = (usize, VehicleState)> + '_ {
        ALL_STATES
            .iter()
            .filter_map(|&s| self.state_of(s).map(|i| (i, s)))
    }

    pub fn state_at(&self, idx: usize) -> VehicleState {
        ALL_STATES
            .iter()
            .copied()
            .find(|&s| self.state_of(s) == Some(idx))
            .expect("compact state index out of range")
    }

    pub fn selected(&self, m: Mode) -> bool {
        self.modes.contains(&m)
    }

    pub fn wants_transit(&self) -> bool {
        self.n_states > 0
    }

    pub fn wants_direct_walk(&self) -> bool {
        self.selected(Mode::Walk)
    }

    pub fn wants_direct_bike(&self) -> bool {
        self.selected(Mode::Bike)
    }

    pub fn wants_direct_car(&self) -> bool {
        self.selected(Mode::Car)
    }

    pub fn uses_vehicle(&self) -> bool {
        [
            VehicleState::BikeInHand,
            VehicleState::BikeDropped,
            VehicleState::CarParked,
            VehicleState::CarEgress,
            VehicleState::BikeEgress,
        ]
        .iter()
        .any(|&s| self.state_of(s).is_some())
    }
}

impl Default for ActiveModes {
    fn default() -> Self {
        Self::new(&[Mode::Walk, Mode::WalkTransit])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_modes_are_walk_and_walk_transit() {
        let am = ActiveModes::default();
        assert!(am.selected(Mode::Walk));
        assert!(am.selected(Mode::WalkTransit));
        assert!(!am.selected(Mode::Bike));
        assert_eq!(am.n_states(), 1);
        assert_eq!(am.state_of(VehicleState::Walked), Some(0));
        assert_eq!(am.state_of(VehicleState::BikeInHand), None);
        assert_eq!(am.state_of(VehicleState::BikeDropped), None);
    }

    #[test]
    fn bike_transit_activates_two_bike_states() {
        let am = ActiveModes::new(&[Mode::BikeTransit]);
        assert_eq!(am.n_states(), 2);
        assert_eq!(am.state_of(VehicleState::Walked), None);
        assert_eq!(am.state_of(VehicleState::BikeInHand), Some(0));
        assert_eq!(am.state_of(VehicleState::BikeDropped), Some(1));
    }

    #[test]
    fn bike_to_transit_activates_only_dropped() {
        let am = ActiveModes::new(&[Mode::BikeToTransit]);
        assert_eq!(am.n_states(), 1);
        assert_eq!(am.state_of(VehicleState::BikeDropped), Some(0));
        assert_eq!(am.state_of(VehicleState::BikeInHand), None);
        assert_eq!(am.state_of(VehicleState::Walked), None);
        assert_eq!(Mode::BikeToTransit.burden(), 1);
    }

    #[test]
    fn bike_on_transit_activates_only_in_hand() {
        let am = ActiveModes::new(&[Mode::BikeOnTransit]);
        assert_eq!(am.n_states(), 1);
        assert_eq!(am.state_of(VehicleState::BikeInHand), Some(0));
        assert_eq!(am.state_of(VehicleState::BikeDropped), None);
    }

    #[test]
    fn all_modes_yield_three_states() {
        let am = ActiveModes::new(&[
            Mode::Walk,
            Mode::Bike,
            Mode::WalkTransit,
            Mode::BikeTransit,
            Mode::BikeOnTransit,
        ]);
        assert_eq!(am.n_states(), 3);
        assert_eq!(am.state_of(VehicleState::Walked), Some(0));
        assert_eq!(am.state_of(VehicleState::BikeInHand), Some(1));
        assert_eq!(am.state_of(VehicleState::BikeDropped), Some(2));
        let states: Vec<_> = am.states().collect();
        assert_eq!(states.len(), 3);
        assert_eq!(am.state_at(1), VehicleState::BikeInHand);
    }

    #[test]
    fn car_modes_activate_expected_states() {
        let car = ActiveModes::new(&[Mode::Car]);
        assert_eq!(car.n_states(), 0);
        assert!(car.wants_direct_car());
        assert!(!car.wants_transit());

        let dropoff = ActiveModes::new(&[Mode::CarDropOff]);
        assert_eq!(dropoff.n_states(), 1);
        assert_eq!(dropoff.state_of(VehicleState::CarParked), Some(0));
        assert_eq!(dropoff.state_of(VehicleState::CarEgress), None);

        let pickup = ActiveModes::new(&[Mode::CarPickup]);
        assert_eq!(pickup.n_states(), 1);
        assert_eq!(pickup.state_of(VehicleState::CarEgress), Some(0));
        assert_eq!(pickup.state_of(VehicleState::CarParked), None);
    }

    #[test]
    fn uses_vehicle_true_for_any_non_walk_state() {
        assert!(!ActiveModes::new(&[Mode::Walk]).uses_vehicle());
        assert!(!ActiveModes::new(&[Mode::WalkTransit]).uses_vehicle());
        assert!(!ActiveModes::new(&[Mode::Walk, Mode::WalkTransit]).uses_vehicle());
        assert!(ActiveModes::new(&[Mode::BikeTransit]).uses_vehicle());
        assert!(ActiveModes::new(&[Mode::BikeToTransit]).uses_vehicle());
        assert!(ActiveModes::new(&[Mode::BikeOnTransit]).uses_vehicle());
        assert!(ActiveModes::new(&[Mode::CarDropOff]).uses_vehicle());
        assert!(ActiveModes::new(&[Mode::CarPickup]).uses_vehicle());
    }

    #[test]
    fn car_modes_have_burden_two() {
        assert_eq!(Mode::Car.burden(), 2);
        assert_eq!(Mode::CarDropOff.burden(), 2);
        assert_eq!(Mode::CarPickup.burden(), 2);
    }

    #[test]
    fn burden_ordering() {
        assert_eq!(Mode::Walk.burden(), 0);
        assert_eq!(Mode::WalkTransit.burden(), 0);
        assert_eq!(Mode::Bike.burden(), 1);
        assert_eq!(Mode::BikeTransit.burden(), 1);
        assert_eq!(Mode::BikeOnTransit.burden(), 1);
    }

    #[test]
    fn wants_transit_and_direct() {
        assert!(ActiveModes::new(&[Mode::WalkTransit]).wants_transit());
        assert!(ActiveModes::new(&[Mode::BikeOnTransit]).wants_transit());
        assert!(!ActiveModes::new(&[Mode::Walk, Mode::Bike]).wants_transit());
        let am = ActiveModes::new(&[Mode::Walk, Mode::Bike]);
        assert!(am.wants_direct_walk());
        assert!(am.wants_direct_bike());
        assert!(!ActiveModes::new(&[Mode::WalkTransit]).wants_direct_walk());
    }

    #[test]
    fn duplicate_modes_are_deduped() {
        let am = ActiveModes::new(&[Mode::Walk, Mode::Walk, Mode::BikeTransit, Mode::BikeTransit]);
        assert_eq!(am.n_states(), 2);
        assert!(am.selected(Mode::Walk));
    }
}
