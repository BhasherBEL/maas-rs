use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, InputObject, Schema, SimpleObject,
    http::GraphiQLSource,
};
use async_graphql_poem::GraphQL;
use chrono::{Local, NaiveDate, NaiveTime};
use poem::{
    IntoResponse, Response, Result, Route, Server, get, handler, listener::TcpListener, web::Html,
};

use crate::{
    ingestion::realtime::ServiceAlert,
    routing::routing_raptor,
    services::realtime_poller::{self, SharedRealtime},
    services::scheduler::{self, SharedGraph},
    structures::{
        ADDRESS_ATTRIBUTION, AddressIndex, Config, Mode, RealtimeIndex, VehiclePos,
        plan::{CandidateStatus, Plan, PlanCoordinate, PlanLeg},
    },
};

/// Hot-swappable handle to the sibling Belgian address index, mirroring
/// `SharedRealtime`/`SharedGraph` so a future feed reload can swap it atomically.
pub type SharedAddressIndex = Arc<arc_swap::ArcSwap<AddressIndex>>;

/// Opaque wrapper so `VehiclePositionMaxAgeSecs` has a unique `TypeId` in the schema
/// context — prevents collision with any other `u64` data item.
struct VehiclePositionMaxAgeSecs(u64);

// ---------------------------------------------------------------------------
// GTFS catalogue types — used for initial data sync by the Flutter client
// ---------------------------------------------------------------------------

#[derive(SimpleObject)]
struct GtfsStop {
    id: String,
    name: String,
    lat: f64,
    lon: f64,
    mode: String,
}

#[derive(SimpleObject)]
struct StationLine {
    mode: String,
    short_name: String,
    /// Line colour as a 6-character hex string (no leading `#`), or `null`.
    color: Option<String>,
    /// Line text colour as a 6-character hex string (no leading `#`), or `null`.
    text_color: Option<String>,
}

/// A geocoded Belgian address from the BeST-Add feed, returned by `searchAddresses`.
#[derive(SimpleObject)]
struct Address {
    id: String,
    label: String,
    lat: f64,
    lon: f64,
    street: String,
    house_number: String,
    postcode: String,
    municipality: String,
}

#[derive(SimpleObject)]
struct GtfsStation {
    id: String,
    name: String,
    lat: f64,
    lon: f64,
    operators: Vec<String>,
    modes: Vec<String>,
    lines: Vec<StationLine>,
    platform_count: i32,
}

#[derive(SimpleObject)]
struct GtfsRoute {
    id: String,
    short_name: String,
    long_name: String,
    mode: String,
    /// GTFS route colour as a 6-character hex string, or `null` if not defined.
    color: Option<String>,
    /// GTFS route text colour as a 6-character hex string, or `null` if not defined.
    text_color: Option<String>,
}

#[derive(SimpleObject)]
struct GtfsAgency {
    id: String,
    name: String,
    url: String,
    routes: Vec<GtfsRoute>,
}

// ---------------------------------------------------------------------------
// raptorExplain debug types
// ---------------------------------------------------------------------------

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
enum CandidateStatusGql {
    /// Plan survived all filters and is in the final result.
    Kept,
    /// This RAPTOR round produced no arrival improvement.
    NotImproving,
    /// Plan reconstruction returned zero legs for this round.
    ReconstructionEmpty,
    /// Dropped by the extreme-risk filter.
    ExtremeRisk,
    /// Dominated in (departure↑, arrival↓, transfers↓) by another plan.
    ParetoDominated,
}

#[derive(SimpleObject)]
struct PlanCandidateGql {
    /// RAPTOR round (0 = walk-only reach; transfer count = round - 1).
    round: i32,
    /// Departure time of the RAPTOR pass that produced this candidate (seconds since midnight).
    origin_departure: i32,
    /// The reconstructed plan. `null` for NOT_IMPROVING and RECONSTRUCTION_EMPTY.
    plan: Option<Plan>,
    status: CandidateStatusGql,
    /// Index into `candidates` of the dominating plan. Set only when status is PARETO_DOMINATED.
    dominator_index: Option<i32>,
    /// Dominator departs later than this plan (true = this plan had an earlier departure but still lost).
    dominator_departs_later: Option<bool>,
    /// Dominator arrives earlier than this plan (true = this plan has a worse arrival time).
    dominator_arrives_earlier: Option<bool>,
    /// Dominator uses fewer transfers than this plan.
    dominator_fewer_transfers: Option<bool>,
    /// Dominator is more reliable (higher reliability bucket) than this plan.
    dominator_higher_reliability: Option<bool>,
}

#[derive(SimpleObject)]
struct AccessInfoGql {
    walk_radius_secs: i32,
    walk_radius_meters: i32,
    origin_stops_found: i32,
    destination_stops_found: i32,
    /// How many times the walk radius doubled before a result was found.
    access_attempts: i32,
    /// True when transit routing failed and a walk-only plan was returned instead.
    fell_back_to_walk_only: bool,
}

#[derive(SimpleObject)]
struct StopPathLegGql {
    /// `true` = transit leg on a scheduled route, `false` = walk.
    is_transit: bool,
    /// Route short name for transit legs; empty string for walk legs.
    route_label: String,
    /// Waypoints: boarding → intermediate stops → alighting (transit), or just endpoints (walk).
    geometry: Vec<PlanCoordinate>,
}

#[derive(SimpleObject)]
struct StopReachGql {
    stop_idx: i32,
    /// 0 = walk-access reach; k≥1 = reached after k transit legs.
    round: i32,
    arrival_secs: i32,
    lat: f64,
    lon: f64,
    name: String,
    /// Ordered sequence of legs that RAPTOR followed from origin to this stop.
    path: Vec<StopPathLegGql>,
}

// ---------------------------------------------------------------------------
// travelTimeMap (isochrone / one-to-many reachability) types
// ---------------------------------------------------------------------------

/// Per-cell aggregation across a departure window.
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "TravelAggregation")]
enum TravelAggregationGql {
    /// Best (minimum) travel time across the sampled departures ("if you time it
    /// perfectly"). The default; matches a single-departure isochrone.
    Best,
    /// Mean travel time across the sampled departures ("on an average departure").
    /// Departures on which a cell is unreachable within `maxSeconds` count as
    /// `maxSeconds`, so a sometimes-reachable cell reads as slower, not faster.
    Average,
}

impl From<TravelAggregationGql> for crate::structures::TravelAggregation {
    fn from(a: TravelAggregationGql) -> Self {
        match a {
            TravelAggregationGql::Best => crate::structures::TravelAggregation::Best,
            TravelAggregationGql::Average => crate::structures::TravelAggregation::Average,
        }
    }
}

/// One sampled reachability cell: a coordinate and the travel time (seconds) to
/// reach it from the centre at the query departure.
#[derive(SimpleObject)]
struct TravelCell {
    lat: f64,
    lng: f64,
    seconds: i32,
}

/// A travel-time map: sampled reachability cells plus the query echo the client
/// needs to paint a green(0)->red(maxSeconds) heatmap.
#[derive(SimpleObject)]
struct TravelTimeMap {
    /// Sampled cells reachable within `maxSeconds`; cells beyond it are omitted.
    cells: Vec<TravelCell>,
    max_seconds: i32,
    center_lat: f64,
    center_lng: f64,
}

#[derive(SimpleObject)]
struct RaptorExplainResult {
    /// Same plans as the `raptor` query would return for identical parameters.
    plans: Vec<Plan>,
    /// Every candidate considered across all RAPTOR rounds, including filtered ones.
    candidates: Vec<PlanCandidateGql>,
    access: AccessInfoGql,
    /// All transit stops that received a RAPTOR label, tagged with the first
    /// round they were reached.  Round 0 = access walk reach.
    stops_reached: Vec<StopReachGql>,
    /// Snapped origin coordinates (the OSM node the router actually used).
    origin: PlanCoordinate,
    /// Snapped destination coordinates.
    destination: PlanCoordinate,
}

fn map_candidate(c: crate::structures::plan::PlanCandidate) -> PlanCandidateGql {
    let (
        status,
        dominator_index,
        dom_departs_later,
        dom_arrives_earlier,
        dom_fewer_transfers,
        dom_higher_reliability,
    ) = match &c.status {
        CandidateStatus::Kept => (CandidateStatusGql::Kept, None, None, None, None, None),
        CandidateStatus::NotImproving => (
            CandidateStatusGql::NotImproving,
            None,
            None,
            None,
            None,
            None,
        ),
        CandidateStatus::ReconstructionEmpty => (
            CandidateStatusGql::ReconstructionEmpty,
            None,
            None,
            None,
            None,
            None,
        ),
        CandidateStatus::ExtremeRisk => (
            CandidateStatusGql::ExtremeRisk,
            None,
            None,
            None,
            None,
            None,
        ),
        CandidateStatus::ParetoDominated {
            dominator_index,
            departure_worse,
            arrival_worse,
            transfers_worse,
            reliability_worse,
        } => (
            CandidateStatusGql::ParetoDominated,
            Some(*dominator_index as i32),
            Some(*departure_worse),
            Some(*arrival_worse),
            Some(*transfers_worse),
            Some(*reliability_worse),
        ),
    };
    PlanCandidateGql {
        round: c.round as i32,
        origin_departure: c.origin_departure as i32,
        plan: c.plan,
        status,
        dominator_index,
        dominator_departs_later: dom_departs_later,
        dominator_arrives_earlier: dom_arrives_earlier,
        dominator_fewer_transfers: dom_fewer_transfers,
        dominator_higher_reliability: dom_higher_reliability,
    }
}

fn parse_date_time(
    date: &Option<String>,
    time: &Option<String>,
) -> std::result::Result<(NaiveDate, NaiveTime), Error> {
    let now = Local::now().naive_local();

    let parsed_date = match date {
        Some(d) => NaiveDate::parse_from_str(d, "%Y-%m-%d")
            .map_err(|e| Error::new(format!("Invalid date '{}': {}", d, e)))?,
        None => now.date(),
    };

    let parsed_time = match time {
        Some(t) => NaiveTime::parse_from_str(t, "%H:%M:%S")
            .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M"))
            .map_err(|e| Error::new(format!("Invalid time '{}': {}", t, e)))?,
        None => now.time(),
    };

    Ok((parsed_date, parsed_time))
}

/// One earlier/later departure alternative for a single transit leg.
#[derive(SimpleObject)]
struct AltDeparture {
    /// Boarding time of the alternative (seconds since midnight).
    start: i32,
    /// Alighting time of the alternative (seconds since midnight).
    end: i32,
    /// Outbound swap reliability: chance the rest of the journey still works if you
    /// take this departure instead, everything else fixed. `null` when unscored.
    reliability: Option<f64>,
}

/// Earlier and later alternatives for one transit leg, computed on demand.
#[derive(SimpleObject)]
struct LegAlternatives {
    previous: Vec<AltDeparture>,
    next: Vec<AltDeparture>,
}

// ---------------------------------------------------------------------------
// liveRefresh: stateless realtime overlay for a client-selected journey
// ---------------------------------------------------------------------------

/// One transit leg of the user's selected journey, identified by stable GTFS
/// handles (not plan/leg indices) so the lookup is stable across polls.
#[derive(InputObject)]
struct LiveLegInput {
    /// GTFS `trip_id` of the boarded vehicle.
    trip_id: String,
    /// GTFS `stop_id` where the user boards.
    board_stop_id: String,
    /// GTFS `stop_id` where the user alights.
    alight_stop_id: String,
}

/// A position aboard a transit trip, used to re-plan from the user's CURRENT
/// onboard location. Supplied to `onboardRaptor` as the origin; the destination
/// stays `toLat`/`toLng`.
#[derive(async_graphql::InputObject)]
struct OnboardOriginInput {
    /// GTFS `trip_id` of the boarded vehicle.
    trip_id: String,
    /// Optional GTFS `stop_id` of the last stop passed (advisory).
    from_stop_id: Option<String>,
    /// Optional pattern position of the last stop passed (advisory).
    from_stop_seq: Option<i32>,
}

/// Realtime status of one selected leg. `NotFound` means a handle did not
/// resolve against the live graph (unknown trip/stop, or the trip does not serve
/// the two stops in order).
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
enum LiveStatusGql {
    /// No realtime information for this trip at the boarding stop.
    NoData,
    /// Reported exactly on schedule at the boarding stop.
    OnTime,
    /// Reported off schedule (see `delaySecs`).
    Delayed,
    /// Reported CANCELED — the trip will not run.
    Canceled,
    /// A handle did not resolve; times are `null`.
    NotFound,
}

/// A service alert that applies to a transit leg at the time of the query.
/// `cause` and `effect` are human-readable strings derived from the GTFS-RT
/// protobuf enum values; `null` when the feed omitted the field.
#[derive(SimpleObject)]
struct LiveAlertGql {
    header: Option<String>,
    description: Option<String>,
    cause: Option<String>,
    effect: Option<String>,
}

/// Convert a GTFS-RT `Cause` i32 to a human-readable string.
fn cause_label(v: i32) -> &'static str {
    match v {
        1 => "UNKNOWN_CAUSE",
        2 => "OTHER_CAUSE",
        3 => "TECHNICAL_PROBLEM",
        4 => "STRIKE",
        5 => "DEMONSTRATION",
        6 => "ACCIDENT",
        7 => "HOLIDAY",
        8 => "WEATHER",
        9 => "MAINTENANCE",
        10 => "CONSTRUCTION",
        11 => "POLICE_ACTIVITY",
        12 => "MEDICAL_EMERGENCY",
        13 => "SPECIAL_EVENT",
        _ => "UNKNOWN_CAUSE",
    }
}

/// Convert a GTFS-RT `Effect` i32 to a human-readable string.
fn effect_label(v: i32) -> &'static str {
    match v {
        1 => "NO_SERVICE",
        2 => "REDUCED_SERVICE",
        3 => "SIGNIFICANT_DELAYS",
        4 => "DETOUR",
        5 => "ADDITIONAL_SERVICE",
        6 => "MODIFIED_SERVICE",
        7 => "OTHER_EFFECT",
        8 => "UNKNOWN_EFFECT",
        9 => "STOP_MOVED",
        10 => "NO_EFFECT",
        11 => "ACCESSIBILITY_ISSUE",
        _ => "UNKNOWN_EFFECT",
    }
}

fn map_alert(alert: &ServiceAlert) -> LiveAlertGql {
    LiveAlertGql {
        header: alert.header.clone(),
        description: alert.description.clone(),
        cause: alert.cause.map(|c| cause_label(c).to_string()),
        effect: alert.effect.map(|e| effect_label(e).to_string()),
    }
}

/// A realtime platform reassignment: the RT feed reports a different platform
/// from the one in the static schedule. `from` is the scheduled platform code,
/// `to` is the actual platform code as reported by the realtime feed.
#[derive(SimpleObject)]
struct PlatformChangeGql {
    from: String,
    to: String,
}

/// Resolved position of the vehicle operating a transit leg, or `null` when the
/// realtime index holds no position for that trip.
#[derive(SimpleObject)]
struct LiveVehicleGql {
    lat: f64,
    lng: f64,
    /// Bearing in degrees clockwise from north, or `null` when not reported.
    bearing: Option<f64>,
    /// Unix epoch seconds of the observation as reported by the realtime feed.
    /// 0 when the feed provided no per-record timestamp.
    observed_at: i64,
    /// True when `(now − observed_at) > vehicle_position_max_age_secs`.
    stale: bool,
}

#[derive(SimpleObject)]
struct LiveLegGql {
    /// Echo of the input `trip_id`.
    trip_id: String,
    /// False when a handle did not resolve; all time fields are then `null`.
    found: bool,
    status: LiveStatusGql,
    /// Realtime delay at the boarding stop (seconds, positive = late). 0 when no
    /// data, canceled, or not found.
    delay_secs: i32,
    /// Scheduled boarding time (seconds since midnight). `null` when not found.
    scheduled_start: Option<i32>,
    /// Scheduled alighting time. `null` when not found.
    scheduled_end: Option<i32>,
    /// Realtime boarding time = `scheduled_start + delay(trip, board)`. `null` when not found.
    realtime_start: Option<i32>,
    /// Realtime alighting time = `scheduled_end + delay(trip, alight)`. `null` when not found.
    realtime_end: Option<i32>,
    /// Latest known vehicle position for this trip, or `null` when not available.
    vehicle: Option<LiveVehicleGql>,
    /// Service alerts currently active that are relevant to this leg (matching
    /// trip or board/alight stop). Empty when none apply or the index holds no
    /// alerts.
    alerts: Vec<LiveAlertGql>,
    /// Realtime platform change at the boarding stop, or `null` when the RT feed
    /// confirms the scheduled platform (or provides no platform info).
    platform_change_board: Option<PlatformChangeGql>,
    /// Realtime platform change at the alighting stop, or `null` when confirmed
    /// or unknown.
    platform_change_alight: Option<PlatformChangeGql>,
}

#[derive(SimpleObject)]
struct LiveTransferGql {
    /// Index (into `legs`) of the arriving (feeder) leg; the user boards `legs[fromLegIndex + 1]`.
    from_leg_index: i32,
    /// Realtime arrival of the feeder leg at the transfer stop (seconds since midnight).
    realtime_arrival: i32,
    /// Realtime departure of the boarded leg.
    realtime_departure: i32,
    /// `realtime_departure − realtime_arrival`. Negative = the connection is broken.
    margin_secs: i32,
    /// Probability of making the connection given the current realtime margin and the
    /// feeder/boarding delay models. `null` when a leg is unresolved or the feeder
    /// route type has no delay model.
    reliability: Option<f64>,
}

/// Realtime overlay of a client-selected journey. Computed purely by indexing the
/// static schedule and the live `RealtimeIndex` by GTFS handles — no routing,
/// no RAPTOR, no plan re-optimisation — so it is stable across polls.
#[derive(SimpleObject)]
struct LivePlanGql {
    legs: Vec<LiveLegGql>,
    /// One entry per interior transfer where both adjacent legs resolved.
    transfers: Vec<LiveTransferGql>,
    /// Realtime arrival at the final alighting stop (seconds since midnight).
    /// `null` when the last leg did not resolve.
    eta: Option<i32>,
    /// Scheduled arrival at the final alighting stop. `null` when unresolved.
    scheduled_eta: Option<i32>,
    /// Unix seconds the realtime snapshot was generated (0 for the inert index).
    generated_at: i64,
}

/// One same-station backup departure: another trip leaving the same boarding
/// stop and reaching the same alighting stop as the user's selected leg, possibly
/// on a different route. Resolved purely by GTFS handle + the live `RealtimeIndex`,
/// so it is stable across polls. Times are seconds since midnight.
#[derive(SimpleObject)]
struct StationBackupGql {
    /// GTFS `trip_id` of the backup (a stable handle the client can act on).
    trip_id: String,
    /// Echo of the boarding `stop_id` (same station as the selected leg).
    board_stop_id: String,
    /// Echo of the alighting `stop_id` (same downstream place).
    alight_stop_id: String,
    /// Route short name (line label), if known.
    route_short_name: Option<String>,
    /// Route long name, if known.
    route_long_name: Option<String>,
    /// Route mode string (e.g. `"Bus"`, `"Tram"`), if known.
    mode: Option<String>,
    /// GTFS route colour as a 6-character hex string, or `null`.
    route_color: Option<String>,
    /// True when this backup runs on the SAME route as the selected leg.
    same_line: bool,
    /// Scheduled boarding time (seconds since midnight).
    scheduled_departure: i32,
    /// Scheduled alighting time.
    scheduled_arrival: i32,
    /// Realtime boarding time = scheduled + delay(trip, board). Equals scheduled
    /// when there is no live data.
    realtime_departure: i32,
    /// Realtime alighting time = scheduled + delay(trip, alight).
    realtime_arrival: i32,
    /// Catch reliability of THIS backup given you are ready on the platform at
    /// your original (realtime) departure time — the probability its own delay
    /// distribution still lets it depart no earlier than that ready time. This is
    /// per-backup catch probability, NOT whole-journey reliability. `null` when
    /// the backup's route type has no delay model.
    reliability: Option<f64>,
}

/// Per-highway cost-factor overrides; every field optional (sparse merge).
#[derive(InputObject, Default)]
struct HighwayFactorsInput {
    trunk: Option<f64>,
    trunk_bike: Option<f64>,
    primary: Option<f64>,
    primary_bike: Option<f64>,
    secondary: Option<f64>,
    secondary_bike: Option<f64>,
    tertiary: Option<f64>,
    tertiary_bike: Option<f64>,
    unclassified: Option<f64>,
    unclassified_bike: Option<f64>,
    residential_paved: Option<f64>,
    residential_unpaved: Option<f64>,
    service_paved: Option<f64>,
    service_unpaved: Option<f64>,
    cycleway: Option<f64>,
    pedestrian: Option<f64>,
    bridleway: Option<f64>,
    other: Option<f64>,
}

impl HighwayFactorsInput {
    fn merge_into(
        self,
        mut b: crate::structures::HighwayFactors,
    ) -> crate::structures::HighwayFactors {
        if let Some(v) = self.trunk {
            b.trunk = v;
        }
        if let Some(v) = self.trunk_bike {
            b.trunk_bike = v;
        }
        if let Some(v) = self.primary {
            b.primary = v;
        }
        if let Some(v) = self.primary_bike {
            b.primary_bike = v;
        }
        if let Some(v) = self.secondary {
            b.secondary = v;
        }
        if let Some(v) = self.secondary_bike {
            b.secondary_bike = v;
        }
        if let Some(v) = self.tertiary {
            b.tertiary = v;
        }
        if let Some(v) = self.tertiary_bike {
            b.tertiary_bike = v;
        }
        if let Some(v) = self.unclassified {
            b.unclassified = v;
        }
        if let Some(v) = self.unclassified_bike {
            b.unclassified_bike = v;
        }
        if let Some(v) = self.residential_paved {
            b.residential_paved = v;
        }
        if let Some(v) = self.residential_unpaved {
            b.residential_unpaved = v;
        }
        if let Some(v) = self.service_paved {
            b.service_paved = v;
        }
        if let Some(v) = self.service_unpaved {
            b.service_unpaved = v;
        }
        if let Some(v) = self.cycleway {
            b.cycleway = v;
        }
        if let Some(v) = self.pedestrian {
            b.pedestrian = v;
        }
        if let Some(v) = self.bridleway {
            b.bridleway = v;
        }
        if let Some(v) = self.other {
            b.other = v;
        }
        b
    }
}

/// Passenger category (GraphQL enum). `YOUNG`/`SENIOR`/`BIM` are the "reduced"
/// categories driving TEC reduced pricing and SNCB reductions/caps.
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq, Default)]
enum PassengerCategoryInput {
    #[default]
    Adult,
    Young,
    Senior,
    Bim,
}

impl PassengerCategoryInput {
    fn to_cost(self) -> crate::structures::cost::PassengerCategory {
        use crate::structures::cost::PassengerCategory as C;
        match self {
            PassengerCategoryInput::Adult => C::Adult,
            PassengerCategoryInput::Young => C::Young,
            PassengerCategoryInput::Senior => C::Senior,
            PassengerCategoryInput::Bim => C::Bim,
        }
    }
}

/// SNCB travel class (default `Second`). Affects ONLY SNCB fares.
#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq, Default)]
enum TravelClassInput {
    #[default]
    Second,
    First,
}

impl TravelClassInput {
    fn to_cost(self) -> crate::structures::cost::TravelClass {
        use crate::structures::cost::TravelClass as T;
        match self {
            TravelClassInput::Second => T::Second,
            TravelClassInput::First => T::First,
        }
    }
}

/// Pre-committed user fare profile: passenger category plus held products. FIXED
/// per query, so every boarding's marginal fare is deterministic. An absent
/// profile means "full single-ticket adult price, no products". All fields
/// optional; camelCase in GraphQL (spec Appendix A.1).
#[derive(InputObject, Default)]
struct FareProfileInput {
    /// `ADULT` | `YOUNG` | `SENIOR` | `BIM` (default `ADULT`).
    passenger_category: Option<PassengerCategoryInput>,
    /// A subscription makes that operator's legs free (0).
    stib_subscription: Option<bool>,
    delijn_subscription: Option<bool>,
    tec_subscription: Option<bool>,
    sncb_subscription: Option<bool>,
    /// SNCB Train+ advantage card.
    sncb_train_plus: Option<bool>,
    /// De Lijn 10-journey card.
    delijn10_journey: Option<bool>,
    /// TEC 6-journey card.
    tec6_journey: Option<bool>,
    /// SNCB travel class (`SECOND` default | `FIRST`). Affects ONLY SNCB fares.
    ///
    /// NOTE: the Brussels multi-operator pass is NOT a user option. It is applied
    /// automatically as a post-hoc cap on the Brussels multi-operator fare (see the
    /// fare model's Brussels single-journey price in config).
    travel_class: Option<TravelClassInput>,
}

impl FareProfileInput {
    fn into_profile(self) -> routing_raptor::FareProfile {
        routing_raptor::FareProfile {
            category: self.passenger_category.unwrap_or_default().to_cost(),
            stib_subscription: self.stib_subscription.unwrap_or(false),
            delijn_subscription: self.delijn_subscription.unwrap_or(false),
            tec_subscription: self.tec_subscription.unwrap_or(false),
            sncb_subscription: self.sncb_subscription.unwrap_or(false),
            sncb_train_plus: self.sncb_train_plus.unwrap_or(false),
            delijn_10_journey: self.delijn10_journey.unwrap_or(false),
            tec_6_journey: self.tec6_journey.unwrap_or(false),
            travel_class: self.travel_class.unwrap_or_default().to_cost(),
        }
    }
}

/// Per-request bike cost profile override; every field optional. Provided fields
/// overlay the graph's default `BikeProfile`, so a sparse object changes only
/// what it names.
#[derive(InputObject, Default)]
struct BikeProfileInput {
    allow_steps: Option<bool>,
    allow_dismount: Option<bool>,
    ignore_cycleroutes: Option<bool>,
    stick_to_cycleroutes: Option<bool>,
    avoid_unsafe: Option<bool>,
    highway: Option<HighwayFactorsInput>,
    steps_cost: Option<f64>,
    unsafe_penalty: Option<f64>,
    oneway_roundabout: Option<f64>,
    oneway_primary: Option<f64>,
    oneway_secondary: Option<f64>,
    oneway_tertiary: Option<f64>,
    oneway_other: Option<f64>,
    access_foot_only: Option<f64>,
    access_cycleroute: Option<f64>,
    access_forbidden: Option<f64>,
    turncost: Option<f64>,
    consider_elevation: Option<bool>,
    uphillcost: Option<f64>,
    uphillcutoff: Option<f64>,
    downhillcost: Option<f64>,
    downhillcutoff: Option<f64>,
    total_mass: Option<f64>,
    max_speed: Option<f64>,
    s_c_x: Option<f64>,
    c_r: Option<f64>,
    biker_power: Option<f64>,
}

impl BikeProfileInput {
    /// Overlay the provided fields onto a base profile.
    fn merge_into(
        self,
        mut base: crate::structures::BikeProfile,
    ) -> crate::structures::BikeProfile {
        if let Some(v) = self.allow_steps {
            base.allow_steps = v;
        }
        if let Some(v) = self.allow_dismount {
            base.allow_dismount = v;
        }
        if let Some(v) = self.ignore_cycleroutes {
            base.ignore_cycleroutes = v;
        }
        if let Some(v) = self.stick_to_cycleroutes {
            base.stick_to_cycleroutes = v;
        }
        if let Some(v) = self.avoid_unsafe {
            base.avoid_unsafe = v;
        }
        if let Some(h) = self.highway {
            base.highway = h.merge_into(base.highway);
        }
        if let Some(v) = self.steps_cost {
            base.steps_cost = v;
        }
        if let Some(v) = self.unsafe_penalty {
            base.unsafe_penalty = v;
        }
        if let Some(v) = self.oneway_roundabout {
            base.oneway_roundabout = v;
        }
        if let Some(v) = self.oneway_primary {
            base.oneway_primary = v;
        }
        if let Some(v) = self.oneway_secondary {
            base.oneway_secondary = v;
        }
        if let Some(v) = self.oneway_tertiary {
            base.oneway_tertiary = v;
        }
        if let Some(v) = self.oneway_other {
            base.oneway_other = v;
        }
        if let Some(v) = self.access_foot_only {
            base.access_foot_only = v;
        }
        if let Some(v) = self.access_cycleroute {
            base.access_cycleroute = v;
        }
        if let Some(v) = self.access_forbidden {
            base.access_forbidden = v;
        }
        if let Some(v) = self.turncost {
            base.turncost = v;
        }
        if let Some(v) = self.consider_elevation {
            base.consider_elevation = v;
        }
        if let Some(v) = self.uphillcost {
            base.uphillcost = v;
        }
        if let Some(v) = self.uphillcutoff {
            base.uphillcutoff = v;
        }
        if let Some(v) = self.downhillcost {
            base.downhillcost = v;
        }
        if let Some(v) = self.downhillcutoff {
            base.downhillcutoff = v;
        }
        if let Some(v) = self.total_mass {
            base.total_mass = v;
        }
        if let Some(v) = self.max_speed {
            base.max_speed = v;
        }
        if let Some(v) = self.s_c_x {
            base.s_c_x = v;
        }
        if let Some(v) = self.c_r {
            base.c_r = v;
        }
        if let Some(v) = self.biker_power {
            base.biker_power = v;
        }
        base
    }
}

fn map_vehicle(pos: &VehiclePos, now_unix_secs: u64, max_age_secs: u64) -> LiveVehicleGql {
    let stale = match pos.timestamp {
        Some(ts) => now_unix_secs.saturating_sub(ts) > max_age_secs,
        None => true,
    };
    LiveVehicleGql {
        lat: pos.lat as f64,
        lng: pos.lng as f64,
        bearing: pos.bearing.map(|b| b as f64),
        observed_at: pos.timestamp.map(|ts| ts as i64).unwrap_or(0),
        stale,
    }
}

/// Detect a realtime platform reassignment for one scheduled stop.
///
/// Returns `Some(PlatformChangeGql { from, to })` when the RT feed reports a
/// different platform-level stop for the same parent station as `scheduled_stop_id`,
/// and both the scheduled and actual stop carry a `platform_code`.
///
/// Returns `None` when: the scheduled stop_id has no `_` suffix (not
/// platform-level), no RT platform info is known, the actual stop is the same
/// as scheduled, or either stop lacks a `platform_code`.
fn detect_platform_change(
    graph: &crate::structures::Graph,
    rt: &RealtimeIndex,
    trip: crate::ingestion::gtfs::TripId,
    scheduled_compact: usize,
    scheduled_stop_id: &str,
) -> Option<PlatformChangeGql> {
    let (parent, _) = scheduled_stop_id.rsplit_once('_')?;
    let scheduled_platform = graph.platform_code_of_stop(scheduled_compact)?;
    let actual_compact = rt.platform_swap(trip, parent)? as usize;
    if actual_compact == scheduled_compact {
        return None;
    }
    let actual_platform = graph.platform_code_of_stop(actual_compact)?;
    if scheduled_platform == actual_platform {
        return None;
    }
    Some(PlatformChangeGql {
        from: scheduled_platform.to_string(),
        to: actual_platform.to_string(),
    })
}

/// Resolve each input leg against the static schedule + live index by GTFS handle
/// only — no routing, no RAPTOR — so the overlay is stable across polls.
fn live_refresh(
    graph: &crate::structures::Graph,
    rt: &RealtimeIndex,
    legs: &[LiveLegInput],
    now_unix_secs: u64,
    max_age_secs: u64,
) -> LivePlanGql {
    use crate::structures::TripStatus;

    // What a resolved leg contributes to transfers and the journey ETA.
    struct Resolved {
        trip: crate::ingestion::gtfs::TripId,
        realtime_start: i32,
        realtime_end: i32,
        scheduled_end: i32,
    }

    let mut out_legs = Vec::with_capacity(legs.len());
    let mut resolved: Vec<Option<Resolved>> = Vec::with_capacity(legs.len());

    for leg in legs {
        // A handle (trip, stop, or board→alight order) failed to resolve.
        let unresolved = LiveLegGql {
            trip_id: leg.trip_id.clone(),
            found: false,
            status: LiveStatusGql::NotFound,
            delay_secs: 0,
            scheduled_start: None,
            scheduled_end: None,
            realtime_start: None,
            realtime_end: None,
            vehicle: None,
            alerts: Vec::new(),
            platform_change_board: None,
            platform_change_alight: None,
        };

        let (Some(trip), Some(board), Some(alight)) = (
            graph.trip_index_of(&leg.trip_id),
            graph.stop_index_of(&leg.board_stop_id),
            graph.stop_index_of(&leg.alight_stop_id),
        ) else {
            out_legs.push(unresolved);
            resolved.push(None);
            continue;
        };

        let Some((sched_start, sched_end)) = graph.scheduled_trip_leg_times(trip, board, alight)
        else {
            out_legs.push(unresolved);
            resolved.push(None);
            continue;
        };
        let (sched_start, sched_end) = (sched_start as i32, sched_end as i32);

        // Cancellation overrides any (possibly stale) per-stop delay: times stay
        // scheduled and the delay reads 0. Otherwise shift each end by its own delay.
        let (status, delay_secs, realtime_start, realtime_end) =
            match rt.status_with_sticky(trip, board as u32) {
                TripStatus::Canceled => (LiveStatusGql::Canceled, 0, sched_start, sched_end),
                non_canceled => {
                    let d_board = rt.delay_with_sticky(trip, board as u32);
                    let d_alight = rt.delay_with_sticky(trip, alight as u32);
                    let status = match non_canceled {
                        TripStatus::OnTime => LiveStatusGql::OnTime,
                        TripStatus::Delayed(_) => LiveStatusGql::Delayed,
                        _ => LiveStatusGql::NoData,
                    };
                    (status, d_board, sched_start + d_board, sched_end + d_alight)
                }
            };

        let leg_route_id = graph.raptor.route_id_of_trip(trip);
        let leg_alerts: Vec<LiveAlertGql> = rt
            .alerts_for_leg(
                &leg.trip_id,
                &leg.board_stop_id,
                &leg.alight_stop_id,
                leg_route_id,
                now_unix_secs,
            )
            .map(map_alert)
            .collect();
        let platform_change_board =
            detect_platform_change(graph, rt, trip, board, &leg.board_stop_id);
        let platform_change_alight =
            detect_platform_change(graph, rt, trip, alight, &leg.alight_stop_id);
        out_legs.push(LiveLegGql {
            trip_id: leg.trip_id.clone(),
            found: true,
            status,
            delay_secs,
            scheduled_start: Some(sched_start),
            scheduled_end: Some(sched_end),
            realtime_start: Some(realtime_start),
            realtime_end: Some(realtime_end),
            vehicle: rt.vehicle(trip).map(|pos| map_vehicle(pos, now_unix_secs, max_age_secs)),
            alerts: leg_alerts,
            platform_change_board,
            platform_change_alight,
        });
        resolved.push(Some(Resolved {
            trip,
            realtime_start,
            realtime_end,
            scheduled_end: sched_end,
        }));
    }

    // One transfer per interior boundary where both adjacent legs resolved.
    let mut transfers = Vec::new();
    for i in 0..resolved.len().saturating_sub(1) {
        let (Some(from), Some(to)) = (&resolved[i], &resolved[i + 1]) else {
            continue;
        };
        let realtime_arrival = from.realtime_end;
        let realtime_departure = to.realtime_start;
        let margin_secs = realtime_departure - realtime_arrival;
        transfers.push(LiveTransferGql {
            from_leg_index: i as i32,
            realtime_arrival,
            realtime_departure,
            margin_secs,
            reliability: transfer_reliability(graph, from.trip, to.trip, margin_secs),
        });
    }

    // ETA tracks the LAST resolved leg, so a trailing unresolved leg never nulls it.
    let last = resolved.iter().rev().flatten().next();
    LivePlanGql {
        legs: out_legs,
        transfers,
        eta: last.map(|r| r.realtime_end),
        scheduled_eta: last.map(|r| r.scheduled_end),
        generated_at: rt.generated_at,
    }
}

/// Chance of making a transfer given the realtime `margin`, scored with the same
/// delay-model primitive plan alternatives use: the feeder leg's CDF vs the
/// boarding leg's. `None` when either route type has no delay model.
fn transfer_reliability(
    graph: &crate::structures::Graph,
    feeder_trip: crate::ingestion::gtfs::TripId,
    board_trip: crate::ingestion::gtfs::TripId,
    margin: i32,
) -> Option<f64> {
    let feeder = graph
        .route_type_of_trip(feeder_trip)
        .and_then(|rt| graph.get_delay_model(rt))?;
    let board = graph
        .route_type_of_trip(board_trip)
        .and_then(|rt| graph.get_delay_model(rt));
    Some(feeder.prob_on_time_vs(board, margin) as f64)
}

/// Catch-probability of THIS backup given you are ready on the platform at your
/// original (realtime) departure time — *not* whole-journey reliability. The
/// backup actually departs at `scheduled_departure + D_backup`, so you catch it
/// iff `D_backup ≥ −slack`, where `slack` is the backup's scheduled departure
/// minus your ready time. Scored from the backup's OWN route-type delay
/// distribution. `None` when that route type has no delay model.
fn catch_reliability(
    graph: &crate::structures::Graph,
    backup_trip: crate::ingestion::gtfs::TripId,
    slack: i32,
) -> Option<f64> {
    let model = graph
        .route_type_of_trip(backup_trip)
        .and_then(|rt| graph.get_delay_model(rt))?;
    Some(model.prob_at_least(-slack) as f64)
}

/// Resolve same-station backups for a selected transit leg by GTFS handle only —
/// no routing — then layer the live `RealtimeIndex` on each backup's times and a
/// catch-reliability score. Returns an empty list when a handle is unknown or the
/// selected trip does not serve `board → alight` (never panics). Backups are
/// ordered chronologically by *scheduled* departure (stable across polls), even
/// though the displayed times are realtime.
fn station_backups(
    graph: &crate::structures::Graph,
    rt: &RealtimeIndex,
    trip_id: &str,
    board_stop_id: &str,
    alight_stop_id: &str,
    before: usize,
    after: usize,
    date: NaiveDate,
) -> Vec<StationBackupGql> {
    use chrono::Datelike;

    let (Some(orig_trip), Some(board), Some(alight)) = (
        graph.trip_index_of(trip_id),
        graph.stop_index_of(board_stop_id),
        graph.stop_index_of(alight_stop_id),
    ) else {
        return vec![];
    };

    let days = crate::ingestion::gtfs::date_to_days(date);
    let weekday = 1u8 << date.weekday().num_days_from_monday();

    // The selected leg's realtime departure is the reference the user "missed".
    let orig_rt_departure = match graph.scheduled_trip_leg_times(orig_trip, board, alight) {
        Some((dep, _)) => dep as i32 + rt.delay(orig_trip, board as u32),
        None => return vec![],
    };

    graph
        .station_backups(orig_trip, board, alight, before, after, days, weekday)
        .into_iter()
        .filter(|b| !rt.is_canceled(b.trip))
        .map(|b| {
            let realtime_departure = b.scheduled_departure as i32 + rt.delay(b.trip, board as u32);
            let realtime_arrival = b.scheduled_arrival as i32 + rt.delay(b.trip, alight as u32);
            let route = graph.get_route(b.route);
            StationBackupGql {
                trip_id: graph.trip_id_str(b.trip).unwrap_or(trip_id).to_string(),
                board_stop_id: board_stop_id.to_string(),
                alight_stop_id: alight_stop_id.to_string(),
                route_short_name: route.map(|r| r.route_short_name.clone()),
                route_long_name: route.map(|r| r.route_long_name.clone()),
                mode: route.map(|r| {
                    crate::ingestion::gtfs::display_route_type(r.route_type).to_string()
                }),
                route_color: route.and_then(|r| {
                    r.route_color
                        .map(|(rr, g, bb)| crate::structures::plan::rgb_to_hex(rr, g, bb))
                }),
                same_line: b.same_route,
                scheduled_departure: b.scheduled_departure as i32,
                scheduled_arrival: b.scheduled_arrival as i32,
                realtime_departure,
                realtime_arrival,
                reliability: catch_reliability(
                    graph,
                    b.trip,
                    b.scheduled_departure as i32 - orig_rt_departure,
                ),
            }
        })
        .collect()
}

pub struct QueryRoot;

#[async_graphql::Object]
impl QueryRoot {
    async fn ping(&self) -> &str {
        "pong"
    }

    async fn realtime_generated_at(&self, ctx: &Context<'_>) -> Result<i64, Error> {
        let rt = ctx.data::<SharedRealtime>()?.load_full();
        Ok(rt.generated_at)
    }

    async fn raptor(
        &self,
        ctx: &Context<'_>,
        from_lat: f64,
        from_lng: f64,
        to_lat: f64,
        to_lng: f64,
        date: Option<String>,
        time: Option<String>,
        // When provided and > 0, return all Pareto-optimal plans departing
        // within this many minutes after `time` (Range-RAPTOR).
        window_minutes: Option<i32>,
        // Override the default walk-radius (seconds) for access/egress stop
        // search.  Falls back to the value in config.yaml (default 600 s).
        walk_radius_secs: Option<i32>,
        // Arrival-slack (seconds): explore plans arriving up to this much after the
        // fastest, surfacing safer-but-slower alternatives. Falls back to config (900 s).
        arrival_slack_secs: Option<i32>,
        // When set, override the graph default for MCR uncapped inter-stop transfers
        // (live per-round multi-source foot-Dijkstra). Lets MCR be A/B-tested per call.
        unrestricted_transfers: Option<bool>,
        // When set, override the graph default for exact CCH foot access/egress.
        use_cch_access: Option<bool>,
        // Reliability bucket edges (sorted, strictly increasing, each in (0,1)).
        // Finer edges surface more reliability-distinct alternatives. Falls back to config.
        reliability_bucket_edges: Option<Vec<f64>>,
        // Travel modes the router may use. Defaults to [WALK, WALK_TRANSIT].
        modes: Option<Vec<Mode>>,
        // Bike cost profile override; sparse fields overlay the graph default.
        bike_profile: Option<BikeProfileInput>,
        // When true, direct walk/bike plans are built with the Deadline leg role.
        terminal_deadline: Option<bool>,
        from_station_id: Option<String>,
        to_station_id: Option<String>,
        // When true, emit a per-phase wall-clock decomposition of this query
        // (discovery/grid_alloc/forward/extract/backward, plus per-pass probe/
        // range/departure counts) as one structured log line. Purely additive
        // observability — never changes routing behavior or results. Falls back
        // to the graph default (config.yaml `profile_latency`, itself off by
        // default) when omitted.
        profile_latency: Option<bool>,
        // Pre-committed fare products (subscriptions, cards, passenger category,
        // Brupass) scaling each operator's marginal fare. Applied: each returned
        // plan's `price` is computed post-hoc from its boardings under this
        // profile. Absent ⇒ default single-ticket pricing.
        fare_profile: Option<FareProfileInput>,
    ) -> Result<Vec<Plan>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;

        let query = routing_raptor::RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
            date: parsed_date,
            time: parsed_time,
            window_minutes: window_minutes.map(|w| w.max(0) as u32),
            min_access_secs: walk_radius_secs.map(|s| s.max(0) as u32),
            arrival_slack_secs: arrival_slack_secs.map(|s| s.max(0) as u32),
            unrestricted_transfers,
            use_cch_access,
            reliability_bucket_edges: reliability_bucket_edges
                .map(|v| v.into_iter().map(|x| x as f32).collect()),
            modes,
            bike_profile: bike_profile.map(|i| i.merge_into(graph.raptor.bike_profile)),
            terminal_deadline: terminal_deadline.unwrap_or(false),
            onboard_origin: None,
            from_station_id,
            to_station_id,
            profile_latency,
            fare_profile: fare_profile.map(|i| i.into_profile()),
        };

        let rt = ctx.data::<SharedRealtime>()?.load_full();
        routing_raptor::route(graph.as_ref(), &query, rt.as_ref())
    }

    /// Re-plan from a position ABOARD a transit trip (between stops) to the
    /// lat/lng destination, surfacing stay-on / alight-and-transfer /
    /// alight-and-walk alternatives in one shot. Unlike `raptor`, there is no
    /// `fromLat`/`fromLng` — the origin is the boarded `onboardOrigin` handle.
    #[allow(clippy::too_many_arguments)]
    async fn onboard_raptor(
        &self,
        ctx: &Context<'_>,
        onboard_origin: OnboardOriginInput,
        to_lat: f64,
        to_lng: f64,
        date: Option<String>,
        time: Option<String>,
        // Override the default walk-radius (seconds) for egress stop search.
        // Falls back to the value in config.yaml (default 600 s).
        walk_radius_secs: Option<i32>,
        // Arrival-slack (seconds): explore plans arriving up to this much after the
        // fastest, surfacing safer-but-slower alternatives. Falls back to config (900 s).
        arrival_slack_secs: Option<i32>,
        // When set, override the graph default for MCR uncapped inter-stop transfers
        // (live per-round multi-source foot-Dijkstra). Lets MCR be A/B-tested per call.
        unrestricted_transfers: Option<bool>,
        // When set, override the graph default for exact CCH foot access/egress.
        use_cch_access: Option<bool>,
        // Reliability bucket edges (sorted, strictly increasing, each in (0,1)).
        reliability_bucket_edges: Option<Vec<f64>>,
        // Bike cost profile override; sparse fields overlay the graph default.
        bike_profile: Option<BikeProfileInput>,
        // When true, direct walk/bike plans are built with the Deadline leg role.
        terminal_deadline: Option<bool>,
        // Fare profile so onboard re-planning prices with the same options as the
        // originating journey (categories, subscriptions, Train+, cards).
        fare_profile: Option<FareProfileInput>,
    ) -> Result<Vec<Plan>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;

        let query = routing_raptor::RouteQuery {
            from_lat: 0.0,
            from_lng: 0.0,
            to_lat,
            to_lng,
            date: parsed_date,
            time: parsed_time,
            window_minutes: None,
            min_access_secs: walk_radius_secs.map(|s| s.max(0) as u32),
            arrival_slack_secs: arrival_slack_secs.map(|s| s.max(0) as u32),
            unrestricted_transfers,
            use_cch_access,
            reliability_bucket_edges: reliability_bucket_edges
                .map(|v| v.into_iter().map(|x| x as f32).collect()),
            modes: None,
            bike_profile: bike_profile.map(|i| i.merge_into(graph.raptor.bike_profile)),
            terminal_deadline: terminal_deadline.unwrap_or(false),
            onboard_origin: Some(routing_raptor::OnboardOrigin {
                trip_id: onboard_origin.trip_id,
                from_stop_id: onboard_origin.from_stop_id,
                from_stop_seq: onboard_origin.from_stop_seq.map(|s| s.max(0) as u32),
            }),
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: fare_profile.map(|i| i.into_profile()),
        };

        let rt = ctx.data::<SharedRealtime>()?.load_full();
        routing_raptor::route(graph.as_ref(), &query, rt.as_ref())
    }

    /// Debug query: same parameters as `raptor`, but also returns every candidate plan
    /// considered (with filter reasons) and the access walk metadata.
    async fn raptor_explain(
        &self,
        ctx: &Context<'_>,
        from_lat: f64,
        from_lng: f64,
        to_lat: f64,
        to_lng: f64,
        date: Option<String>,
        time: Option<String>,
        window_minutes: Option<i32>,
        walk_radius_secs: Option<i32>,
        arrival_slack_secs: Option<i32>,
        // When set, override the graph default for MCR uncapped inter-stop transfers
        // (live per-round multi-source foot-Dijkstra). Lets MCR be A/B-tested per call.
        unrestricted_transfers: Option<bool>,
        // When set, override the graph default for exact CCH foot access/egress.
        use_cch_access: Option<bool>,
        reliability_bucket_edges: Option<Vec<f64>>,
        modes: Option<Vec<Mode>>,
        bike_profile: Option<BikeProfileInput>,
        terminal_deadline: Option<bool>,
        // Fare profile, applied like `raptor`: each plan's `price` is computed
        // post-hoc from its boardings under this profile.
        fare_profile: Option<FareProfileInput>,
    ) -> Result<RaptorExplainResult, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;

        let query = routing_raptor::RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
            date: parsed_date,
            time: parsed_time,
            window_minutes: window_minutes.map(|w| w.max(0) as u32),
            min_access_secs: walk_radius_secs.map(|s| s.max(0) as u32),
            arrival_slack_secs: arrival_slack_secs.map(|s| s.max(0) as u32),
            unrestricted_transfers,
            use_cch_access,
            reliability_bucket_edges: reliability_bucket_edges
                .map(|v| v.into_iter().map(|x| x as f32).collect()),
            modes,
            bike_profile: bike_profile.map(|i| i.merge_into(graph.raptor.bike_profile)),
            terminal_deadline: terminal_deadline.unwrap_or(false),
            onboard_origin: None,
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: fare_profile.map(|i| i.into_profile()),
        };

        let rt = ctx.data::<SharedRealtime>()?.load_full();
        let result = routing_raptor::route_explain(graph.as_ref(), &query, rt.as_ref())?;

        Ok(RaptorExplainResult {
            plans: result.plans,
            candidates: result.candidates.into_iter().map(map_candidate).collect(),
            access: AccessInfoGql {
                walk_radius_secs: result.access.walk_radius_secs as i32,
                walk_radius_meters: result.access.walk_radius_meters as i32,
                origin_stops_found: result.access.origin_stops_found as i32,
                destination_stops_found: result.access.destination_stops_found as i32,
                access_attempts: result.access.access_attempts as i32,
                fell_back_to_walk_only: result.access.fell_back_to_walk_only,
            },
            stops_reached: result
                .stops_reached
                .into_iter()
                .map(|s| StopReachGql {
                    stop_idx: s.stop_idx as i32,
                    round: s.round as i32,
                    arrival_secs: s.arrival_secs as i32,
                    lat: s.lat,
                    lon: s.lon,
                    name: s.name,
                    path: s
                        .path
                        .into_iter()
                        .map(|l| StopPathLegGql {
                            is_transit: l.is_transit,
                            route_label: l.route_label,
                            geometry: l.geometry,
                        })
                        .collect(),
                })
                .collect(),
            origin: result.origin,
            destination: result.destination,
        })
    }

    /// Lazily compute earlier/later departure alternatives for **one** transit leg
    /// of one plan, instead of pre-computing them for every leg of every plan.
    /// `plan_index` / `leg_index` address the leg within the deterministic result
    /// of the same routing inputs the UI already issued. Returns only the data the
    /// alternatives UI needs (boarding time, arrival, swap reliability).
    #[allow(clippy::too_many_arguments)]
    async fn leg_alternatives(
        &self,
        ctx: &Context<'_>,
        from_lat: f64,
        from_lng: f64,
        to_lat: f64,
        to_lng: f64,
        date: Option<String>,
        time: Option<String>,
        window_minutes: Option<i32>,
        walk_radius_secs: Option<i32>,
        arrival_slack_secs: Option<i32>,
        // When set, override the graph default for MCR uncapped inter-stop transfers
        // (live per-round multi-source foot-Dijkstra). Lets MCR be A/B-tested per call.
        unrestricted_transfers: Option<bool>,
        // When set, override the graph default for exact CCH foot access/egress.
        use_cch_access: Option<bool>,
        reliability_bucket_edges: Option<Vec<f64>>,
        modes: Option<Vec<Mode>>,
        plan_index: i32,
        leg_index: i32,
        #[graphql(default = 0)] prev_count: i32,
        #[graphql(default = 0)] next_count: i32,
    ) -> Result<LegAlternatives, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;

        let query = routing_raptor::RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
            date: parsed_date,
            time: parsed_time,
            window_minutes: window_minutes.map(|w| w.max(0) as u32),
            min_access_secs: walk_radius_secs.map(|s| s.max(0) as u32),
            arrival_slack_secs: arrival_slack_secs.map(|s| s.max(0) as u32),
            unrestricted_transfers,
            use_cch_access,
            reliability_bucket_edges: reliability_bucket_edges
                .map(|v| v.into_iter().map(|x| x as f32).collect()),
            modes,
            bike_profile: None,
            terminal_deadline: false,
            onboard_origin: None,
            from_station_id: None,
            to_station_id: None,
            profile_latency: None,
            fare_profile: None,
        };

        let rt = ctx.data::<SharedRealtime>()?.load_full();
        let plans = routing_raptor::route(graph.as_ref(), &query, rt.as_ref())?;

        let plan = plans
            .get(plan_index.max(0) as usize)
            .ok_or_else(|| Error::new("plan_index out of range"))?;
        let leg = match plan.legs.get(leg_index.max(0) as usize) {
            Some(PlanLeg::Transit(t)) => t,
            _ => return Err(Error::new("leg_index is not a transit leg")),
        };

        let previous = leg.previous_departures_on(graph.as_ref(), prev_count.max(0) as usize)?;
        let next = leg.next_departures_on(graph.as_ref(), next_count.max(0) as usize)?;

        let to_alt = |legs: Vec<crate::structures::plan::PlanTransitLeg>| {
            legs.into_iter()
                .map(|l| AltDeparture {
                    start: l.start as i32,
                    end: l.end as i32,
                    reliability: l.transfer_risk.map(|r| r.reliability as f64),
                })
                .collect()
        };

        Ok(LegAlternatives {
            previous: to_alt(previous),
            next: to_alt(next),
        })
    }

    /// Stateless realtime overlay for a client-selected journey. The client passes
    /// the ordered transit legs by stable GTFS handles (`tripId` + board/alight
    /// `stopId`) each poll; this resolves them against the static schedule and the
    /// live `RealtimeIndex` WITHOUT re-running RAPTOR or re-optimising the plan, so
    /// the result is keyed by trip/stop and stable across polls. `date` is accepted
    /// for API consistency but unused: the `tripId` fully selects the stop-time column.
    async fn live_refresh(
        &self,
        ctx: &Context<'_>,
        legs: Vec<LiveLegInput>,
        date: Option<String>,
    ) -> Result<LivePlanGql, Error> {
        let _ = date;
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let rt = ctx.data::<SharedRealtime>()?.load_full();
        let max_age_secs = ctx
            .data::<VehiclePositionMaxAgeSecs>()
            .map(|v| v.0)
            .unwrap_or(120);
        let now_unix_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(live_refresh(graph.as_ref(), rt.as_ref(), &legs, now_unix_secs, max_age_secs))
    }

    /// Same-station backups for one transit leg the user is on/selected, keyed by
    /// stable GTFS handles (`tripId` + board/alight `stopId`). Returns every other
    /// departure FROM THE SAME boarding stop reaching the SAME alighting stop —
    /// including OTHER routes — each scored with catch-reliability against the live
    /// `RealtimeIndex`. Stateless (no routing, no plan index), so it is stable
    /// across polls; unknown handles resolve to an empty list, never a panic.
    /// `beforeCount` earlier and `afterCount` later departures are returned,
    /// ordered chronologically by scheduled departure.
    async fn station_backups(
        &self,
        ctx: &Context<'_>,
        trip_id: String,
        board_stop_id: String,
        alight_stop_id: String,
        #[graphql(default = 0)] before_count: i32,
        #[graphql(default = 3)] after_count: i32,
        date: Option<String>,
    ) -> Result<Vec<StationBackupGql>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let rt = ctx.data::<SharedRealtime>()?.load_full();
        let (parsed_date, _) = parse_date_time(&date, &None)?;
        Ok(station_backups(
            graph.as_ref(),
            rt.as_ref(),
            &trip_id,
            &board_stop_id,
            &alight_stop_id,
            before_count.max(0) as usize,
            after_count.max(0) as usize,
            parsed_date,
        ))
    }

    /// Travel-time map (isochrone / one-to-many reachability). From `center` at the
    /// given day + `time`, compute the travel time to reach many sampled points, so
    /// the client can paint a continuous green(0)->red(maxSeconds) heatmap. Reuses
    /// the RAPTOR forward pass + the exact one-to-many foot machinery (no separate
    /// engine). When `windowEndTime` is set, the isochrone is evaluated across the
    /// departure window `[time, windowEndTime]` and aggregated per cell by `aggregation`.
    #[allow(clippy::too_many_arguments)]
    async fn travel_time_map(
        &self,
        ctx: &Context<'_>,
        center_lat: f64,
        center_lng: f64,
        date: Option<String>,
        time: Option<String>,
        max_seconds: i32,
        // Allowed travel modes (same enum the router uses). Defaults to
        // [WALK, WALK_TRANSIT]. Empty is rejected.
        modes: Option<Vec<Mode>>,
        // Per-cell aggregation across the departure window. Defaults to BEST.
        aggregation: Option<TravelAggregationGql>,
        // When set, evaluate over the departure window [time, windowEndTime]
        // instead of a single departure.
        window_end_time: Option<String>,
        // Grid cell edge in metres. Clamped to [10, 1000]; absent ⇒ the configured
        // default (travel_map_grid_step_m). A safety cap coarsens it further if a
        // fine step over a large area would produce too many cells.
        grid_step_m: Option<f64>,
        // Override the CCH foot-access default (A/B). Falls back to graph default.
        use_cch_access: Option<bool>,
        // Override MCR uncapped inter-stop transfers. Falls back to graph default.
        unrestricted_transfers: Option<bool>,
    ) -> Result<TravelTimeMap, Error> {
        use chrono::{Datelike, Timelike};

        let graph = ctx.data::<SharedGraph>()?.load_full();
        let rt = ctx.data::<SharedRealtime>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;

        if max_seconds <= 0 {
            return Err(Error::new("maxSeconds must be positive"));
        }
        let max_secs = max_seconds as u32;

        let am = match &modes {
            None => crate::structures::ActiveModes::default(),
            Some(m) if m.is_empty() => return Err(Error::new("modes must not be empty")),
            Some(m) => crate::structures::ActiveModes::new(m),
        };
        let agg: crate::structures::TravelAggregation =
            aggregation.unwrap_or(TravelAggregationGql::Best).into();

        let start_time = parsed_time.num_seconds_from_midnight();
        let days = crate::ingestion::gtfs::date_to_days(parsed_date);
        let weekday = 1u8 << parsed_date.weekday().num_days_from_monday();

        let buckets = crate::structures::ReliabilityBuckets::new(&graph.raptor.reliability_bucket_edges);
        let slack = graph.raptor.arrival_slack_secs;
        let unrestricted = unrestricted_transfers.unwrap_or(graph.raptor.unrestricted_transfers);
        let use_cch = use_cch_access.unwrap_or(graph.raptor.use_cch_access);
        // Per-query grid cell size: clamp to [10, 1000] m; absent ⇒ configured default.
        // The graph fill applies a further safety cap (travel_map_max_cells) so a fine
        // step over a large reachable box can never blow up the cell count.
        let grid_step = match grid_step_m {
            Some(v) => v.clamp(10.0, 1000.0),
            None => graph.raptor.travel_map_grid_step_m,
        };
        let bike = crate::structures::BikeCost::new(graph.raptor.bike_profile);
        let center = crate::structures::LatLng {
            latitude: center_lat,
            longitude: center_lng,
        };

        let window_end = match &window_end_time {
            Some(t) => {
                let (_, end_t) = parse_date_time(&date, &Some(t.clone()))?;
                Some(end_t.num_seconds_from_midnight())
            }
            None => None,
        };

        let g = graph.as_ref();
        let cells = match window_end {
            Some(end) if end > start_time => g.travel_time_map_window(
                center, start_time, end, days, weekday, max_secs, grid_step, agg, &am, &buckets,
                slack, unrestricted, use_cch, rt.as_ref(), &bike,
            ),
            _ => g.travel_time_map(
                center, start_time, days, weekday, max_secs, grid_step, &am, &buckets, slack,
                unrestricted, use_cch, rt.as_ref(), &bike,
            ),
        };

        Ok(TravelTimeMap {
            cells: cells
                .into_iter()
                .map(|c| TravelCell {
                    lat: c.loc.latitude,
                    lng: c.loc.longitude,
                    seconds: c.seconds as i32,
                })
                .collect(),
            max_seconds,
            center_lat,
            center_lng,
        })
    }

    /// Returns every transit stop loaded from GTFS.
    /// Used by the Flutter client for the initial data sync (stop search).
    async fn gtfs_stops(&self, ctx: &Context<'_>) -> Result<Vec<GtfsStop>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        Ok(graph
            .gtfs_stops()
            .into_iter()
            .map(|(idx, name, lat, lon, mode)| GtfsStop {
                id: format!("maas:stop:{}", idx),
                name,
                lat,
                lon,
                mode,
            })
            .collect())
    }

    /// Returns every deduped physical station (platforms grouped by GTFS
    /// `parent_station`). Used by the client to offer zero-cost station-hub
    /// origins/destinations (`fromStationId`/`toStationId` on `raptor`).
    async fn gtfs_stations(&self, ctx: &Context<'_>) -> Result<Vec<GtfsStation>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        Ok(graph
            .gtfs_stations()
            .into_iter()
            .map(|(id, name, lat, lon, operators, modes, lines, platform_count)| GtfsStation {
                id,
                name,
                lat,
                lon,
                operators,
                modes,
                lines: lines
                    .into_iter()
                    .map(|l| StationLine {
                        mode: l.mode,
                        short_name: l.short_name,
                        color: l.color,
                        text_color: l.text_color,
                    })
                    .collect(),
                platform_count: platform_count as i32,
            })
            .collect())
    }

    /// Search the sibling Belgian address index (BeST-Add). Normalized
    /// prefix/substring matching over street and municipality names in NL/FR/DE
    /// (all spellings of one record are searchable). `limit` defaults to 10.
    /// When both `focusLat` and `focusLng` are given (the map centre the user is
    /// viewing), results are biased toward that point so the nearest match ranks
    /// first; otherwise ranking is pure text relevance.
    async fn search_addresses(
        &self,
        ctx: &Context<'_>,
        query: String,
        limit: Option<i32>,
        focus_lat: Option<f64>,
        focus_lng: Option<f64>,
    ) -> Result<Vec<Address>, Error> {
        let index = ctx.data::<SharedAddressIndex>()?.load_full();
        let limit = limit.map(|l| l.max(0) as usize).unwrap_or(10);
        let focus = match (focus_lat, focus_lng) {
            (Some(lat), Some(lng)) => Some((lat, lng)),
            _ => None,
        };
        Ok(index
            .search(&query, limit, focus)
            .into_iter()
            .map(|h| Address {
                id: h.id,
                label: h.label,
                lat: h.lat,
                lon: h.lon,
                street: h.street,
                house_number: h.house_number,
                postcode: h.postcode,
                municipality: h.municipality,
            })
            .collect())
    }

    /// CC-BY 4.0 attribution string that clients must display alongside results
    /// derived from the BeST-Add address feed (FPS BOSA).
    async fn address_attribution(&self) -> &'static str {
        ADDRESS_ATTRIBUTION
    }

    /// Returns every transit agency with its routes loaded from GTFS.
    /// Used by the Flutter client for the initial data sync (agency/route filter).
    async fn gtfs_agencies(&self, ctx: &Context<'_>) -> Result<Vec<GtfsAgency>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        Ok(graph
            .gtfs_agencies_with_routes()
            .into_iter()
            .map(|(agency_idx, name, url, routes)| GtfsAgency {
                id: format!("maas:agency:{}", agency_idx),
                name,
                url,
                routes: routes
                    .into_iter()
                    .map(
                        |(route_idx, short_name, long_name, mode, color, text_color)| GtfsRoute {
                            id: format!("maas:route:{}", route_idx),
                            short_name,
                            long_name,
                            mode,
                            color,
                            text_color,
                        },
                    )
                    .collect(),
            })
            .collect())
    }
}

const INDEX_HTML: &str = include_str!("static/index.html");
const TRAVEL_MAP_HTML: &str = include_str!("static/travel_map.html");
const TRAVEL_MAP_JS: &str = include_str!("static/js/travel-map.mjs");
const MAAS_JS: &str = include_str!("static/maas.js");
const SW_JS: &str = include_str!("static/sw.js");
const MANIFEST: &str = include_str!("static/manifest.webmanifest");
const ICON_SVG: &str = include_str!("static/icon.svg");
const ICON_MASKABLE_SVG: &str = include_str!("static/icon-maskable.svg");

// Live-journey persistence layer (sqlite-wasm + OPFS SAHPool VFS). The SAHPool
// VFS runs on the main thread and needs NO COOP/COEP headers, so none are set.
const LIVE_DB_JS: &str = include_str!("static/js/live-db.mjs");
const LIVE_STORE_JS: &str = include_str!("static/js/live-store.mjs");
const LIVE_LOGIC_JS: &str = include_str!("static/js/live-logic.mjs");
const LIVE_VIEW_JS: &str = include_str!("static/js/live-view.mjs");
const LIVE_MEM_JS: &str = include_str!("static/js/live-mem.mjs");
const SQLITE_WASM_JS: &str = include_str!("static/js/vendor/sqlite-wasm/sqlite3.mjs");
const SQLITE_WASM: &[u8] = include_bytes!("static/js/vendor/sqlite-wasm/sqlite3.wasm");

struct Js(&'static str);
impl IntoResponse for Js {
    fn into_response(self) -> Response {
        Response::builder()
            .content_type("application/javascript; charset=utf-8")
            .body(self.0)
    }
}

struct Wasm(&'static [u8]);
impl IntoResponse for Wasm {
    fn into_response(self) -> Response {
        Response::builder()
            .content_type("application/wasm")
            .body(self.0)
    }
}

struct WebManifest(&'static str);
impl IntoResponse for WebManifest {
    fn into_response(self) -> Response {
        Response::builder()
            .content_type("application/manifest+json; charset=utf-8")
            .body(self.0)
    }
}

struct Svg(&'static str);
impl IntoResponse for Svg {
    fn into_response(self) -> Response {
        Response::builder()
            .content_type("image/svg+xml; charset=utf-8")
            .body(self.0)
    }
}

#[handler]
pub async fn index_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

#[handler]
pub async fn travel_map_page() -> Html<&'static str> {
    Html(TRAVEL_MAP_HTML)
}

#[handler]
pub async fn travel_map_js_handler() -> Js {
    Js(TRAVEL_MAP_JS)
}

#[handler]
pub async fn maas_js_handler() -> Js {
    Js(MAAS_JS)
}

#[handler]
pub async fn live_db_js_handler() -> Js {
    Js(LIVE_DB_JS)
}

#[handler]
pub async fn live_store_js_handler() -> Js {
    Js(LIVE_STORE_JS)
}

#[handler]
pub async fn live_logic_js_handler() -> Js {
    Js(LIVE_LOGIC_JS)
}

#[handler]
pub async fn live_view_js_handler() -> Js {
    Js(LIVE_VIEW_JS)
}

#[handler]
pub async fn live_mem_js_handler() -> Js {
    Js(LIVE_MEM_JS)
}

#[handler]
pub async fn sqlite_wasm_js_handler() -> Js {
    Js(SQLITE_WASM_JS)
}

#[handler]
pub async fn sqlite_wasm_handler() -> Wasm {
    Wasm(SQLITE_WASM)
}

#[handler]
pub async fn sw_js_handler() -> Js {
    Js(SW_JS)
}

#[handler]
pub async fn manifest_handler() -> WebManifest {
    WebManifest(MANIFEST)
}

#[handler]
pub async fn icon_svg_handler() -> Svg {
    Svg(ICON_SVG)
}

#[handler]
pub async fn icon_maskable_svg_handler() -> Svg {
    Svg(ICON_MASKABLE_SVG)
}

#[handler]
async fn graphiql() -> Html<String> {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

pub fn build_schema(graph: SharedGraph) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    let realtime: SharedRealtime = Arc::new(arc_swap::ArcSwap::from_pointee(RealtimeIndex::new()));
    build_schema_rt(graph, realtime)
}

pub fn build_schema_rt(
    graph: SharedGraph,
    realtime: SharedRealtime,
) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    build_schema_rt_full(graph, realtime, 120)
}

pub fn build_schema_rt_full(
    graph: SharedGraph,
    realtime: SharedRealtime,
    vehicle_position_max_age_secs: u64,
) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    let address: SharedAddressIndex = Arc::new(arc_swap::ArcSwap::from_pointee(AddressIndex::default()));
    build_schema_full(graph, realtime, vehicle_position_max_age_secs, address)
}

/// Build the schema with an explicit address index handle. Used by `server()` to
/// inject the loaded BeST-Add index and by tests to inject synthetic data.
pub fn build_schema_full(
    graph: SharedGraph,
    realtime: SharedRealtime,
    vehicle_position_max_age_secs: u64,
    address: SharedAddressIndex,
) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(graph)
        .data(realtime)
        .data(address)
        .data(VehiclePositionMaxAgeSecs(vehicle_position_max_age_secs))
        .finish()
}

pub async fn server(graph: SharedGraph, config: Arc<Config>) -> std::io::Result<()> {
    scheduler::spawn(graph.clone(), config.clone());

    let realtime: SharedRealtime = Arc::new(arc_swap::ArcSwap::from_pointee(RealtimeIndex::new()));
    realtime_poller::spawn(graph.clone(), realtime.clone(), config.clone());

    let vp_max_age = config
        .realtime
        .as_ref()
        .map(|r| r.vehicle_position_max_age_secs)
        .unwrap_or(120);

    let cache_dir = config
        .auto_update
        .as_ref()
        .map(|a| a.cache_dir.clone())
        .unwrap_or_else(|| "cache".to_string());
    let mut address_index = crate::services::build::load_or_build_address_index(
        &config.build,
        &cache_dir,
        "address.bin",
        config.default_routing.address_box_coord_epsilon_m(),
    );
    address_index.set_search_params(config.default_routing.to_address_search_params());
    let address: SharedAddressIndex = Arc::new(arc_swap::ArcSwap::from_pointee(address_index));

    let schema = build_schema_full(graph, realtime, vp_max_age, address);
    let app = Route::new()
        .at("/graphql", GraphQL::new(schema))
        .at("/graphiql", get(graphiql))
        .at("/maas.js", get(maas_js_handler))
        .at("/static/js/live-db.mjs", get(live_db_js_handler))
        .at("/static/js/live-store.mjs", get(live_store_js_handler))
        .at("/static/js/live-logic.mjs", get(live_logic_js_handler))
        .at("/static/js/live-view.mjs", get(live_view_js_handler))
        .at("/static/js/live-mem.mjs", get(live_mem_js_handler))
        .at(
            "/static/js/vendor/sqlite-wasm/sqlite3.mjs",
            get(sqlite_wasm_js_handler),
        )
        .at(
            "/static/js/vendor/sqlite-wasm/sqlite3.wasm",
            get(sqlite_wasm_handler),
        )
        .at("/sw.js", get(sw_js_handler))
        .at("/manifest.webmanifest", get(manifest_handler))
        .at("/icon.svg", get(icon_svg_handler))
        .at("/icon-maskable.svg", get(icon_maskable_svg_handler))
        .at("/static/js/travel-map.mjs", get(travel_map_js_handler))
        .at("/travel_map", get(travel_map_page))
        .at("/", get(index_page));

    let bind = format!("{}:{}", config.server.host, config.server.port);
    tracing::info!("serving on {bind}");
    Server::new(TcpListener::bind(&bind)).run(app).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_date_time_valid_date_and_time() {
        let (d, t) = parse_date_time(
            &Some("2025-03-15".to_string()),
            &Some("08:30:00".to_string()),
        )
        .unwrap();
        assert_eq!(d, NaiveDate::from_ymd_opt(2025, 3, 15).unwrap());
        assert_eq!(t, NaiveTime::from_hms_opt(8, 30, 0).unwrap());
    }

    #[test]
    fn parse_date_time_short_time_format() {
        let (_, t) =
            parse_date_time(&Some("2025-01-01".to_string()), &Some("14:05".to_string())).unwrap();
        assert_eq!(t, NaiveTime::from_hms_opt(14, 5, 0).unwrap());
    }

    #[test]
    fn parse_date_time_none_defaults_to_now() {
        let (d, t) = parse_date_time(&None, &None).unwrap();
        let now = Local::now().naive_local();
        assert_eq!(d, now.date());
        // Time should be within a second of now
        let diff = (t - now.time()).num_seconds().abs();
        assert!(diff < 2, "time diff {diff}s too large");
    }

    #[test]
    fn parse_date_time_invalid_date_returns_error() {
        let result = parse_date_time(&Some("not-a-date".to_string()), &None);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Invalid date"));
    }

    #[test]
    fn parse_date_time_invalid_time_returns_error() {
        let result = parse_date_time(&None, &Some("99:99:99".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("Invalid time"));
    }
}
