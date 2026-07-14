use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, InputObject, Schema, SimpleObject,
    http::GraphiQLSource,
};
use async_graphql_poem::GraphQL;
use chrono::{Local, NaiveDate, NaiveTime};
use poem::{
    EndpointExt, IntoResponse, Response, Result, Route, Server, get, handler,
    listener::TcpListener, middleware::SizeLimit, web::Html,
};
use tokio::sync::Semaphore;

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

pub type SharedAddressIndex = Arc<arc_swap::ArcSwap<AddressIndex>>;

/// Opaque wrapper so this has a unique `TypeId` in the schema context, preventing
/// collision with any other `u64` data item.
struct VehiclePositionMaxAgeSecs(u64);

const HEAVY_QUERY_PERMITS: usize = 4;

const HEAVY_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Enforced ceiling on the GraphQL complexity limit regardless of the configured
/// value. Guarantees a batch of aliased heavy fields is rejected even if the
/// config value is raised.
const MAX_COMPLEXITY_CEILING: usize = 2000;

const MAX_WINDOW_MINUTES: i32 = 1440;
const MAX_WALK_RADIUS_SECS: i32 = 3600;
const MAX_ARRIVAL_SLACK_SECS: i32 = 7200;
const MAX_TRAVEL_MAP_SECONDS: i32 = 4 * 3600;

struct HeavyQueryLimiter(Arc<Semaphore>);

fn reject_over(name: &str, value: i32, max: i32) -> Result<(), Error> {
    if value > max {
        return Err(Error::new(format!("{name} must be <= {max}")));
    }
    Ok(())
}

async fn run_heavy<T, F>(ctx: &Context<'_>, f: F) -> Result<T, Error>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, Error> + Send + 'static,
{
    let sem = ctx.data::<HeavyQueryLimiter>()?.0.clone();
    let permit = sem
        .acquire_owned()
        .await
        .map_err(|_| Error::new("routing limiter unavailable"))?;
    let mut handle = tokio::task::spawn_blocking(f);
    match tokio::time::timeout(HEAVY_QUERY_TIMEOUT, &mut handle).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(Error::new("routing query failed")),
        Err(_) => {
            tokio::spawn(async move {
                let _ = handle.await;
                drop(permit);
            });
            Err(Error::new("routing query timed out"))
        }
    }
}

#[derive(Clone, async_graphql::SimpleObject)]
pub struct WebConfig {
    pub tile_url: String,
    pub tile_attribution: String,
    pub graphiql_enabled: bool,
}

impl Default for WebConfig {
    fn default() -> Self {
        WebConfig {
            tile_url: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png".to_string(),
            tile_attribution: "© OpenStreetMap contributors".to_string(),
            graphiql_enabled: false,
        }
    }
}

#[derive(SimpleObject)]
struct GtfsStop {
    id: String,
    name: String,
    lat: f64,
    #[graphql(name = "lng")]
    lon: f64,
    mode: String,
}

#[derive(SimpleObject)]
struct StationLine {
    mode: String,
    short_name: String,
    color: Option<String>,
    text_color: Option<String>,
}

#[derive(SimpleObject)]
struct Address {
    id: String,
    label: String,
    lat: f64,
    #[graphql(name = "lng")]
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
    #[graphql(name = "lng")]
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
    color: Option<String>,
    text_color: Option<String>,
}

#[derive(SimpleObject)]
struct GtfsAgency {
    id: String,
    name: String,
    url: String,
    routes: Vec<GtfsRoute>,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "CandidateStatus")]
enum CandidateStatusGql {
    Kept,
    NotImproving,
    ReconstructionEmpty,
    ExtremeRisk,
    ParetoDominated,
}

#[derive(SimpleObject)]
#[graphql(name = "PlanCandidate")]
struct PlanCandidateGql {
    round: i32,
    origin_departure: i32,
    plan: Option<Plan>,
    status: CandidateStatusGql,
    dominator_index: Option<i32>,
    dominator_departs_later: Option<bool>,
    dominator_arrives_earlier: Option<bool>,
    dominator_fewer_transfers: Option<bool>,
    dominator_higher_reliability: Option<bool>,
}

#[derive(SimpleObject)]
#[graphql(name = "AccessInfo")]
struct AccessInfoGql {
    walk_radius_secs: i32,
    walk_radius_meters: i32,
    origin_stops_found: i32,
    destination_stops_found: i32,
    access_attempts: i32,
    fell_back_to_walk_only: bool,
}

#[derive(SimpleObject)]
#[graphql(name = "StopPathLeg")]
struct StopPathLegGql {
    is_transit: bool,
    route_label: String,
    geometry: Vec<PlanCoordinate>,
}

#[derive(SimpleObject)]
#[graphql(name = "StopReach")]
struct StopReachGql {
    stop_idx: i32,
    round: i32,
    arrival_secs: i32,
    lat: f64,
    #[graphql(name = "lng")]
    lon: f64,
    name: String,
    path: Vec<StopPathLegGql>,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "TravelAggregation")]
enum TravelAggregationGql {
    Best,
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

#[derive(SimpleObject)]
struct TravelCell {
    lat: f64,
    lng: f64,
    seconds: i32,
}

#[derive(SimpleObject)]
struct TravelTimeMap {
    cells: Vec<TravelCell>,
    max_seconds: i32,
    center_lat: f64,
    center_lng: f64,
}

#[derive(SimpleObject)]
struct RaptorExplainResult {
    plans: Vec<Plan>,
    candidates: Vec<PlanCandidateGql>,
    access: AccessInfoGql,
    stops_reached: Vec<StopReachGql>,
    origin: PlanCoordinate,
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

#[derive(SimpleObject)]
struct AltDeparture {
    start: i32,
    end: i32,
    reliability: Option<f64>,
}

#[derive(SimpleObject)]
struct LegAlternatives {
    previous: Vec<AltDeparture>,
    next: Vec<AltDeparture>,
}

#[derive(InputObject)]
struct LiveLegInput {
    trip_id: String,
    board_stop_id: String,
    alight_stop_id: String,
}

#[derive(async_graphql::InputObject)]
struct OnboardOriginInput {
    trip_id: String,
    from_stop_id: Option<String>,
    from_stop_seq: Option<i32>,
}

#[derive(async_graphql::Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "LiveStatus")]
enum LiveStatusGql {
    NoData,
    OnTime,
    Delayed,
    Canceled,
    NotFound,
}

#[derive(SimpleObject)]
#[graphql(name = "LiveAlert")]
struct LiveAlertGql {
    header: Option<String>,
    description: Option<String>,
    cause: Option<String>,
    effect: Option<String>,
}

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

#[derive(SimpleObject)]
#[graphql(name = "PlatformChange")]
struct PlatformChangeGql {
    from: String,
    to: String,
}

#[derive(SimpleObject)]
#[graphql(name = "LiveVehicle")]
struct LiveVehicleGql {
    lat: f64,
    lng: f64,
    bearing: Option<f64>,
    observed_at: i64,
    stale: bool,
}

#[derive(SimpleObject)]
#[graphql(name = "LiveLeg")]
struct LiveLegGql {
    trip_id: String,
    found: bool,
    status: LiveStatusGql,
    delay_secs: i32,
    scheduled_start: Option<i32>,
    scheduled_end: Option<i32>,
    realtime_start: Option<i32>,
    realtime_end: Option<i32>,
    vehicle: Option<LiveVehicleGql>,
    alerts: Vec<LiveAlertGql>,
    platform_change_board: Option<PlatformChangeGql>,
    platform_change_alight: Option<PlatformChangeGql>,
}

#[derive(SimpleObject)]
#[graphql(name = "LiveTransfer")]
struct LiveTransferGql {
    from_leg_index: i32,
    realtime_arrival: i32,
    realtime_departure: i32,
    margin_secs: i32,
    reliability: Option<f64>,
}

#[derive(SimpleObject)]
#[graphql(name = "LivePlan")]
struct LivePlanGql {
    legs: Vec<LiveLegGql>,
    transfers: Vec<LiveTransferGql>,
    eta: Option<i32>,
    scheduled_eta: Option<i32>,
    generated_at: i64,
}

#[derive(SimpleObject)]
#[graphql(name = "StationBackup")]
struct StationBackupGql {
    trip_id: String,
    board_stop_id: String,
    alight_stop_id: String,
    route_short_name: Option<String>,
    route_long_name: Option<String>,
    mode: Option<String>,
    route_color: Option<String>,
    same_line: bool,
    scheduled_departure: i32,
    scheduled_arrival: i32,
    realtime_departure: i32,
    realtime_arrival: i32,
    reliability: Option<f64>,
}

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

#[derive(InputObject, Default)]
struct FareProfileInput {
    passenger_category: Option<PassengerCategoryInput>,
    stib_subscription: Option<bool>,
    delijn_subscription: Option<bool>,
    tec_subscription: Option<bool>,
    sncb_subscription: Option<bool>,
    sncb_train_plus: Option<bool>,
    delijn10_journey: Option<bool>,
    tec6_journey: Option<bool>,
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

fn live_refresh(
    graph: &crate::structures::Graph,
    rt: &RealtimeIndex,
    legs: &[LiveLegInput],
    now_unix_secs: u64,
    max_age_secs: u64,
) -> LivePlanGql {
    use crate::structures::TripStatus;

    struct Resolved {
        trip: crate::ingestion::gtfs::TripId,
        realtime_start: i32,
        realtime_end: i32,
        scheduled_end: i32,
    }

    let mut out_legs = Vec::with_capacity(legs.len());
    let mut resolved: Vec<Option<Resolved>> = Vec::with_capacity(legs.len());

    for leg in legs {
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

    // ETA tracks the last RESOLVED leg, so a trailing unresolved leg never nulls it.
    let last = resolved.iter().rev().flatten().next();
    LivePlanGql {
        legs: out_legs,
        transfers,
        eta: last.map(|r| r.realtime_end),
        scheduled_eta: last.map(|r| r.scheduled_end),
        generated_at: rt.generated_at,
    }
}

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

    async fn web_config(&self, ctx: &Context<'_>) -> Result<WebConfig, Error> {
        Ok(ctx.data::<WebConfig>()?.clone())
    }

    async fn realtime_generated_at(&self, ctx: &Context<'_>) -> Result<i64, Error> {
        let rt = ctx.data::<SharedRealtime>()?.load_full();
        Ok(rt.generated_at)
    }

    #[graphql(
        complexity = "50 + child_complexity + (window_minutes.unwrap_or(0).max(0) as usize) / 10"
    )]
    async fn raptor(
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
        unrestricted_transfers: Option<bool>,
        use_cch_access: Option<bool>,
        reliability_bucket_edges: Option<Vec<f64>>,
        modes: Option<Vec<Mode>>,
        bike_profile: Option<BikeProfileInput>,
        terminal_deadline: Option<bool>,
        from_station_id: Option<String>,
        to_station_id: Option<String>,
        profile_latency: Option<bool>,
        fare_profile: Option<FareProfileInput>,
    ) -> Result<Vec<Plan>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;
        reject_over("windowMinutes", window_minutes.unwrap_or(0), MAX_WINDOW_MINUTES)?;
        reject_over("walkRadiusSecs", walk_radius_secs.unwrap_or(0), MAX_WALK_RADIUS_SECS)?;
        reject_over("arrivalSlackSecs", arrival_slack_secs.unwrap_or(0), MAX_ARRIVAL_SLACK_SECS)?;

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
        run_heavy(ctx, move || {
            routing_raptor::route(graph.as_ref(), &query, rt.as_ref())
        })
        .await
    }

    #[allow(clippy::too_many_arguments)]
    #[graphql(complexity = "50 + child_complexity")]
    async fn onboard_raptor(
        &self,
        ctx: &Context<'_>,
        onboard_origin: OnboardOriginInput,
        to_lat: f64,
        to_lng: f64,
        date: Option<String>,
        time: Option<String>,
        walk_radius_secs: Option<i32>,
        arrival_slack_secs: Option<i32>,
        unrestricted_transfers: Option<bool>,
        use_cch_access: Option<bool>,
        reliability_bucket_edges: Option<Vec<f64>>,
        bike_profile: Option<BikeProfileInput>,
        terminal_deadline: Option<bool>,
        fare_profile: Option<FareProfileInput>,
    ) -> Result<Vec<Plan>, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;
        reject_over("walkRadiusSecs", walk_radius_secs.unwrap_or(0), MAX_WALK_RADIUS_SECS)?;
        reject_over("arrivalSlackSecs", arrival_slack_secs.unwrap_or(0), MAX_ARRIVAL_SLACK_SECS)?;

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
        run_heavy(ctx, move || {
            routing_raptor::route(graph.as_ref(), &query, rt.as_ref())
        })
        .await
    }

    #[graphql(
        complexity = "80 + child_complexity + (window_minutes.unwrap_or(0).max(0) as usize) / 10"
    )]
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
        unrestricted_transfers: Option<bool>,
        use_cch_access: Option<bool>,
        reliability_bucket_edges: Option<Vec<f64>>,
        modes: Option<Vec<Mode>>,
        bike_profile: Option<BikeProfileInput>,
        terminal_deadline: Option<bool>,
        fare_profile: Option<FareProfileInput>,
    ) -> Result<RaptorExplainResult, Error> {
        let graph = ctx.data::<SharedGraph>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;
        reject_over("windowMinutes", window_minutes.unwrap_or(0), MAX_WINDOW_MINUTES)?;
        reject_over("walkRadiusSecs", walk_radius_secs.unwrap_or(0), MAX_WALK_RADIUS_SECS)?;
        reject_over("arrivalSlackSecs", arrival_slack_secs.unwrap_or(0), MAX_ARRIVAL_SLACK_SECS)?;

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
        let result = run_heavy(ctx, move || {
            routing_raptor::route_explain(graph.as_ref(), &query, rt.as_ref())
        })
        .await?;

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

    #[allow(clippy::too_many_arguments)]
    #[graphql(
        complexity = "50 + child_complexity + (window_minutes.unwrap_or(0).max(0) as usize) / 10"
    )]
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
        unrestricted_transfers: Option<bool>,
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
        reject_over("windowMinutes", window_minutes.unwrap_or(0), MAX_WINDOW_MINUTES)?;
        reject_over("walkRadiusSecs", walk_radius_secs.unwrap_or(0), MAX_WALK_RADIUS_SECS)?;
        reject_over("arrivalSlackSecs", arrival_slack_secs.unwrap_or(0), MAX_ARRIVAL_SLACK_SECS)?;

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
        let route_graph = graph.clone();
        let plans = run_heavy(ctx, move || {
            routing_raptor::route(route_graph.as_ref(), &query, rt.as_ref())
        })
        .await?;

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

    #[allow(clippy::too_many_arguments)]
    #[graphql(complexity = "100 + child_complexity + (max_seconds.max(0) as usize) / 60")]
    async fn travel_time_map(
        &self,
        ctx: &Context<'_>,
        center_lat: f64,
        center_lng: f64,
        date: Option<String>,
        time: Option<String>,
        max_seconds: i32,
        modes: Option<Vec<Mode>>,
        aggregation: Option<TravelAggregationGql>,
        window_end_time: Option<String>,
        grid_step_m: Option<f64>,
        use_cch_access: Option<bool>,
        unrestricted_transfers: Option<bool>,
    ) -> Result<TravelTimeMap, Error> {
        use chrono::{Datelike, Timelike};

        let graph = ctx.data::<SharedGraph>()?.load_full();
        let rt = ctx.data::<SharedRealtime>()?.load_full();
        let (parsed_date, parsed_time) = parse_date_time(&date, &time)?;

        if max_seconds <= 0 {
            return Err(Error::new("maxSeconds must be positive"));
        }
        reject_over("maxSeconds", max_seconds, MAX_TRAVEL_MAP_SECONDS)?;
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

        let cells = run_heavy(ctx, move || {
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
            Ok(cells)
        })
        .await?;

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

    /// CC-BY 4.0 attribution clients must display alongside BeST-Add results.
    async fn address_attribution(&self) -> &'static str {
        ADDRESS_ATTRIBUTION
    }

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

// SAHPool VFS runs on the main thread and needs NO COOP/COEP headers, so none are set.
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
    build_schema_full(graph, realtime, vehicle_position_max_age_secs, address, WebConfig::default(), None, None)
}

pub fn build_schema_full(
    graph: SharedGraph,
    realtime: SharedRealtime,
    vehicle_position_max_age_secs: u64,
    address: SharedAddressIndex,
    web_config: WebConfig,
    max_depth: Option<usize>,
    max_complexity: Option<usize>,
) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    let mut builder = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(graph)
        .data(realtime)
        .data(address)
        .data(web_config)
        .data(VehiclePositionMaxAgeSecs(vehicle_position_max_age_secs))
        .data(HeavyQueryLimiter(Arc::new(Semaphore::new(HEAVY_QUERY_PERMITS))));
    if let Some(depth) = max_depth {
        builder = builder.limit_depth(depth);
    }
    let complexity = max_complexity
        .map(|c| c.min(MAX_COMPLEXITY_CEILING))
        .unwrap_or(MAX_COMPLEXITY_CEILING);
    builder = builder.limit_complexity(complexity);
    builder.finish()
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

    let cache_dir = config.cache_dir();
    let address_fp = crate::services::fingerprint::address_fingerprint(config.as_ref(), &cache_dir);
    let mut address_index = crate::services::build::load_or_build_address_index(
        &config.build,
        &cache_dir,
        &config.build.address_output,
        config.default_routing.address_box_coord_epsilon_m(),
        &address_fp,
    );
    address_index.set_search_params(config.default_routing.to_address_search_params());
    let address: SharedAddressIndex = Arc::new(arc_swap::ArcSwap::from_pointee(address_index));

    let web_config = WebConfig {
        tile_url: config.server.tiles.url.clone(),
        tile_attribution: config.server.tiles.attribution.clone(),
        graphiql_enabled: config.server.graphiql_enabled,
    };
    let schema = build_schema_full(
        graph,
        realtime,
        vp_max_age,
        address,
        web_config,
        Some(config.server.graphql_max_depth),
        Some(config.server.graphql_max_complexity),
    );
    let mut app = Route::new()
        .at("/graphql", GraphQL::new(schema).with(SizeLimit::new(64 * 1024)))
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

    if config.server.graphiql_enabled {
        app = app.at("/graphiql", get(graphiql));
    }

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
