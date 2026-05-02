use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Schema, SimpleObject,
    http::GraphiQLSource,
};
use async_graphql_poem::GraphQL;
use chrono::{Local, NaiveDate, NaiveTime};
use poem::{Result, Route, Server, get, handler, listener::TcpListener, web::Html};

use crate::{
    routing::routing_raptor,
    structures::{
        Graph, ServerConfig,
        plan::{CandidateStatus, Plan},
    },
};

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
    /// Dropped because a transit leg moves away from the destination.
    BackwardDetour,
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
struct RaptorExplainResult {
    /// Same plans as the `raptor` query would return for identical parameters.
    plans: Vec<Plan>,
    /// Every candidate considered across all RAPTOR rounds, including filtered ones.
    candidates: Vec<PlanCandidateGql>,
    access: AccessInfoGql,
}

fn map_candidate(c: crate::structures::plan::PlanCandidate) -> PlanCandidateGql {
    let (status, dominator_index) = match &c.status {
        CandidateStatus::Kept => (CandidateStatusGql::Kept, None),
        CandidateStatus::NotImproving => (CandidateStatusGql::NotImproving, None),
        CandidateStatus::ReconstructionEmpty => (CandidateStatusGql::ReconstructionEmpty, None),
        CandidateStatus::ExtremeRisk => (CandidateStatusGql::ExtremeRisk, None),
        CandidateStatus::BackwardDetour => (CandidateStatusGql::BackwardDetour, None),
        CandidateStatus::ParetoDominated { dominator_index } => {
            (CandidateStatusGql::ParetoDominated, Some(*dominator_index as i32))
        }
    };
    PlanCandidateGql {
        round: c.round as i32,
        origin_departure: c.origin_departure as i32,
        plan: c.plan,
        status,
        dominator_index,
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

pub struct QueryRoot;

#[async_graphql::Object]
impl QueryRoot {
    async fn ping(&self) -> &str {
        "pong"
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
    ) -> Result<Vec<Plan>, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;
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
        };

        routing_raptor::route(graph.as_ref(), &query)
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
    ) -> Result<RaptorExplainResult, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;
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
        };

        let result = routing_raptor::route_explain(graph.as_ref(), &query)?;

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
        })
    }

    /// Returns every transit stop loaded from GTFS.
    /// Used by the Flutter client for the initial data sync (stop search).
    async fn gtfs_stops(&self, ctx: &Context<'_>) -> Result<Vec<GtfsStop>, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;
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

    /// Returns every transit agency with its routes loaded from GTFS.
    /// Used by the Flutter client for the initial data sync (agency/route filter).
    async fn gtfs_agencies(&self, ctx: &Context<'_>) -> Result<Vec<GtfsAgency>, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;
        Ok(graph
            .gtfs_agencies_with_routes()
            .into_iter()
            .map(|(agency_idx, name, url, routes)| GtfsAgency {
                id: format!("maas:agency:{}", agency_idx),
                name,
                url,
                routes: routes
                    .into_iter()
                    .map(|(route_idx, short_name, long_name, mode, color, text_color)| GtfsRoute {
                        id: format!("maas:route:{}", route_idx),
                        short_name,
                        long_name,
                        mode,
                        color,
                        text_color,
                    })
                    .collect(),
            })
            .collect())
    }
}

#[handler]
async fn graphiql() -> Html<String> {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

pub fn build_schema(
    graph: Arc<Graph>,
) -> Schema<QueryRoot, EmptyMutation, EmptySubscription> {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(graph)
        .finish()
}

pub async fn server(graph: Arc<Graph>, server_config: &ServerConfig) -> std::io::Result<()> {
    let schema = build_schema(graph);
    let app = Route::new()
        .at("/graphql", GraphQL::new(schema))
        .at("/graphiql", get(graphiql));

    let bind = format!("{}:{}", server_config.host, server_config.port);
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
        let (_, t) = parse_date_time(
            &Some("2025-01-01".to_string()),
            &Some("14:05".to_string()),
        )
        .unwrap();
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
