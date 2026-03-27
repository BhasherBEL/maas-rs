use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Schema, SimpleObject,
    http::GraphiQLSource,
};
use async_graphql_poem::{GraphQL, GraphQLSubscription};
use chrono::{Local, NaiveDate, NaiveTime};
use poem::{Result, Route, Server, get, handler, listener::TcpListener, web::Html};

use crate::{
    routing::{routing_astar, routing_raptor},
    structures::{Graph, plan::Plan},
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

struct QueryRoot;

#[async_graphql::Object]
impl QueryRoot {
    async fn ping(&self) -> &str {
        "pong"
    }

    async fn astar(
        &self,
        ctx: &Context<'_>,
        from_lat: f64,
        from_lng: f64,
        to_lat: f64,
        to_lng: f64,
        date: Option<String>,
        time: Option<String>,
    ) -> Result<Plan, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;

        let now = Local::now().naive_local();

        let parsed_date = match date {
            Some(ref d) => NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map_err(|e| Error::new(format!("Invalid date '{}': {}", d, e)))?,
            None => now.date(),
        };

        let parsed_time = match time {
            Some(ref t) => NaiveTime::parse_from_str(t, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M"))
                .map_err(|e| Error::new(format!("Invalid time '{}': {}", t, e)))?,
            None => now.time(),
        };

        let query = routing_astar::RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
            date: parsed_date,
            time: parsed_time,
        };

        routing_astar::route(graph.as_ref(), &query)
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
    ) -> Result<Vec<Plan>, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;

        let now = Local::now().naive_local();

        let parsed_date = match date {
            Some(ref d) => NaiveDate::parse_from_str(d, "%Y-%m-%d")
                .map_err(|e| Error::new(format!("Invalid date '{}': {}", d, e)))?,
            None => now.date(),
        };

        let parsed_time = match time {
            Some(ref t) => NaiveTime::parse_from_str(t, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(t, "%H:%M"))
                .map_err(|e| Error::new(format!("Invalid time '{}': {}", t, e)))?,
            None => now.time(),
        };

        let query = routing_raptor::RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
            date: parsed_date,
            time: parsed_time,
        };

        routing_raptor::route(graph.as_ref(), &query)
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
    Html(
        GraphiQLSource::build()
            .endpoint("/graphql")
            .subscription_endpoint("/ws")
            .finish(),
    )
}

pub async fn server(graph: Arc<Graph>) -> std::io::Result<()> {
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(graph)
        .finish();
    let app = Route::new()
        .at("/graphql", GraphQL::new(schema.clone()))
        .at("/ws", GraphQLSubscription::new(schema))
        .at("/graphiql", get(graphiql));

    println!("Serving on 0.0.0.0:3000");
    Server::new(TcpListener::bind("0.0.0.0:3000"))
        .run(app)
        .await
}
