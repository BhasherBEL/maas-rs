use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Schema, http::GraphiQLSource,
};
use async_graphql_poem::{GraphQL, GraphQLSubscription};
use chrono::{Local, NaiveDate, NaiveTime};
use poem::{Result, Route, Server, get, handler, listener::TcpListener, web::Html};

use crate::{
    routing::{routing_astar, routing_raptor},
    structures::{Graph, plan::Plan},
};

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

    println!("Serving on 127.0.0.1:3000");
    Server::new(TcpListener::bind("127.0.0.1:3000"))
        .run(app)
        .await
}
