use std::sync::Arc;

use async_graphql::{
    Context, EmptyMutation, EmptySubscription, Error, Schema, http::GraphiQLSource,
};
use async_graphql_poem::{GraphQL, GraphQLSubscription};
use poem::{Result, Route, Server, get, handler, listener::TcpListener, web::Html};

use crate::{
    routing::routing::{RouteQuery, route},
    structures::{Graph, plan::Plan},
};

struct QueryRoot;

#[async_graphql::Object]
impl QueryRoot {
    async fn ping(&self) -> &str {
        "pong"
    }

    async fn plan(
        &self,
        ctx: &Context<'_>,
        from_lat: f64,
        from_lng: f64,
        to_lat: f64,
        to_lng: f64,
    ) -> Result<Plan, Error> {
        let graph = ctx.data::<Arc<Graph>>()?;

        let query = RouteQuery {
            from_lat,
            from_lng,
            to_lat,
            to_lng,
        };

        route(graph.as_ref(), &query)
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
