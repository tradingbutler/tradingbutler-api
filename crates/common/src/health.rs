use axum::{Router, http::StatusCode, routing::get};

/// A `GET /healthz` route that always returns `200 OK`. Merge into any
/// service's router so every binary exposes the same liveness check:
///
/// ```rust,ignore
/// let router = Router::new()
///     .route("/ws", get(ws_handler))
///     .merge(common::health::router())
///     .with_state(state);
/// ```
pub fn router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new().route("/healthz", get(healthz))
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}
