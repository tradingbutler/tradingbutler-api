use axum::{Router, http::StatusCode, routing::get};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

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

/// Client side of the check above: connect to `host:port` and issue
/// `GET /healthz`, succeeding only on a `200` status line. Backs the
/// `healthcheck` CLI subcommand every service exposes (see `common::cli`),
/// which lets a shell-less/curl-less container image (distroless) probe
/// itself: `HEALTHCHECK CMD ["/app/service", "healthcheck"]`.
///
/// `0.0.0.0`/`::` are treated as "bind, not connect" addresses and rewritten
/// to their loopback equivalents so a self-check against `HTTP_HOST=0.0.0.0`
/// (the default) actually connects.
pub async fn check(host: &str, port: u16) -> anyhow::Result<()> {
    let connect_host = match host {
        "0.0.0.0" => "127.0.0.1",
        "::" => "::1",
        other => other,
    };

    let response = tokio::time::timeout(Duration::from_secs(3), async {
        let mut stream = TcpStream::connect((connect_host, port)).await?;
        stream
            .write_all(
                format!(
                    "GET /healthz HTTP/1.1\r\nHost: {connect_host}\r\nConnection: close\r\n\r\n"
                )
                .as_bytes(),
            )
            .await?;

        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        anyhow::Ok(response)
    })
    .await??;

    let status_line = String::from_utf8_lossy(&response);
    let status_line = status_line.lines().next().unwrap_or_default();

    if status_line.contains(" 200 ") {
        Ok(())
    } else {
        anyhow::bail!("service at {connect_host}:{port} is unhealthy: {status_line:?}");
    }
}
