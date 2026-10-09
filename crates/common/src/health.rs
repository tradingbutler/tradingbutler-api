use axum::{Json, Router, http::StatusCode, routing::get};
use serde::Serialize;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// `GET /healthz` (always `200 OK`) and `GET /version` (`{"version": "…"}`,
/// same shape as `web/`'s and `admin/`'s). Merge into any service's router so
/// every binary exposes the same liveness check and version probe:
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
    Router::new()
        .route("/healthz", get(healthz))
        .route("/version", get(version))
}

async fn healthz() -> StatusCode {
    StatusCode::OK
}

#[derive(Serialize)]
struct Version {
    version: &'static str,
}

async fn version() -> Json<Version> {
    Json(Version {
        version: crate::VERSION,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(port: u16, path: &str) -> String {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }

    #[tokio::test]
    async fn serves_healthz_and_version() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move { axum::serve(listener, router::<()>()).await });

        check("127.0.0.1", port).await.unwrap();

        let response = get(port, "/version").await;
        assert!(response.starts_with("HTTP/1.1 200 "));
        assert!(response.contains("application/json"));
        assert!(response.ends_with(&format!("{{\"version\":\"{}\"}}", crate::VERSION)));
    }
}
