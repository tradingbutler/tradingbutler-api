use async_stream::try_stream;
use futures_util::Stream;
use redis::{
    AsyncCommands, AsyncConnectionConfig, Client, RedisError,
    aio::{ConnectionManager, ConnectionManagerConfig, MultiplexedConnection},
    streams::{StreamId, StreamMaxlen, StreamReadOptions, StreamReadReply},
};

/// Build a multiplexed connection suitable for blocking reads (XREAD BLOCK).
///
/// redis 1.3 applies a 500ms default response timeout to multiplexed connections,
/// which aborts blocking commands prematurely. Disabling it lets `BLOCK` run to its
/// server-side timeout. Retries with a 1s backoff until a connection is established.
async fn blocking_connection(client: &Client) -> MultiplexedConnection {
    let config = AsyncConnectionConfig::new().set_response_timeout(None);
    loop {
        match client
            .get_multiplexed_async_connection_with_config(&config)
            .await
        {
            Ok(conn) => return conn,
            Err(_) => tokio::time::sleep(std::time::Duration::from_secs(1)).await,
        }
    }
}

/// Where in the stream a reader should start.
pub enum StreamPosition {
    /// Replay all existing entries, then continue with new ones.
    Beginning,
    /// Skip existing entries; only receive entries added after this call.
    NewOnly,
    /// Resume from a specific entry ID (exclusive — the entry with this ID is not re-delivered).
    After(String),
}

impl StreamPosition {
    fn into_id(self) -> String {
        match self {
            Self::Beginning => "0".to_owned(),
            Self::NewOnly => "$".to_owned(),
            Self::After(id) => id,
        }
    }
}

#[derive(Clone)]
pub struct RedisService {
    client: Client,
    conn: ConnectionManager,
}

impl RedisService {
    pub async fn new(url: &str) -> Result<Self, RedisError> {
        let client = Client::open(url)?;
        let config = ConnectionManagerConfig::new().set_response_timeout(None);
        let conn = ConnectionManager::new_with_config(client.clone(), config).await?;
        Ok(Self { client, conn })
    }

    /// Append an entry to a stream. Returns the auto-generated entry ID.
    /// If `max_len` is set the stream is trimmed to approximately that many entries.
    pub async fn xadd(
        &mut self,
        stream: &str,
        fields: &[(&str, &str)],
        max_len: Option<usize>,
    ) -> Result<String, RedisError> {
        match max_len {
            Some(n) => {
                self.conn
                    .xadd_maxlen(stream, StreamMaxlen::Approx(n), "*", fields)
                    .await
            }
            None => self.conn.xadd(stream, "*", fields).await,
        }
    }

    /// Returns an async `Stream` that continuously yields entries from `stream_key`.
    ///
    /// Uses a dedicated connection with `XREAD BLOCK` so the underlying
    /// `ConnectionManager` stays free for concurrent `xadd` / `xread` calls.
    /// The stream ends when a connection error occurs; callers should handle
    /// the `Err` item and decide whether to restart.
    ///
    /// # Example
    /// ```rust,ignore
    /// use futures_util::StreamExt;
    ///
    /// let mut reader = svc.stream_reader("rates", StreamPosition::NewOnly);
    /// while let Some(result) = reader.next().await {
    ///     let entry = result?;
    ///     println!("{}: {:?}", entry.id, entry.map);
    /// }
    /// ```
    pub fn stream_reader<K: Into<String>>(
        &self,
        stream_key: K,
        position: StreamPosition,
    ) -> impl Stream<Item = Result<StreamId, RedisError>> + use<K> {
        let client = self.client.clone();
        let stream_key = stream_key.into();
        let initial_id = position.into_id();

        try_stream! {
            let mut last_id = initial_id;
            let opts = StreamReadOptions::default().block(5_000).count(100);

            loop {
                let mut conn = blocking_connection(&client).await;

                loop {
                    match conn.xread_options::<&str, &str, StreamReadReply>(&[stream_key.as_str()], &[last_id.as_str()], &opts).await {
                        Ok(reply) => {
                            for key in reply.keys {
                                for entry in key.ids {
                                    last_id = entry.id.clone();
                                    yield entry;
                                }
                            }
                        }
                        Err(_) => break, // reconnect on hard error
                    }
                }
            }
        }
    }

    /// Create a consumer group on `stream` if it does not already exist.
    ///
    /// Call once at startup before spawning `group_reader` instances.
    /// `start_from` controls the earliest entry new consumers will receive:
    /// - `Beginning` → process all existing entries in the stream
    /// - `NewOnly`   → only entries added after the group is created
    pub async fn ensure_consumer_group(
        &mut self,
        stream: &str,
        group: &str,
        start_from: StreamPosition,
    ) -> Result<(), RedisError> {
        let id = start_from.into_id();
        let result: Result<redis::Value, RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(stream)
            .arg(group)
            .arg(&id)
            .arg("MKSTREAM")
            .query_async(&mut self.conn)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) if e.code() == Some("BUSYGROUP") => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Returns an async `Stream` that participates in a consumer group, enabling
    /// load-balanced delivery across multiple instances.
    ///
    /// Each entry is delivered to exactly one consumer in the group. On startup,
    /// it first re-delivers any pending (unacknowledged) entries left over from
    /// a previous crash, then switches to blocking reads for new entries.
    ///
    /// Call [`ack`] after successfully processing each entry.
    ///
    /// # Example — 3 balanced instances
    /// ```rust,ignore
    /// // At startup (once, shared across instances):
    /// svc.ensure_consumer_group("rates", "writers", StreamPosition::NewOnly).await?;
    ///
    /// // Each instance uses a unique consumer name:
    /// let mut feed = svc.group_reader("rates", "writers", "instance-1");
    /// while let Some(result) = feed.next().await {
    ///     let entry = result?;
    ///     process(&entry).await;
    ///     svc.ack("rates", "writers", &[&entry.id]).await?;
    /// }
    /// ```
    pub fn group_reader<K: Into<String>, G: Into<String>, C: Into<String>>(
        &self,
        stream_key: K,
        group: G,
        consumer: C,
    ) -> impl Stream<Item = Result<StreamId, RedisError>> + use<K, G, C> {
        let client = self.client.clone();
        let stream_key = stream_key.into();
        let group = group.into();
        let consumer = consumer.into();

        try_stream! {
            let pending_opts = StreamReadOptions::default()
                .group(&group, &consumer)
                .count(100);
            let new_opts = StreamReadOptions::default()
                .group(&group, &consumer)
                .block(5_000)
                .count(100);

            loop {
                let mut conn = blocking_connection(&client).await;

                // Re-deliver entries that were delivered but never acknowledged
                // (e.g. from a previous crash). Page through with cursor "0".
                let mut pending_cursor = "0".to_owned();
                loop {
                    let reply: StreamReadReply = match conn
                        .xread_options(&[stream_key.as_str()], &[pending_cursor.as_str()], &pending_opts)
                        .await
                    {
                        Ok(reply) => reply,
                        Err(_) => break,
                    };

                    let mut got_any = false;
                    for key in reply.keys {
                        for entry in key.ids {
                            pending_cursor = entry.id.clone();
                            got_any = true;
                            yield entry;
                        }
                    }
                    if !got_any {
                        break;
                    }
                }

                // ">" means "not yet delivered to any consumer in this group".
                loop {
                    match conn
                        .xread_options::<&str, &str, StreamReadReply>(&[stream_key.as_str()], &[">"], &new_opts)
                        .await
                    {
                        Ok(reply) => {
                            for key in reply.keys {
                                for entry in key.ids {
                                    yield entry;
                                }
                            }
                        }
                        Err(_) => break, // reconnect on hard error
                    }
                }
            }
        }
    }

    /// Acknowledge processed entries so Redis removes them from the pending list.
    pub async fn ack(
        &mut self,
        stream: &str,
        group: &str,
        ids: &[&str],
    ) -> Result<usize, RedisError> {
        self.conn.xack(stream, group, ids).await
    }

    /// Execute multiple commands in a single round trip.
    pub async fn pipeline<F>(&mut self, build: F) -> Result<(), RedisError>
    where
        F: FnOnce(&mut redis::Pipeline),
    {
        let mut pipe = redis::pipe();
        build(&mut pipe);
        pipe.query_async(&mut self.conn).await
    }

    /// Store a string value at `key`.
    pub async fn set(&mut self, key: &str, value: &str) -> Result<(), RedisError> {
        self.conn.set(key, value).await
    }

    /// Store multiple fields in a hash at `key`.
    pub async fn hset(&mut self, key: &str, fields: &[(&str, &str)]) -> Result<(), RedisError> {
        self.conn.hset_multiple(key, fields).await
    }

    /// Delete one or more keys.
    pub async fn del(&mut self, key: &str) -> Result<(), RedisError> {
        self.conn.del(key).await
    }

    /// Return all fields and values of a hash.
    pub async fn hgetall(
        &mut self,
        key: &str,
    ) -> Result<std::collections::HashMap<String, String>, RedisError> {
        self.conn.hgetall(key).await
    }

    /// Return all keys matching `pattern`.
    pub async fn keys(&mut self, pattern: &str) -> Result<Vec<String>, RedisError> {
        self.conn.keys(pattern).await
    }

    /// Expose the raw client for cases that need a custom connection.
    pub fn client(&self) -> &Client {
        &self.client
    }
}
