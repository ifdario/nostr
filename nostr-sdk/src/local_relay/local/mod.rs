// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nostr::event::Event;
use nostr::filter::Filter;
use nostr::types::RelayUrl;
use nostr_database::SaveEventStatus;
use tokio::io::{AsyncRead, AsyncWrite};

mod inner;
mod session;
mod util;

use self::inner::InnerLocalRelay;
use super::builder::LocalRelayBuilder;
use crate::client::{Output, RelayUrlArg, SyncSummary};
use crate::error::Error;
use crate::relay::SyncOptions;

/// A local nostr relay
///
/// This is automatically shutdown when all instances/clones are dropped!
///
/// If a session falls behind the bounded live event buffer, its active live
/// subscriptions receive `CLOSED`. Clients can resubscribe to recover stored
/// events; ephemeral events and events only passed to [`Self::notify_event`]
/// cannot be recovered from the relay database.
#[derive(Debug)]
pub struct LocalRelay {
    inner: InnerLocalRelay,
    // Keep track of the atomic reference count to know when shutdown the relay.
    atomic_counter: Arc<AtomicUsize>,
}

impl Clone for LocalRelay {
    fn clone(&self) -> Self {
        self.atomic_counter.fetch_add(1, Ordering::SeqCst);

        Self {
            inner: self.inner.clone(),
            atomic_counter: self.atomic_counter.clone(),
        }
    }
}

impl Drop for LocalRelay {
    fn drop(&mut self) {
        // Shutdown exactly once when the last handle is dropped.
        if self.atomic_counter.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.shutdown();
        }
    }
}

impl Default for LocalRelay {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl LocalRelay {
    /// Create a new local relay with the default configuration.
    ///
    /// Use [`LocalRelay::builder`] for customizing it!
    #[inline]
    pub fn new() -> Self {
        Self::builder().build()
    }

    /// Create a new local relay builder
    #[inline]
    pub fn builder() -> LocalRelayBuilder {
        LocalRelayBuilder::default()
    }

    #[inline]
    pub(super) fn from_builder(builder: LocalRelayBuilder) -> Self {
        Self {
            inner: InnerLocalRelay::new(builder),
            atomic_counter: Arc::new(AtomicUsize::new(1)),
        }
    }

    /// Run the local relay
    #[inline]
    pub async fn run(&self) -> Result<(), Error> {
        self.inner.run().await?;
        Ok(())
    }

    /// Get url
    #[inline]
    pub async fn url(&self) -> RelayUrl {
        self.inner.url().await
    }

    /// Returns the remaining connection capacity before the limit is reached.
    #[inline]
    pub fn connections_left(&self) -> usize {
        self.inner.connections_limit.available_permits()
    }

    /// Sync events with other relay(s).
    #[inline]
    pub async fn sync_with<'a, I, U>(
        &self,
        urls: I,
        filter: Filter,
        opts: SyncOptions,
    ) -> Result<Output<SyncSummary>, Error>
    where
        I: IntoIterator<Item = U>,
        U: Into<RelayUrlArg<'a>>,
    {
        self.inner.sync_with(urls, filter, opts).await
    }

    /// Send event to subscribers
    ///
    /// Return `true` if the event is successfully sent.
    ///
    /// This method doesn't save the event into the database!
    /// It's intended to be used ONLY when the database is shared with other apps (i.e. with the nostr-sdk `Client`).
    pub fn notify_event(&self, event: Event) -> bool {
        if event.verify().is_err() {
            return false;
        }

        self.inner.notify_event(event)
    }

    /// Save the event to the database and, if success, notify the subscribers.
    pub async fn add_event(&self, event: Event) -> Result<SaveEventStatus, Error> {
        event.verify()?;

        let status = self.inner.save_event(&event).await?;

        if status.is_success() {
            self.inner.notify_event(event);
        }

        Ok(status)
    }

    /// Shutdown relay
    #[inline]
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    /// Pass an already upgraded stream
    pub async fn take_connection<S>(&self, stream: S, addr: SocketAddr) -> Result<(), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.inner.handle_upgraded_connection(stream, addr).await
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    use futures::{SinkExt, StreamExt};
    use negentropy::{Negentropy, NegentropyStorageVector};
    use nostr::event::{EventBuilder, FinalizeEvent, Kind};
    use nostr::filter::Filter;
    use nostr::key::Keys;
    use nostr::message::{MachineReadablePrefix, RelayMessage};
    use nostr::types::Url;
    use tokio::time;
    use yawc::frame::{Frame, OpCode};
    use yawc::{TcpWebSocket, WebSocket};

    use super::*;
    use crate::local_relay::{QueryPolicy, QueryPolicyResult};

    async fn connect(relay: &LocalRelay) -> TcpWebSocket {
        relay.run().await.unwrap();
        let url = Url::parse(relay.url().await.as_str()).unwrap();
        WebSocket::connect(url).await.unwrap()
    }

    async fn next_relay_message(socket: &mut TcpWebSocket) -> RelayMessage<'static> {
        let frame = socket
            .next()
            .await
            .expect("WebSocket connection terminated");
        assert_eq!(frame.opcode(), OpCode::Text);
        RelayMessage::from_json(frame.payload()).unwrap()
    }

    #[derive(Debug)]
    struct RejectQueries;

    impl QueryPolicy for RejectQueries {
        fn admit_query<'a>(
            &'a self,
            _query: &'a mut Filter,
            _addr: &'a SocketAddr,
        ) -> Pin<Box<dyn Future<Output = QueryPolicyResult> + Send + 'a>> {
            Box::pin(async {
                QueryPolicyResult::reject(MachineReadablePrefix::Blocked, "query rejected")
            })
        }
    }

    #[tokio::test]
    async fn add_event_rejects_unverified_events() {
        let relay = LocalRelay::new();
        let keys = Keys::generate();
        let mut event = EventBuilder::new(Kind::TextNote, "original")
            .finalize(&keys)
            .unwrap();
        event.content = String::from("forged");

        let err = relay.add_event(event).await.unwrap_err();
        assert_eq!(err.kind(), crate::error::ErrorKind::Protocol);
    }

    #[test]
    fn notify_event_rejects_unverified_events() {
        let relay = LocalRelay::new();
        let keys = Keys::generate();
        let mut event = EventBuilder::new(Kind::TextNote, "original")
            .finalize(&keys)
            .unwrap();
        event.content = String::from("forged");

        assert!(!relay.notify_event(event));
    }

    #[tokio::test]
    async fn test_malformed_client_message_does_not_close_connection() {
        let relay = LocalRelay::new();

        let mut socket = connect(&relay).await;

        socket
            .send(Frame::text(
                r#"["REQ","short-author",{"authors":["deadbeef"]}]"#,
            ))
            .await
            .unwrap();
        socket
            .send(Frame::text(r#"["REQ","valid",{}]"#))
            .await
            .unwrap();

        time::timeout(Duration::from_secs(1), async {
            let mut received_notice = false;

            loop {
                match next_relay_message(&mut socket).await {
                    RelayMessage::Notice(..) => received_notice = true,
                    RelayMessage::EndOfStoredEvents(subscription_id)
                        if subscription_id.as_str() == "valid" =>
                    {
                        assert!(received_notice);
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("timed out waiting for EOSE");
    }

    #[tokio::test]
    async fn test_malformed_negentropy_messages_do_not_close_connection() {
        let relay = LocalRelay::new();

        let mut socket = connect(&relay).await;

        socket
            .send(Frame::text(r#"["NEG-OPEN","neg-odd",{},"abc"]"#))
            .await
            .unwrap();
        socket
            .send(Frame::text(r#"["NEG-OPEN","neg-nonhex",{},"zz"]"#))
            .await
            .unwrap();
        socket
            .send(Frame::text(r#"["NEG-MSG","neg-msg","abc"]"#))
            .await
            .unwrap();
        socket
            .send(Frame::text(r#"["REQ","valid",{}]"#))
            .await
            .unwrap();

        time::timeout(Duration::from_secs(1), async {
            let mut neg_errors: usize = 0;

            loop {
                match next_relay_message(&mut socket).await {
                    RelayMessage::NegErr { message, .. } => {
                        assert_eq!(message, "error: invalid negentropy message");
                        neg_errors += 1;
                    }
                    RelayMessage::EndOfStoredEvents(subscription_id)
                        if subscription_id.as_str() == "valid" =>
                    {
                        assert_eq!(neg_errors, 3);
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await
        .expect("timed out waiting for EOSE");
    }

    #[tokio::test]
    async fn test_invalid_neg_msg_terminates_only_the_negentropy_subscription() {
        let relay = LocalRelay::new();

        let mut socket = connect(&relay).await;

        // Open a valid negentropy subscription
        let mut storage = NegentropyStorageVector::new();
        storage.seal().unwrap();
        let mut negentropy = Negentropy::owned(storage, 60_000).unwrap();
        let initial_message = faster_hex::hex_string(&negentropy.initiate().unwrap());
        socket
            .send(Frame::text(format!(
                r#"["NEG-OPEN","neg",{{}},"{initial_message}"]"#
            )))
            .await
            .unwrap();

        let reply = next_relay_message(&mut socket).await;
        assert!(matches!(reply, RelayMessage::NegMsg { .. }));

        // A malformed payload must terminate only this negentropy subscription
        socket
            .send(Frame::text(r#"["NEG-MSG","neg","zz"]"#))
            .await
            .unwrap();

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "neg"
                && message == "error: invalid negentropy message"
        ));

        // The subscription is gone, but the connection keeps serving requests
        socket
            .send(Frame::text(r#"["NEG-MSG","neg","6100"]"#))
            .await
            .unwrap();

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "neg"
                && message == "error: subscription not found"
        ));

        socket
            .send(Frame::text(r#"["REQ","valid",{}]"#))
            .await
            .unwrap();

        let eose = next_relay_message(&mut socket).await;
        assert!(matches!(
            eose,
            RelayMessage::EndOfStoredEvents(subscription_id)
                if subscription_id.as_str() == "valid"
        ));
    }

    #[tokio::test]
    async fn test_nip42_read_auth_is_required_for_count() {
        let relay = LocalRelay::builder()
            .nip42(crate::local_relay::LocalRelayBuilderNip42::read())
            .build();

        let mut socket = connect(&relay).await;
        socket
            .send(Frame::text(r#"["COUNT","count",{}]"#))
            .await
            .unwrap();

        let auth = next_relay_message(&mut socket).await;
        assert!(matches!(auth, RelayMessage::Auth { .. }));

        let closed = next_relay_message(&mut socket).await;
        assert!(matches!(
            closed,
            RelayMessage::Closed {
                subscription_id,
                message,
            } if subscription_id.as_str() == "count"
                && message.starts_with("auth-required:")
        ));
    }

    #[tokio::test]
    async fn test_nip42_read_auth_is_required_for_negentropy() {
        let relay = LocalRelay::builder()
            .nip42(crate::local_relay::LocalRelayBuilderNip42::read())
            .build();

        let mut socket = connect(&relay).await;
        socket
            .send(Frame::text(r#"["NEG-OPEN","neg",{},""]"#))
            .await
            .unwrap();

        let auth = next_relay_message(&mut socket).await;
        assert!(matches!(auth, RelayMessage::Auth { .. }));

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "neg"
                && message.starts_with("auth-required:")
        ));
    }

    #[tokio::test]
    async fn test_query_policy_is_applied_to_count_and_negentropy() {
        let relay = LocalRelay::builder().query_policy(RejectQueries).build();

        let mut socket = connect(&relay).await;
        socket
            .send(Frame::text(r#"["COUNT","count",{}]"#))
            .await
            .unwrap();

        let closed = next_relay_message(&mut socket).await;
        assert!(matches!(
            closed,
            RelayMessage::Closed {
                subscription_id,
                message,
            } if subscription_id.as_str() == "count"
                && message == "blocked: query rejected"
        ));

        socket
            .send(Frame::text(r#"["NEG-OPEN","neg",{},""]"#))
            .await
            .unwrap();

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "neg"
                && message == "blocked: query rejected"
        ));
    }

    #[tokio::test]
    async fn test_negentropy_subscription_limit() {
        let relay = LocalRelay::builder()
            .max_negentropy_subscriptions(0)
            .build();

        let mut socket = connect(&relay).await;
        socket
            .send(Frame::text(r#"["NEG-OPEN","neg",{},""]"#))
            .await
            .unwrap();

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "neg"
                && message.starts_with("rate-limited:")
        ));
    }

    #[tokio::test]
    async fn test_subscription_id_limit_counts_utf8_bytes() {
        let relay = LocalRelay::builder().max_subid_length(3).build();

        let mut socket = connect(&relay).await;
        socket
            .send(Frame::text(r#"["NEG-OPEN","éé",{},""]"#))
            .await
            .unwrap();

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "éé"
                && message == "blocked: subscription ID exceeds max length 3"
        ));
    }

    #[tokio::test]
    async fn test_negentropy_item_limit() {
        let relay = LocalRelay::builder().max_negentropy_items(2).build();
        let keys = Keys::generate();
        for i in 0..3 {
            relay
                .add_event(
                    EventBuilder::new(Kind::TextNote, i.to_string())
                        .finalize(&keys)
                        .unwrap(),
                )
                .await
                .unwrap();
        }

        let mut socket = connect(&relay).await;
        socket
            .send(Frame::text(r#"["NEG-OPEN","neg",{},""]"#))
            .await
            .unwrap();

        let neg_err = next_relay_message(&mut socket).await;
        assert!(matches!(
            neg_err,
            RelayMessage::NegErr {
                subscription_id,
                message,
            } if subscription_id.as_str() == "neg"
                && message == "rate-limited: too many negentropy items"
        ));
    }

    #[tokio::test]
    async fn test_default_message_limit_allows_301_protocol_frames() {
        let relay = LocalRelay::new();

        let mut socket = connect(&relay).await;

        time::timeout(Duration::from_secs(5), async {
            for _ in 0..301 {
                socket
                    .send(Frame::text(r#"["CLOSE","harmless"]"#))
                    .await
                    .unwrap();
            }
            socket
                .send(Frame::text(r#"["REQ","valid",{}]"#))
                .await
                .unwrap();

            let eose = next_relay_message(&mut socket).await;
            assert!(matches!(
                eose,
                RelayMessage::EndOfStoredEvents(subscription_id)
                    if subscription_id.as_str() == "valid"
            ));
        })
        .await
        .expect("timed out waiting for EOSE after 301 harmless frames");
    }

    #[tokio::test]
    async fn test_default_message_limit_closes_rapid_burst_over_6000_frames() {
        let relay = LocalRelay::new();

        let mut socket = connect(&relay).await;

        time::timeout(Duration::from_secs(5), async {
            for _ in 0..6_001 {
                socket
                    .send(Frame::text(r#"["CLOSE","harmless"]"#))
                    .await
                    .unwrap();
            }

            let closed = socket.next().await;
            assert!(closed.is_none_or(|frame| frame.opcode() == OpCode::Close));
        })
        .await
        .expect("connection did not close after 6,001 rapid frames");
    }

    #[tokio::test]
    async fn test_configured_message_limit_is_exact_for_binary_frames() {
        let relay = LocalRelay::builder().messages_per_minute(3).build();

        let mut socket = connect(&relay).await;

        time::timeout(Duration::from_secs(5), async {
            for byte in 1..=3 {
                socket.send(Frame::binary(vec![byte])).await.unwrap();
                let notice = next_relay_message(&mut socket).await;
                assert!(matches!(notice, RelayMessage::Notice(..)));
            }

            socket.send(Frame::binary(vec![4])).await.unwrap();
            let closed = socket.next().await;
            assert!(closed.is_none_or(|frame| frame.opcode() == OpCode::Close));
        })
        .await
        .expect("connection did not enforce the configured three-frame limit");
    }

    #[tokio::test]
    async fn test_shutdown() {
        let relay = LocalRelay::new();

        assert!(!relay.inner.is_running());

        relay.run().await.unwrap();

        time::sleep(Duration::from_secs(1)).await;

        assert!(relay.inner.is_running());

        relay.shutdown();

        time::sleep(Duration::from_millis(100)).await;

        assert!(!relay.inner.is_running());
    }

    #[tokio::test]
    async fn test_shutdown_on_drop() {
        let inner: InnerLocalRelay = {
            let relay: LocalRelay = LocalRelay::new();

            assert!(!relay.inner.is_running());

            relay.run().await.unwrap();

            time::sleep(Duration::from_secs(1)).await;

            assert!(relay.inner.is_running());

            // Clone the inner relay
            let inner: InnerLocalRelay = relay.inner.clone();

            {
                let r2: LocalRelay = relay.clone();
                tokio::spawn(async move {
                    assert_eq!(r2.atomic_counter.load(Ordering::SeqCst), 2);

                    time::sleep(Duration::from_secs(1)).await;

                    // r2 dropped here
                });
            }

            time::sleep(Duration::from_secs(2)).await;

            assert_eq!(relay.atomic_counter.load(Ordering::SeqCst), 1);

            inner
        }; // relay dropped here

        time::sleep(Duration::from_secs(1)).await;

        assert!(!inner.is_running());
    }
}
