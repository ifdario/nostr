// Copyright (c) 2022-2023 Yuki Kishimoto
// Copyright (c) 2023-2025 Rust Nostr Developers
// Distributed under the MIT software license

//! WebSocket transport

use std::fmt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use nostr::types::Url;
#[cfg(target_arch = "wasm32")]
use yawc::WebSocket;
use yawc::WebSocketError;
use yawc::frame::Frame;
#[cfg(not(target_arch = "wasm32"))]
use yawc::{HttpRequest, Options, Proxy, WebSocket};

use crate::error::Error;
use crate::future::BoxedFuture;

#[cfg(not(target_arch = "wasm32"))]
const USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"));

/// Largest single frame payload accepted from a relay.
#[cfg(not(target_arch = "wasm32"))]
const MAX_PAYLOAD_READ: usize = 16 * 1024 * 1024;

/// Largest message accepted from a relay once its fragments are reassembled.
#[cfg(not(target_arch = "wasm32"))]
const MAX_READ_BUFFER: usize = 64 * 1024 * 1024;

/// WebSocket transport sink
pub type WebSocketSink = Pin<Box<dyn Sink<Frame, Error = Error> + Send>>;
/// WebSocket transport stream
pub type WebSocketStream = Pin<Box<dyn Stream<Item = Result<Frame, Error>> + Send>>;

#[doc(hidden)]
pub trait IntoWebSocketTransport {
    fn into_transport(self) -> Arc<dyn WebSocketTransport>;
}

impl IntoWebSocketTransport for Arc<dyn WebSocketTransport> {
    fn into_transport(self) -> Arc<dyn WebSocketTransport> {
        self
    }
}

impl<T> IntoWebSocketTransport for T
where
    T: WebSocketTransport + Sized + 'static,
{
    fn into_transport(self) -> Arc<dyn WebSocketTransport> {
        Arc::new(self)
    }
}

impl<T> IntoWebSocketTransport for Arc<T>
where
    T: WebSocketTransport + 'static,
{
    fn into_transport(self) -> Arc<dyn WebSocketTransport> {
        self
    }
}

/// WebSocket transport
pub trait WebSocketTransport: fmt::Debug + Send + Sync {
    /// Support ping/pong
    fn support_ping(&self) -> bool;

    /// Connect
    fn connect<'a>(
        &'a self,
        url: &'a Url,
        proxy: Option<SocketAddr>,
    ) -> BoxedFuture<'a, Result<(WebSocketSink, WebSocketStream), Error>>;
}

/// Default websocket transport
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DefaultWebsocketTransport;

impl WebSocketTransport for DefaultWebsocketTransport {
    fn support_ping(&self) -> bool {
        true
    }

    fn connect<'a>(
        &'a self,
        url: &'a Url,
        proxy: Option<SocketAddr>,
    ) -> BoxedFuture<'a, Result<(WebSocketSink, WebSocketStream), Error>> {
        Box::pin(async move {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let mut connection = WebSocket::connect(url.clone())
                    .with_options(
                        Options::default()
                            .with_limits(MAX_PAYLOAD_READ, MAX_READ_BUFFER)
                            .with_utf8(),
                    )
                    .with_request(HttpRequest::builder().header("user-agent", USER_AGENT));

                if let Some(proxy) = proxy {
                    // Resolve names at the proxy so `.onion` addresses work.
                    let url =
                        Url::parse(&format!("socks5h://{proxy}")).map_err(Error::transport)?;
                    connection = connection.with_proxy(Proxy::socks5(url)?);
                }

                Ok(split(Reporting::new(connection.await?)))
            }

            #[cfg(target_arch = "wasm32")]
            {
                // The browser dials on our behalf, so a proxy can't be applied here.
                let _ = proxy;
                Ok(split(WebSocket::connect(url.clone()).await?))
            }
        })
    }
}

fn split<T>(socket: T) -> (WebSocketSink, WebSocketStream)
where
    T: Sink<Frame, Error = WebSocketError>
        + Stream<Item = Result<Frame, WebSocketError>>
        + Send
        + Unpin
        + 'static,
{
    let (tx, rx) = socket.split();

    // NOTE: don't use sink_map_err here, as it may cause panics!
    // Issue: https://github.com/nostrdevkit/nostr/issues/984
    (
        Box::pin(TransportSink(tx)),
        Box::pin(rx.map_err(Error::transport)),
    )
}

/// Reports read errors that yawc's [`Stream`] implementation hides.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct Reporting<S> {
    socket: WebSocket<S>,
    failed: bool,
}

#[cfg(not(target_arch = "wasm32"))]
impl<S> Reporting<S> {
    pub(crate) fn new(socket: WebSocket<S>) -> Self {
        Self {
            socket,
            failed: false,
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<S> Stream for Reporting<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    type Item = Result<Frame, WebSocketError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.failed {
            return Poll::Ready(None);
        }

        let result = futures::ready!(this.socket.poll_next_frame(cx));
        this.failed = result.is_err();
        Poll::Ready(Some(result))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<S> Sink<Frame> for Reporting<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    type Error = WebSocketError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.socket.poll_ready_unpin(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        self.socket.start_send_unpin(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.socket.poll_flush_unpin(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.socket.poll_close_unpin(cx)
    }
}

struct TransportSink<S>(S);

impl<S> Sink<Frame> for TransportSink<S>
where
    S: Sink<Frame, Error = WebSocketError> + Unpin,
{
    type Error = Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready_unpin(cx).map_err(Error::from)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Frame) -> Result<(), Self::Error> {
        self.0.start_send_unpin(item).map_err(Error::from)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_flush_unpin(cx).map_err(Error::from)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_close_unpin(cx).map_err(Error::from)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::error::Error as StdError;
    use std::io;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    /// Read the HTTP request a client wrote, up to the blank line that ends its head.
    async fn read_request<S>(stream: &mut S) -> io::Result<String>
    where
        S: tokio::io::AsyncRead + Unpin,
    {
        let mut request = [0u8; 4096];
        let mut len = 0;

        while len < request.len() && !request[..len].ends_with(b"\r\n\r\n") {
            let read = stream.read(&mut request[len..]).await?;
            if read == 0 {
                break;
            }
            len += read;
        }

        String::from_utf8(request[..len].to_vec())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Serve one SOCKS5 CONNECT, then read whatever the client tunnels through it.
    ///
    /// Nothing answers the WebSocket handshake, so the client's connect fails. What the test
    /// cares about is the address the proxy was asked to reach and the bytes that followed.
    async fn socks5_connect(listener: TcpListener) -> io::Result<(String, String)> {
        let (mut stream, _) = listener.accept().await?;

        // Greeting: version, then the authentication methods the client offers.
        let mut greeting = [0u8; 2];
        stream.read_exact(&mut greeting).await?;
        assert_eq!(greeting[0], 0x05);
        let mut methods = vec![0u8; greeting[1] as usize];
        stream.read_exact(&mut methods).await?;

        // "No authentication required"
        stream.write_all(&[0x05, 0x00]).await?;

        // Request: version, command, reserved, address type.
        let mut request = [0u8; 4];
        stream.read_exact(&mut request).await?;
        assert_eq!(request[0], 0x05);
        assert_eq!(request[1], 0x01, "expected a CONNECT command");
        assert_eq!(
            request[3], 0x03,
            "expected a domain, left for the proxy to resolve"
        );

        let mut length = [0u8; 1];
        stream.read_exact(&mut length).await?;
        let mut domain = vec![0u8; length[0] as usize];
        stream.read_exact(&mut domain).await?;
        let mut port = [0u8; 2];
        stream.read_exact(&mut port).await?;

        let domain =
            String::from_utf8(domain).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let target = format!("{domain}:{}", u16::from_be_bytes(port));

        // Success, with a bound address of 0.0.0.0:0.
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0])
            .await?;

        Ok((target, read_request(&mut stream).await?))
    }

    #[tokio::test]
    async fn default_transport_dials_through_socks5_proxy() -> Result<(), Box<dyn StdError>> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let proxy = listener.local_addr()?;

        let server = tokio::spawn(socks5_connect(listener));

        // A name that never resolves locally, so reaching it proves the proxy resolved it.
        // This is what makes `.onion` addresses work.
        let url = Url::parse("ws://relay.invalid:8080")?;
        assert!(
            DefaultWebsocketTransport
                .connect(&url, Some(proxy))
                .await
                .is_err()
        );

        let (target, request) = server.await??;
        assert_eq!(target, "relay.invalid:8080");
        assert!(request.starts_with("GET / HTTP/1.1"), "{request}");
        assert!(
            request
                .lines()
                .any(|line| line.eq_ignore_ascii_case(&format!("user-agent: {USER_AGENT}")))
        );

        Ok(())
    }
}
