use std::sync::Arc;
use tokio::prelude::*;
use tokio::sync::{
    broadcast,
    oneshot,
};
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Framed};
use futures::{
    stream::{
        self,
        Stream,
        StreamExt,
        TryStreamExt,
    },
    sink::{
        SinkExt,
    },
};

use rml_rtmp::{
    chunk_io::Packet,
    messages::{
        MessagePayload,
        RtmpMessage,
        PeerBandwidthLimitType,
    },
    sessions::{
        ClientSession, ClientSessionConfig, ClientSessionError, ClientSessionEvent,
        ClientSessionResult, PublishRequestType,
    },
};
use slog::{
    o,
    trace, debug, info, warn, error,
    Logger,
};

use crate::{
    flv,
    error::{
        Error,
        ErrorKind,
    },
    PacketType,
    ReceivedType,
};

#[derive(Debug)]
pub struct Client {
}

struct Session {
    app: String,
    stream: String,
    inner: ClientSession,
    ready: bool,
    logger: Logger,
}

impl Client {
    pub async fn new(addr: String, port: u16, app: String, stream: String, broadcast_rx: broadcast::Receiver<Arc<PacketType>>, logger: &Logger) -> Self {
        let logger = logger.new(o!("app" => app.clone(), "stream" => stream.clone()));
        let (notify_tx, notify_rx) = oneshot::channel();
        let (buffer_tx, buffer_rx) = futures::channel::mpsc::channel(8);

        tokio::spawn(async move {
            broadcast_rx.into_stream()
                .map(|r| async { r })
                .buffered(8)
                .map_err(|_e| format!("Receive broadcast error") )
                .forward(buffer_tx.sink_map_err(|_e| format!("Buffer sender error"))).await
        });

        let logger_inner = logger.clone();
        tokio::spawn(async move {
            match Self::connect(format!("{}:{}", addr, port)).await {
                Ok(transport) => {
                    let tc_url = format!("rtmp://{}:{}/{}", addr, port, app);
                    info!(logger_inner, "starting to push {}/{}", tc_url, stream);
                    Self::start_push(transport, buffer_rx, notify_tx, app, stream, tc_url, logger_inner.clone()).await;
                }
                Err(e) => {
                    error!(logger_inner, "connect to server error: {}", e);
                }
            }
        });

        if let Err(e) = notify_rx.await {
            error!(logger, "notify_rx error"; "error" => %e);
        }
        Self {}
    }

    async fn connect<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Framed<TcpStream, super::codec::Codec>, Error> {
        let socket = TcpStream::connect(addr).await?;
        let io = super::handshake(socket).await?;
        Ok(super::codec::Codec::default().framed(io))
    }

    async fn start_push<T, S1, S2>(transport: Framed<T, super::codec::Codec>,
                                   buffer_rx: futures::channel::mpsc::Receiver<Arc<PacketType>>,
                                   notify_tx: oneshot::Sender<()>,
                                   app: S1, stream: S2, tc_url: String,
                                   logger: Logger)
        where S1: Into<String>,
              S2: Into<String>,
              T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (to_server, from_server) = transport.split();
        let (tx, rx) = futures::channel::mpsc::channel(8); // response to socket channel

        // write back to connection asynchronously
        let logger_inner = logger.clone();
        tokio::spawn(async move {
            let rs = rx.map(|r| Ok(r)).forward(to_server).await;
            match rs {
                Ok(_) => warn!(logger_inner, "Publisher write end finished"),
                Err(e) => error!(logger_inner, "Publisher write error"; "error" => %e),
            };
        });

        let broadcast_rx = buffer_rx
            .map(|m| Ok::<_, Error>(ReceivedType::Broadcast(m)))
            .map_err(|e| {
                let err_msg = format!("Receive source broadcast error: {}", e);
                ErrorKind::Unknown(err_msg).into()
            })
            .chain(stream::once(async { Err(ErrorKind::Unknown("session lost".into()).into()) }));

        let from_server = from_server
            .map_ok(|(message, bytes_read)| ReceivedType::FromClient{ message, bytes_read })
            .chain(stream::once(async { Err(ErrorKind::Unknown("connection lost".into()).into()) }));

        let reading_rx = stream::select(from_server, broadcast_rx);

        start_reading(tx, reading_rx, notify_tx, app.into(), stream.into(), tc_url, logger).await;
    }
}

impl Session {
    fn new(app: String, stream: String, inner: ClientSession, logger: &Logger) -> Self {
        Self { app, stream, inner, ready: false, logger: logger.clone() }
    }

    fn request_connect(&mut self, tc_url: String) -> Result<Packet, Error> {
        self.inner.request_connection(self.app.clone(), Some(tc_url)).map_err(From::from)
    }

    fn handle_broadcast(&mut self, send_type: Arc<PacketType>) -> Result<Packet, ClientSessionError> {
        match *send_type {
            PacketType::Audio{ ref data, ref ts, .. } => {
                if flv::is_audio_sequence_header(data) {
                    debug!(self.logger, "Send audio sequence header")
                }
                self.inner.publish_audio_data(data.clone(), ts.clone(), false)
            }
            PacketType::Video{ ref data, ref ts, .. } => {
                if flv::is_video_sequence_header(data) {
                    debug!(self.logger, "Send video sequence header");
                }
                self.inner.publish_video_data(data.clone(), ts.clone(), false)
            }
            PacketType::Metadata( ref metadata ) => {
                debug!(self.logger, "Send metadata");
                self.inner.publish_metadata(&metadata)
            }
        }
    }

    fn handle_from_peer_server(&mut self, message: MessagePayload, bytes_read: usize) -> Vec<Result<Packet, Error>> {
        let (outbounds, event, unknown) = match self.inner.handle_input_message(message, bytes_read) {
            Ok(v) => v,
            Err(e) => {
                return vec![Err(Error::from(e))];
            }
        };

        if let Some(msg) = unknown {
            match msg.to_rtmp_message() {
                Ok(RtmpMessage::SetPeerBandwidth{ size, limit_type }) => {
                    let limit_type = match limit_type {
                        PeerBandwidthLimitType::Dynamic => "Dynamic",
                        PeerBandwidthLimitType::Hard => "Hard",
                        PeerBandwidthLimitType::Soft => "Soft",
                    };
                    trace!(self.logger, "Ignore SetPeerBandwidth"; "size" => size, "limit_type" => limit_type);
                }
                Ok(_) => {
                    debug!(self.logger, "Received unknown message: {:?}", msg);
                }
                Err(e) => {
                    error!(self.logger, "Parse received packet error: {:?}", e);
                }
            }
        }

        // handle raised event
        let mut outbounds = outbounds.into_iter().map(Ok).collect::<Vec<_>>();
        match self.handle_raised_event(event) {
            Ok(None) => (),
            Ok(Some(v)) => {
                let v = v.into_iter().map(Ok);
                outbounds.extend(v);
            }
            Err(e) => {
                outbounds.push(Err(e));
            }
        };

        outbounds
    }

    fn handle_raised_event(&mut self, event: Option<ClientSessionEvent>) -> Result<Option<Vec<Packet>>, Error> {
        use self::ClientSessionEvent::*;

        match event {
            None => (),
            Some(event) => {
                match event {
                    ConnectionRequestAccepted => {
                        debug!(self.logger, "Connect request accepted");
                        return self.handle_push_connection_accepted_event().map(Some);
                    }
                    ConnectionRequestRejected{ description } => {
                        debug!(self.logger, "Connect request rejected");
                        return Err(ErrorKind::Unknown(format!("connect request rejected by peer server: {}", description)).into());
                    }
                    PublishRequestAccepted => {
                        debug!(self.logger, "Publish request accepted");
                        self.handle_push_publish_accepted_event();
                    }
                    AcknowledgementReceived{ bytes_received } => {
                        trace!(self.logger, "Ack received: {:?}", bytes_received);
                    }
                    UnhandleableAmf0Command{ command_name, .. } if command_name == "onFCPublish" => {
                        debug!(self.logger, "Received onFCPublish");
                    }
                    UnhandleableAmf0Command{ command_name, .. } if command_name == "onBWDone" => {
                        debug!(self.logger, "Received onBWDone");
                    }
                    x => {
                        warn!(self.logger, "Unknown event raised by peer server: {:?}", x);
                    }
                }
            }
        }

        Ok(None)
    }

    fn handle_push_connection_accepted_event(&mut self) -> Result<Vec<Packet>, Error> {
        let outbounds = self.inner.request_publishing(self.stream.clone(), PublishRequestType::Live)?;
        Ok(outbounds)
    }

    fn handle_push_publish_accepted_event(&mut self) {
        if !self.ready {
            self.ready = true;
        }
    }
}

async fn start_reading<T>(tx: futures::channel::mpsc::Sender<Packet>,
                          rx: T,
                          notify_tx: oneshot::Sender<()>,
                          app: String,
                          stream: String,
                          tc_url: String,
                          logger: Logger)
    where
        T: Stream<Item = Result<ReceivedType, Error>> + Send + 'static,
{
    let mut tx = tx.sink_map_err(|_| {
        ErrorKind::Unknown("send response error".into()).into()
    });
    // maybe create push cilent config from configs
    let (session, session_results) = ClientSession::new(ClientSessionConfig::new()).unwrap();
    let mut requests = session_results.into_iter().filter_map(|result| {
        match result {
            ClientSessionResult::OutboundResponse(packet) => {
                Some(Ok(packet))
            }
            _ => None,
        }
    }).collect::<Vec<_>>();

    let mut session = Session::new(app, stream, session, &logger);

    let packet = session.request_connect(tc_url).unwrap();
    requests.push(Ok(packet));
    if let Err(e) = tx.send_all(&mut stream::iter(requests)).await {
        error!(logger, "Send request to server error"; "error" => %e);
    }
    let result = rx.try_fold((tx, session, Some(notify_tx), logger.clone()), |(mut tx, mut session, notify_tx, logger), received| async move {
        let to_send = match received {
            ReceivedType::FromClient{ message, bytes_read } => {
                session.handle_from_peer_server(message, bytes_read)
            }
            ReceivedType::Broadcast(received) => {
                let result = session.handle_broadcast(received).map_err(Error::from);
                vec![result]
            }
        };

        if let Err(e) = tx.send_all(&mut stream::iter(to_send)).await {
            error!(logger, "Send to server response error"; "error" => %e);
        }
        let notify_tx = match (notify_tx, session.ready) {
            (Some(notify_tx), true) => {
                info!(logger, "Publish accepted for push stream");
                if let Err(_) = notify_tx.send(()) {
                    error!(logger, "Notify error");
                }
                None
            }
            (Some(notify_tx), false) => {
                Some(notify_tx)
            }
            (None, _) => {
                None
            }
        };
        Ok((tx, session, notify_tx, logger))
    }).await;
    match result {
        Ok((_tx, _session, _, _logger)) => {
            info!(logger, "Reading broadcast done");
        }
        Err(e) => {
            error!(logger, "Reading broadcast error"; "error" => %e);
        }
    }
}
