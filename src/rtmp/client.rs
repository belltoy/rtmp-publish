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
    messages::MessagePayload,
    sessions::{
        ClientSession, ClientSessionConfig, ClientSessionError, ClientSessionEvent,
        ClientSessionResult, PublishRequestType,
    },
};

use crate::{
    error::{
        Error,
        ErrorKind,
    },
    PacketType,
    ReceivedType,
};

pub struct Client {
}

struct Session {
    app: String,
    stream: String,
    inner: ClientSession,
    notify_tx: Option<oneshot::Sender<()>>,
}

impl Client {
    pub async fn new(addr: String, port: u16, app: String, stream: String, broadcast_rx: broadcast::Receiver<Arc<PacketType>>) -> Self {
        let (notify_tx, notify_rx) = oneshot::channel();
        tokio::spawn(async move {
            match Self::connect(format!("{}:{}", addr, port)).await {
                Ok(transport) => {
                    let tc_url = format!("rtmp://{}:{}/{}", addr, port, app);
                    println!("[INFO] starting to push {}/{}", tc_url, stream);
                    Self::start_push(transport, broadcast_rx, notify_tx, app, stream, tc_url).await;
                }
                Err(e) => {
                    println!("[ERROR] connect to server error: {}", e);
                }
            }
        });

        let _ = notify_rx.await;
        Self {}
    }

    async fn connect<A: tokio::net::ToSocketAddrs>(addr: A) -> Result<Framed<TcpStream, super::codec::Codec>, Error> {
        let socket = TcpStream::connect(addr).await?;
        let io = super::handshake(socket).await?;
        Ok(super::codec::Codec::default().framed(io))
    }

    async fn start_push<T, S1, S2>(transport: Framed<T, super::codec::Codec>,
                                   broadcast_rx: broadcast::Receiver<Arc<PacketType>>,
                                   notify_tx: oneshot::Sender<()>,
                                   app: S1, stream: S2, tc_url: String)
        where S1: Into<String>,
              S2: Into<String>,
              T: AsyncRead + AsyncWrite + Send + 'static,
    {
        let (to_server, from_server) = transport.split();
        let (tx, rx) = futures::channel::mpsc::channel(16); // response to socket channel

        // write back to connection asynchronously
        tokio::spawn(async {
            let _ = rx.map(|r| Ok(r)).forward(to_server).await;
            println!("[DEBUG] publisher write end finished");
        });

        let broadcast_rx = broadcast_rx.into_stream().map_ok(|m| ReceivedType::Broadcast(m))
            .map_err(|_| {
                ErrorKind::Unknown("receive source broadcast error".into()).into()
            })
            .chain(stream::once(async { Err(ErrorKind::Unknown("session lost".into()).into()) }));

        let from_server = from_server
            .map_ok(|(message, bytes_read)| ReceivedType::FromClient{ message, bytes_read })
            .chain(stream::once(async { Err(ErrorKind::Unknown("connection lost".into()).into()) }));

        let rx = stream::select(from_server, broadcast_rx);

        start_reading(tx, rx, notify_tx, app.into(), stream.into(), tc_url).await;
    }
}

impl Session {
    fn new(app: String, stream: String, inner: ClientSession, notify_tx: oneshot::Sender<()>) -> Self {
        Self { app, stream, inner, notify_tx: Some(notify_tx) }
    }

    fn request_connect(&mut self, tc_url: String) -> Result<Packet, Error> {
        self.inner.request_connection(self.app.clone(), Some(tc_url)).map_err(From::from)
    }

    fn handle_broadcast(&mut self, send_type: Arc<PacketType>) -> Result<Packet, ClientSessionError> {
        match *send_type {
            PacketType::Audio{ ref data, ref ts, .. } => {
                self.inner.publish_audio_data(data.clone(), ts.clone(), false)
            }
            PacketType::Video{ ref data, ref ts, .. } => {
                self.inner.publish_video_data(data.clone(), ts.clone(), false)
            }
            PacketType::Metadata( ref metadata ) => {
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
            println!("[DEBUG] received unknown message: {:?}", msg);
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
                        return self.handle_push_connection_accepted_event().map(Some);
                    }
                    ConnectionRequestRejected{ description } => {
                        return Err(ErrorKind::Unknown(format!("connect request rejected by peer server: {}", description)).into());
                    }
                    PublishRequestAccepted => {
                        self.handle_push_publish_accepted_event();
                    }
                    x => {
                        println!("[WARN] Unknown event raised by peer server: {:?}", x);
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
        println!("[INFO] Publish accepted for push stream {}", self.stream);
        if let Some(notify_tx) = self.notify_tx.take() {
            let _ = notify_tx.send(());
        }
    }
}

async fn start_reading<T>(tx: futures::channel::mpsc::Sender<Packet>,
                          rx: T,
                          notify_tx: oneshot::Sender<()>,
                          app: String,
                          stream: String,
                          tc_url: String)
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

    let mut session = Session::new(app, stream, session, notify_tx);

    let packet = session.request_connect(tc_url).unwrap();
    requests.push(Ok(packet));
    let _ = tx.send_all(&mut stream::iter(requests)).await;
    let _ = rx.try_fold((tx, session), |(mut tx, mut session), received| async move {
        let to_send = match received {
            ReceivedType::FromClient{ message, bytes_read } => {
                session.handle_from_peer_server(message, bytes_read)
            }
            ReceivedType::Broadcast(received) => {
                let result = session.handle_broadcast(received).map_err(Error::from);
                vec![result]
            }
        };

        let _ = tx.send_all(&mut stream::iter(to_send)).await;
        Ok((tx, session))
    }).await;
}
