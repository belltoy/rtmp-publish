use std::io;
use std::fmt::{self, Display};

use failure::{Backtrace, Context, Fail};
use rml_rtmp::{
    chunk_io::{ChunkSerializationError, ChunkDeserializationError},
    messages::{MessageSerializationError, MessageDeserializationError},
    sessions::{ServerSessionError, ClientSessionError},
};
use rml_rtmp::handshake::HandshakeError;

#[derive(Debug)]
pub struct Error {
    inner: Context<ErrorKind>
}

#[derive(Debug, Fail)]
pub enum ErrorKind {
    #[fail(display = "IO error: {}", _0)]
    Io(#[fail(cause)] io::Error),

    #[fail(display = "chunk serialization error: {}", _0)]
    Serialization(#[fail(cause)] ChunkSerializationError),

    #[fail(display = "chunk deserialization error: {}", _0)]
    Deserialization(#[fail(cause)] ChunkDeserializationError),

    #[fail(display = "message serialization error: {}", _0)]
    MessageSerialization(#[fail(cause)] MessageSerializationError),

    #[fail(display = "message deserialization error: {}", _0)]
    MessageDeserialization(#[fail(cause)] MessageDeserializationError),

    #[fail(display = "handshake error: {}", _0)]
    Handshake(#[fail(cause)] HandshakeError),

    #[fail(display = "server session error: {}", _0)]
    ServerSession(#[fail(cause)] ServerSessionError),

    #[fail(display = "outbound client session error: {}", _0)]
    ClientSession(#[fail(cause)] ClientSessionError),

    #[fail(display = "unknown error: {}", _0)]
    Unknown(String),
}

impl Fail for Error {
    fn cause(&self) -> Option<&dyn Fail> {
        self.inner.cause()
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        self.inner.backtrace()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Error {
        Error { inner: Context::new(kind) }
    }
}

impl From<Context<ErrorKind>> for Error {
    fn from(inner: Context<ErrorKind>) -> Error {
        Error { inner }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        ErrorKind::Io(e).into()
    }
}

impl From<ChunkDeserializationError> for Error {
    fn from(e: ChunkDeserializationError) -> Error {
        ErrorKind::Deserialization(e).into()
    }
}

impl From<ChunkSerializationError> for Error {
    fn from(e: ChunkSerializationError) -> Error {
        ErrorKind::Serialization(e).into()
    }
}

impl From<MessageSerializationError> for Error {
    fn from(e: MessageSerializationError) -> Error {
        ErrorKind::MessageSerialization(e).into()
    }
}

impl From<MessageDeserializationError> for Error {
    fn from(e: MessageDeserializationError) -> Error {
        ErrorKind::MessageDeserialization(e).into()
    }
}

impl From<ServerSessionError> for Error {
    fn from(e: ServerSessionError) -> Error {
        ErrorKind::ServerSession(e).into()
    }
}

impl From<ClientSessionError> for Error {
    fn from(e: ClientSessionError) -> Error {
        ErrorKind::ClientSession(e).into()
    }
}

impl From<HandshakeError> for Error {
    fn from(e: HandshakeError) -> Error {
        ErrorKind::Handshake(e).into()
    }
}
