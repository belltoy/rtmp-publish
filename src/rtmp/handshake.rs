use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use rml_rtmp::handshake::{Handshake as RtmpHandshake, PeerType, HandshakeProcessResult};
use crate::error::{Error, ErrorKind};

pub async fn handshake<T>(mut io: T) -> Result<T, Error>
    where T: AsyncRead + AsyncWrite + Unpin
{
    let mut handshake = RtmpHandshake::new(PeerType::Client);
    let c0_and_c1 = handshake.generate_outbound_p0_and_p1()?;
    io.write_all(c0_and_c1.as_slice()).await?;
    io.flush().await?;
    let mut buf = [0; super::S0S1S2_SIZE];
    io.read_exact(&mut buf).await?;
    let response_bytes = match handshake.process_bytes(&buf)? {
        HandshakeProcessResult::InProgress{ .. } => {
            return Err(ErrorKind::Unknown("unknow error in handshake".into()).into());
        }
        HandshakeProcessResult::Completed{ response_bytes, .. } => {
            response_bytes
        }
    };
    io.write_all(response_bytes.as_slice()).await?;
    io.flush().await?;
    Ok(io)
}
