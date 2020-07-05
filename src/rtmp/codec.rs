use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};
use bytes::buf::BufMut;

use rml_rtmp::{
    chunk_io::Packet,
    messages::{MessagePayload, RtmpMessage},
};
use rml_rtmp::chunk_io::ChunkDeserializer;
use crate::error::Error;

pub struct Codec {
    de: ChunkDeserializer,
}

impl Default for Codec {
    fn default() -> Self {
        Self {
            de: ChunkDeserializer::new(),
        }
    }
}

impl Decoder for Codec {
    type Item = (MessagePayload, usize);
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let before = src.len();
        let result = self.de.get_next_message(src)?;
        let bytes_read = before - src.len();

        let result = if let Some(payload) = result {
            if payload.type_id == 1 {
                if let RtmpMessage::SetChunkSize{ size } = payload.to_rtmp_message()? {
                    self.de.set_max_chunk_size(size as usize)?;
                }
            }
            Some((payload, bytes_read))
        } else {
            None
        };

        Ok(result)
    }
}

impl Encoder<Packet> for Codec {
    type Error = Error;

    fn encode(&mut self, item: Packet, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if dst.remaining_mut() < item.bytes.len() {
            dst.reserve(item.bytes.len());
        }
        dst.put(item.bytes.as_slice());

        Ok(())
    }
}

