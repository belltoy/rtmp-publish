#![allow(dead_code)]
use std::fs::File;
use std::io::BufReader;
use std::time::Duration;
use std::sync::Arc;

use bytes::{Bytes, BufMut, BytesMut};
use bytecodec::io::{IoDecodeExt, IoEncodeExt};
use bytecodec::EncodeExt;

use futures::{
    future::{
        TryFutureExt,
    },
    stream::Stream,
};
use async_stream::{try_stream};

use flv_codec::*;
use rml_rtmp::{
    time::RtmpTimestamp,
};
use crate::PacketType;

pub async fn read_flv_tag(path: &str) -> Result<impl Stream<Item = std::io::Result<Arc<PacketType>>>, std::io::Error> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let input_file = File::open(path)?;
        let flv_stream: async_stream::AsyncStream<_, _> = try_stream! {
            let mut reader = BufReader::new(input_file);
            let mut decoder = flv_codec::FileDecoder::new();
            let mut last_ts = 0;
            while let Ok(tag) = decoder.decode_exact(&mut reader) {
                match tag {
                    Tag::Video(VideoTag { timestamp, .. }) => {
                        let mut data = Vec::new();
                        let mut tag_encoder = TagEncoder::with_item(tag.clone()).unwrap();
                        tag_encoder.encode_all(&mut data).unwrap();

                        let timestamp_value = timestamp.value() as u32;

                        let ts_delta = timestamp_value.checked_sub(last_ts).unwrap_or(0);
                        let timestamp = RtmpTimestamp::new(timestamp_value);
                        if last_ts <= 0 {
                            last_ts = timestamp_value;
                        }

                        if ts_delta > 300 {
                            sleep(ts_delta).await;
                            last_ts = timestamp_value;
                        }
                        let packet = PacketType::Video{ data: Bytes::from(data), ts: timestamp };
                        yield Arc::new(packet);
                    }
                    Tag::Audio(tag) => {
                        let sound_format = match tag.sound_format {
                            SoundFormat::Aac => 10,
                            _ => continue,
                        };
                        let sound_rate = match tag.sound_rate {
                            SoundRate::Khz5 => 0,
                            SoundRate::Khz11 => 1,
                            SoundRate::Khz22 => 2,
                            SoundRate::Khz44 => 3,
                        };
                        let sound_size = match tag.sound_size {
                            SoundSize::Bit8 => 0,
                            SoundSize::Bit16 => 1,
                        };
                        let sound_type = match tag.sound_type {
                            SoundType::Mono => 0,
                            SoundType::Stereo => 1,
                    };
                        let aac_packet_type = match tag.aac_packet_type {
                            Some(AacPacketType::SequenceHeader) => 0,
                            Some(AacPacketType::Raw) => 1,
                            None => continue,
                        };
                        let mut buf = BytesMut::with_capacity(tag.data.len() + 5);
                        let infos: u8 = ((sound_format as u8) << 4)
                            | ((sound_rate as u8) << 2)
                            | ((sound_size as u8) << 1)
                            | (sound_type as u8);
                        buf.put_u8(infos);
                        buf.put_u8(aac_packet_type);
                        buf.put_slice(&tag.data[..]);
                        let data: Bytes = buf.into();

                        let timestamp_value = tag.timestamp.value() as u32;
                        let ts_delta = timestamp_value.checked_sub(last_ts).unwrap_or(0);
                        let timestamp = RtmpTimestamp::new(timestamp_value);

                        if last_ts <= 0 {
                            last_ts = timestamp_value;
                        }

                        if ts_delta > 300 {
                            sleep(ts_delta).await;
                            last_ts = timestamp_value;
                        }
                        let packet = PacketType::Audio{ data, ts: timestamp};
                        yield Arc::new(packet);
                    }
                    Tag::ScriptData(s) => {
                        let timestamp_value = s.timestamp.value() as u32;
                        let ts_delta = timestamp_value.checked_sub(last_ts).unwrap_or(0);
                        // let timestamp = RtmpTimestamp::new(timestamp_value);
                        if last_ts <= 0 {
                            last_ts = timestamp_value;
                        }

                        let mut d = s.data.as_slice();
                        let mut data = rml_amf0::deserialize(&mut d).unwrap();
                        if data.len() == 2 {
                            let object = data.pop();
                            let key = data.pop();
                            match (key, object) {
                                (Some(rml_amf0::Amf0Value::Utf8String(s)), Some(rml_amf0::Amf0Value::Object(metadata_object))) if s == "onMetaData" => {
                                    let mut metadata = rml_rtmp::sessions::StreamMetadata::new();
                                    metadata.apply_metadata_values(metadata_object);
                                    if ts_delta > 300 {
                                        sleep(ts_delta).await;
                                        last_ts = timestamp_value;
                                    }
                                    let packet = PacketType::Metadata(Arc::new(metadata));
                                    yield Arc::new(packet);
                                }
                                _ => {
                                }
                            };
                        }
                    },
                }
            }
        };
        Ok(flv_stream)
    }).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "open input file error")
    }).await?
}

async fn sleep(ms_time: u32) {
    let delta = ms_time.checked_sub(100).unwrap_or(0);
    if delta > 0 {
        tokio::time::delay_for(Duration::from_millis(delta as u64)).await;
    }
}

pub fn is_video_sequence_header(data: &[u8]) -> bool {
    // This is assuming h264.
    return data.len() >= 2 && data[0] == 0x17 && data[1] == 0x00;
}

pub fn is_video_sequence_end(data: &[u8]) -> bool {
    // This is assuming h264.
    return data.len() >= 2 && data[0] == 0x17 && data[1] == 0x02;
}

pub fn is_audio_sequence_header(data: &[u8]) -> bool {
    // This is assuming aac
    return data.len() >= 2 && data[0] == 0xaf && data[1] == 0x00;
}

pub fn is_video_keyframe(data: &[u8]) -> bool {
    // assumings h264
    return data.len() >= 2 && data[0] == 0x17 && data[1] != 0x00; // 0x00 is the sequence header, don't count that for now
}

pub fn is_video_keyframe_or_header(data: &[u8]) -> bool {
    // assumings h264
    return data.len() >= 2 && data[0] == 0x17 && data[1] != 0x02; // 0x02 is the sequence end, don't count that for now
}
