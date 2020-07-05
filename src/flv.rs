#![allow(dead_code)]
use std::fs::File;
use std::io::{
    BufReader,
    Seek,
    SeekFrom,
};
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
use slog::{
    info, error,
    Logger,
};
use async_stream::{try_stream};

use flv_codec::*;
use rml_rtmp::{
    time::RtmpTimestamp,
};
use crate::PacketType;

pub async fn read_flv_tag(path: &str, repeat: bool, logger: Logger) -> Result<impl Stream<Item = std::io::Result<Arc<PacketType>>>, std::io::Error> {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || {
        let input_file = File::open(path)?;
        let mut reader = BufReader::new(input_file);
        let flv_stream: async_stream::AsyncStream<_, _> = try_stream! {
            let mut metadata_sent = false;
            let mut video_seq_header_sent = false;
            let mut audio_seq_header_sent = false;
            let mut base_ts = 0;
            let mut last_ts = 0;
            'outter: loop {
                let position = reader.seek(SeekFrom::Start(0))?;
                let mut decoder = flv_codec::FileDecoder::new();
                'inner: loop {
                    let tag = match decoder.decode_exact(&mut reader) {
                        Ok(tag) => Some(tag),
                        Err(ref e) if *e.kind() == bytecodec::ErrorKind::DecoderTerminated => {
                            base_ts = last_ts;
                            break 'inner;
                        }
                        Err(e) => {
                            Err(std::io::Error::new(std::io::ErrorKind::Other, format!("{}", e)))?;
                            None
                        }
                    };
                    let tag = tag.unwrap();

                    match tag {
                        Tag::Video(mut video_tag) => {
                            let is_vsh = video_tag.avc_packet_type.map(|t| t == flv_codec::AvcPacketType::SequenceHeader).unwrap_or(false);
                            if !video_seq_header_sent && is_vsh {
                                video_seq_header_sent = true;
                            } else if is_vsh {
                                continue;
                            }

                            let timestamp = video_tag.timestamp;

                            let mut timestamp_value = timestamp.value() as u32;
                            timestamp_value += base_ts;

                            let ts_delta = timestamp_value.checked_sub(last_ts).unwrap_or(0);
                            let timestamp = RtmpTimestamp::new(timestamp_value);
                            if last_ts <= 0 {
                                last_ts = timestamp_value;
                            }

                            if ts_delta > 300 {
                                sleep(ts_delta).await;
                                last_ts = timestamp_value;
                            }

                            video_tag.timestamp = Timestamp::new(timestamp_value as i32);
                            let mut data = Vec::new();
                            let mut tag_encoder = TagEncoder::with_item(Tag::Video(video_tag)).unwrap();
                            tag_encoder.encode_all(&mut data).unwrap();

                            let packet = PacketType::Video{ data: Bytes::from(data), ts: timestamp };
                            yield Arc::new(packet);
                        }
                        Tag::Audio(tag) => {
                            let is_ash = tag.aac_packet_type.map(|t| t == flv_codec::AacPacketType::SequenceHeader).unwrap_or(false);
                            if !audio_seq_header_sent && is_ash {
                                audio_seq_header_sent = true;
                            } else if is_ash {
                                continue;
                            }

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

                            let mut timestamp_value = tag.timestamp.value() as u32;
                            timestamp_value += base_ts;
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
                            let mut timestamp_value = s.timestamp.value() as u32;
                            timestamp_value += base_ts;
                            let ts_delta = timestamp_value.checked_sub(last_ts).unwrap_or(0);
                            // let timestamp = RtmpTimestamp::new(timestamp_value);
                            if last_ts <= 0 {
                                last_ts = timestamp_value;
                            }

                            let mut d = s.data.as_slice();
                            let mut data = rml_amf0::deserialize(&mut d).unwrap();
                            if metadata_sent {
                                continue;
                            }
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
                                        metadata_sent = true;
                                        yield Arc::new(packet);
                                    }
                                    _ => {
                                    }
                                };
                            }
                        },
                    }
                }

                if  !repeat {
                    break;
                }
                info!(logger, "Loop flv from beginning");
            }
        };
        Ok(flv_stream)
    }).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::NotFound, "open input file error")
    }).await?
}

async fn sleep(delta: u32) {
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
