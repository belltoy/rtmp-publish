mod handshake;
pub mod client;
mod codec;

pub use self::handshake::handshake;

const HANDSHAKE_PACKET_SIZE: usize = 1536;
#[allow(dead_code)]
const C0C1_SIZE: usize = HANDSHAKE_PACKET_SIZE + 1;
#[allow(dead_code)]
const C2_SIZE: usize = HANDSHAKE_PACKET_SIZE;
const S0S1S2_SIZE: usize = HANDSHAKE_PACKET_SIZE * 2 + 1;
