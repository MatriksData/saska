use error::{self, Error};
use bytes::{BufMut, BytesMut, Bytes};
use mqtt3::Packet;
use mqtt3::{self, MqttRead, MqttWrite};
use std::io::{self, ErrorKind, Cursor};
use std::sync::Arc;
use std::net::SocketAddr;
use load_balancer::OutBuf;
use tokio_io::codec::{Decoder, Encoder};
use tokio_core::net::{UdpCodec};
use worker::{SystemMessage, SubscribeInfo};

pub fn decode(buf: &mut Vec<u8>) -> Result<Option<Packet>, ()> {
    if buf.len() < 2 {
        return Ok(None);
    }

    let (packet, len) = {
        let mut buf_ref : &[u8]= (*buf).as_ref();
        match buf_ref.read_packet_with_len() {
            Err(e) => {
                if let mqtt3::Error::Io(e) = e {
                    match e.kind() {
                        ErrorKind::TimedOut | ErrorKind::WouldBlock => return Ok(None),
                        ErrorKind::UnexpectedEof => return Ok(None),
                        _ => {
                            error!("mqtt3 io error = {:?}", e);
                            return Err(())
                        },
                    }
                } else {
                    error!("mqtt3 read error = {:?}", e);
                    return Err(());
                }
            }
            Ok(v) => v,
        }
    };

    if buf.len() < len {
        return Ok(None);
    }

    *buf = buf.split_off(len);
    
    Ok(Some(packet))
}

pub fn serialize_packet(packet: &Packet) -> io::Result<OutBuf> {
    let mut serialized = Cursor::new(Vec::with_capacity(128));
    if let Err(e) = serialized.write_packet(packet) {
        error!("Encode error. Error = {:?}", e);
        return Err(io::Error::new(io::ErrorKind::Other, "Unable to encode!"));
    }
    let mut buf = Bytes::new();
    buf.extend(serialized.get_ref());
    Ok(buf)
}

pub fn accumulate_and_decode(x: Bytes, 
                             buffer:&mut Vec<u8>) -> Result<Option<mqtt3::Packet>, error::Error>{
                        
    buffer.extend(x);
    let packet;
    match decode(buffer){
        Ok(packet_opt) => {
            packet = packet_opt
        },
        Err(_) => {
            // Anlasilmayan bir paket varsa baglantiyi direk kes.
            return Err(error::Error::ParsingError);
        }
    }

    Ok(packet)
}

#[derive(Debug)]
pub struct NopCodec;

impl Decoder for NopCodec {
    type Item = Bytes;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, io::Error> {
        if buf.len() > 0 {
            let len = buf.len();
            Ok(Some(buf.split_to(len).freeze()))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for NopCodec {
    type Item = OutBuf;
    type Error = io::Error;

    fn encode(&mut self, data: OutBuf, buf: &mut BytesMut) -> Result<(), io::Error> {
        buf.extend_from_slice(data.as_ref());
        Ok(())
    }
}

pub struct SystemMessageUdpCodec{
    multicast_addr : SocketAddr
}

impl SystemMessageUdpCodec{
    pub fn new(multicast_addr : SocketAddr) -> SystemMessageUdpCodec{
        SystemMessageUdpCodec{
            multicast_addr
        }
    }
}

impl UdpCodec for SystemMessageUdpCodec {
    type In = (SystemMessage, String, String);
    type Out = (SystemMessage, String, String);

    fn decode(&mut self, src: &SocketAddr, buf: &[u8]) ->io::Result<Self::In>{
        let (system_message, system_message_size) = decode_system_message(buf);
        let broker_url_size = buf[system_message_size] as usize ;
        let broker_url : String = buf[(1 + system_message_size) .. (system_message_size + broker_url_size+1)].iter().map(|x| *x as char).collect();
        let broker_type_size = buf[system_message_size + broker_url_size + 1] as usize;
        let broker_type : String = buf[(system_message_size + broker_url_size + 2)..(system_message_size + broker_url_size + broker_type_size + 2)]
                                       .iter().map(|x| *x as char).collect();
        Ok((system_message, broker_url, broker_type))
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> SocketAddr {
        encode_system_message(msg.0, buf);
        buf.put_u8(msg.1.len() as u8);
        buf.extend(msg.1.as_bytes());
        buf.put_u8(msg.2.len() as u8);
        buf.extend(msg.2.as_bytes());
        self.multicast_addr.clone()
    }
}

const SYSTEM_MESSAGE_CONNECTED      : usize = 0;
const SYSTEM_MESSAGE_DISCONNECTED   : usize = 1;
const SYSTEM_MESSAGE_SUBSCRIPTION   : usize = 2;
const SYSTEM_MESSAGE_UNSUBSCRIPTION : usize = 3;

fn decode_system_message(buf: &[u8]) -> (SystemMessage, usize){
    let system_message_size = buf[0] as usize;
    let system_message_type = buf[1] as usize;
    match system_message_type {
        SYSTEM_MESSAGE_CONNECTED => {
            let user_name_size = system_message_size - 1;
            let user_name : String = buf[2..user_name_size+2].iter().map(|x| *x as char).collect();
            (SystemMessage::Connected(user_name), system_message_size + 1)
        },
        SYSTEM_MESSAGE_DISCONNECTED => {
            (SystemMessage::Disconnected, system_message_size + 1)
        },
        SYSTEM_MESSAGE_SUBSCRIPTION => {
            let subscription_count = vec_to_usize(&buf[2..6]);
            (SystemMessage::Subscribed(SubscribeInfo::Count(subscription_count)), 6)
        },
        SYSTEM_MESSAGE_UNSUBSCRIPTION => {
            let unsubscription_count = vec_to_usize(&buf[2..6]);
            (SystemMessage::Unsubscribed(unsubscription_count), 6)
        },
        _ => {
            unreachable!()
        }
    }
}

fn encode_system_message(system_message : SystemMessage, buf: &mut Vec<u8>) {
    match system_message {
        SystemMessage::Connected(user_name) => {
            let system_message_type = SYSTEM_MESSAGE_CONNECTED as u8;
            let system_message_size = (user_name.len() + 1) as u8;
            buf.put_u8(system_message_size);
            buf.put_u8(system_message_type);
            buf.extend(user_name.as_bytes());
        },
        SystemMessage::Disconnected => {
            let system_message_type = SYSTEM_MESSAGE_DISCONNECTED as u8;
            buf.put_u8(1);
            buf.put_u8(system_message_type);
        },
        SystemMessage::Subscribed(sub_info) => {
            let len = match sub_info{
                SubscribeInfo::Count(count) => count,
                SubscribeInfo::Topics(_, topics) => topics.len(),
            };
            let system_message_type = SYSTEM_MESSAGE_SUBSCRIPTION as u8;
            buf.put_u8(5);
            buf.put_u8(system_message_type);
            let b0 = ((len >> 24) & 0xFF) as u8;
            let b1 = ((len >> 16) & 0xFF) as u8;
            let b2 = ((len >>  8) & 0xFF) as u8;
            let b3 = ((len      ) & 0xFF) as u8;
            let k  = &[b3, b2, b1, b0];
            buf.extend(k);
        },
        SystemMessage::Unsubscribed(unsubscription_count) => {
            let system_message_type = SYSTEM_MESSAGE_UNSUBSCRIPTION as u8;
            buf.put_u8(5);
            buf.put_u8(system_message_type);
            let b0 = ((unsubscription_count >> 24) & 0xFF) as u8;
            let b1 = ((unsubscription_count >> 16) & 0xFF) as u8;
            let b2 = ((unsubscription_count >>  8) & 0xFF) as u8;
            let b3 = ((unsubscription_count      ) & 0xFF) as u8;
            let k  = &[b3, b2, b1, b0];
            buf.extend(k);
        },
    }
}

pub fn vec_to_usize(c: &[u8]) -> usize{
    let b0 = (c[0] as usize) <<  0;
    let b1 = (c[1] as usize) <<  8;
    let b2 = (c[2] as usize) << 16;
    let b3 = (c[3] as usize) << 24;
    b0 | b1 | b2 | b3
}

pub fn usize_to_vec(c: usize) -> Vec<u8>{
    let b0 = ((c >> 24) & 0xFF) as u8;
    let b1 = ((c >> 16) & 0xFF) as u8;
    let b2 = ((c >>  8) & 0xFF) as u8;
    let b3 = ((c      ) & 0xFF) as u8;
    vec![b0, b1, b2, b3]
}