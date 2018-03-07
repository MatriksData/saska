use std;
use futures::sync::mpsc::{Sender, Receiver};
use futures::sink::Sink;
use std::fmt;
use bytes::Bytes;
use tokio_io::codec::Framed;
use tokio_core::net::TcpStream;
use futures::stream::SplitSink;
use codec::NopCodec;
use std::rc::Rc;
use std::cell::RefCell;
use mqtt3::Packet;
use std::sync::Arc;
use error::Error;
use websocket;
use websocket::message::OwnedMessage;
use broker::SocketWriterHalf;


pub type OutBuf = Bytes;

pub type SocketWriterHalfRc = Rc<RefCell<SocketWriterHalf>>;

pub type SinkTcp       = SplitSink<Framed<TcpStream, NopCodec>>;
pub type SinkWebSocket = SplitSink<websocket::client::async::Framed<websocket::client::async::TcpStream, websocket::codec::ws::MessageCodec<websocket::message::OwnedMessage>>>;

pub type Writer = (Receiver<Packet>, SocketWriterHalf, Sender<()>);


pub enum LoadBalancingStrategy{
    SocketCount
}


pub struct LoadBalancer{
    senders : Vec<Sender<Writer>>,

    socket_counts : Vec<usize>,

    strategy : LoadBalancingStrategy,
}

impl LoadBalancer{
    pub fn new(senders: Vec<Sender<Writer>>,
               strategy: LoadBalancingStrategy) -> LoadBalancer{
        let socket_counts = {
            let mut sc = Vec::with_capacity(senders.len());
            for _ in 0..senders.len(){
                sc.push(0);
            }
            sc
        };
        LoadBalancer {
            senders,
            socket_counts,
            strategy
        }
    }

    pub fn connect(&mut self, writer: Writer) -> usize{
        // 1) en dusuk yogunluktaki threadin indexini bul 
        let index = match self.strategy{
            LoadBalancingStrategy::SocketCount => {
                self.find_lowest_density()
            }
        };

        // 2) send channels to thread
        self.senders[index].start_send(writer).expect("Load Balancer: Sending channels error!");
        self.senders[index].poll_complete();
        self.socket_counts[index] += 1;
        index
    }

    pub fn disconnect(&mut self, i: usize){
        self.socket_counts[i] -= 1;
    }

    // TODO Better name 
    fn find_lowest_density(&self) -> usize {
        let mut min = self.socket_counts[0];
        let mut index = 0;
        for (i, c) in self.socket_counts.iter().enumerate().skip(1){
            if *c < min{
                index = i;
                min = *c;
            }
        }
        index
    }
}

impl fmt::Display for LoadBalancer{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, sc) in self.socket_counts.iter().enumerate(){
            write!(f, "Worker No: {}, Socket Count: {} \n", i, sc).unwrap();
        }
        Ok(())
    }
}


