use std;
use std::fmt::{self, Debug};
use std::net::SocketAddr;
use std::cell::{RefCell, Cell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use futures::sync::mpsc::Sender;
use futures::{Future, Sink};
use futures::unsync;
use radix_trie::Trie;
use mqtt3::*;
use bytes::Bytes;
use broker::Message;
use load_balancer::{OutBuf, SocketWriterHalfRc};
use broker::{SocketWriterHalf};
use subscription::{Subscription};
use error::Error;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
}

#[derive(Debug)]
pub struct State {
    pub status: ConnectionStatus,
    pub last_control_at: Instant,
    pub last_pkid: PacketIdentifier
}

impl State {
    pub fn new() -> Self {
        State {
            status: ConnectionStatus::Connected,
            last_control_at: Instant::now(),
            last_pkid: PacketIdentifier(0)
        }
    }

    pub fn clear(&mut self) {
        self.last_pkid = PacketIdentifier(0);
    }
}

#[derive(Debug)]
pub struct Client {
    pub user_name       : String,
    pub packet_sender   : unsync::mpsc::Sender<Message>,
    pub keep_alive      : Option<Duration>,       
    pub clean_session   : bool,
    pub last_will       : Option<LastWill>,
    pub state           : State,
    pub channel_len     : Rc<Cell<usize>>,
    pub conn_closer     : Option<unsync::oneshot::Sender<Error>>,
    pub subscriptions   : Vec<String>,
} 

impl Client {
    pub fn new(packet_sender    : unsync::mpsc::Sender<Message>, 
               conn_closer      : unsync::oneshot::Sender<Error>,
               channel_len      : Rc<Cell<usize>>) -> Client {

        let state = State::new();

        Client {
            user_name     : String::new(),
            packet_sender : packet_sender,
            keep_alive    : None,
            clean_session : true,
            last_will     : None,
            state         : state,
            conn_closer   : Some(conn_closer),
            channel_len   : channel_len,
            subscriptions : vec![]
        }
    }

    pub fn set_keep_alive(&mut self, t: u16) {
        if t == 0 {
            self.keep_alive = Some(Duration::new(300, 0));
        } else {
            self.keep_alive = Some(Duration::new(t as u64, 0));
        }
    }

    pub fn set_persisent_session(&mut self) {
        self.clean_session = false;
    }

    pub fn set_lastwill(&mut self, will: LastWill) {
        self.last_will = Some(will);
    }

    pub fn end_task(&mut self, err: Error) {
        self.state.status = ConnectionStatus::Disconnected;
        self.conn_closer.take().unwrap().send(err).expect("Client: end_task start_send error");
    } 
    
    pub fn lastwill_publish(&self) -> Option<Publish> {
        if let Some(ref last_will) = self.last_will {
            Some(
                Publish {
                    dup: false,
                    qos: last_will.qos,
                    retain: last_will.retain,
                    topic_name: last_will.topic.clone(),
                    pid: None,
                    // TODO: Optimize the clone here
                    payload: Arc::new(last_will.message.clone().into_bytes())
                }
            )
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        self.state.clear();
    }

    pub fn status(&self) -> ConnectionStatus {
        self.state.status
    }

    pub fn set_disconnected(&mut self) {
        self.state.status = ConnectionStatus::Disconnected;
    }

    pub fn is_connected(&self) -> bool{
        self.state.status == ConnectionStatus::Connected
    }

    pub fn set_status(&mut self, s: ConnectionStatus) {
        self.state.status = s;
    }

    // reset the last control packet received time
    pub fn reset_last_control_at(&mut self) {
        self.state.last_control_at = Instant::now();
    }

    pub fn get_channel_len(&self) -> usize{
        self.channel_len.get()
    }

    pub fn increment_channel_len(&mut self) {
        let l = self.channel_len.get() + 1;
        self.channel_len.set(l); 
    }

    // check when the last control packet/pingreq packet
    // is received and return the status which tells if
    // keep alive time has exceeded
    // NOTE: status will be checked for zero keepalive times also
    pub fn has_exceeded_keep_alive(&self) -> bool {
        let last_control_at = self.state.last_control_at;

        if let Some(keep_alive) = self.keep_alive  {
            let keep_alive = keep_alive.as_secs();
            let keep_alive = Duration::new(f32::ceil(1.5 * keep_alive as f32) as u64, 0);
            last_control_at.elapsed() > keep_alive
        } else {
            true
        }
    }

    #[inline(always)]
    pub fn send(&mut self, packet: Message, channel_size : usize) -> std::result::Result<(), Error>{
        if self.get_channel_len() < channel_size{
            let res = self.packet_sender.start_send(packet); // AsyncSink= Result< As, Error>
            if let Err(_) = res {
                error!("Returning WriterError");
                return Err(Error::WriterError);
            } else {
                if res.unwrap().is_not_ready(){
                    warn!("Couldnt send packet!(Possibly due to busy channel)");
                } else {
                    self.increment_channel_len();
                }
                return Ok(());
            }
        } else {
            return Ok(())
        }

    }

    pub fn save_subscription(&mut self, path: String){
        self.subscriptions.push(path);
    }

    pub fn delete_subscription(&mut self, paths: &[String]){
        self.subscriptions = self.subscriptions.iter().filter(|x| !paths.contains(*x)).map(|x| x.clone()).collect::<Vec<String>>();
    }
}

pub enum SlotState<T>{
    Reserved,
    Some(T),
    None
}

pub struct Clients{
    pub heap : Vec<SlotState<Client>>
}

impl Clients{
    pub fn with_capacity(cap: usize) -> Clients {
        let heap = {
            let mut v = Vec::with_capacity(cap);
            for _ in 0..cap{
                v.push(SlotState::None);
            }
            v
        };

        Clients{
            heap
        }
    }

    pub fn len(&self) -> usize {
        let mut len  = 0;
        for i in self.heap.iter(){
            if let &SlotState::Some(_) = i {
                len += 1;
            }
        }
        len
    }

    pub fn get_empty_client(&mut self) -> usize{
        for (i, slot) in self.heap.iter_mut().enumerate(){
            if let &mut SlotState::None = slot{
                *slot = SlotState::Reserved;
                return i;
            }
        }

        let client_index = self.heap.len();

        for _ in 0..10{
            self.heap.push(SlotState::None);
        }

        self.heap[client_index] = SlotState::Reserved;
        client_index
    }

    pub fn delete_empty_client(&mut self, ind: usize) {
        self.heap[ind] = SlotState::None;
    }


    // TODO daha guzel olabilirdi
    pub fn add_client(&mut self, client: Client) -> usize {
        for (i, slot) in self.heap.iter_mut().enumerate(){
            if let &mut SlotState::None = slot{
                *slot = SlotState::Some(client);
                return i;
            }
        }

        let ind = self.heap.len();

        for _ in 0..10{
            self.heap.push(SlotState::None);
        }

        self.heap[ind] = SlotState::Some(client);

        ind
    }

    pub fn remove_client(&mut self, ind: usize) -> std::result::Result<(), ()> {
        if let SlotState::Some(_) = self.heap[ind]{
            self.heap[ind] = SlotState::None;
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn get_client(&self, client_index: usize) -> Option<&Client> {
        match self.heap[client_index] {
            SlotState::Reserved | SlotState::None=> {
                None
            },
            SlotState::Some(ref c) => {
                Some(c)
            },
        }
    }

    pub fn get_client_mut(&mut self, client_index: usize) -> Option<&mut Client> {
        match self.heap[client_index] {
            SlotState::Reserved | SlotState::None=> {
                None
            },
            SlotState::Some(ref mut c) => {
                Some(c)
            },
        }
    }

    pub fn get_client_index_with_user_name(&self, user_name : &str) -> Option<usize> {
        for (ind, o) in self.heap.iter().enumerate(){
            if let &SlotState::Some(ref c) = o {
                if c.user_name == user_name {
                    return Some(ind);
                }
            }
        }
        None
    }

    pub fn is_client_connected(&self, ind: usize) -> bool{
        self.get_client(ind).map_or(false, |c| c.state.status == ConnectionStatus::Connected)
    }

    pub fn insert_client(&mut self, client_index: usize, mut client: Client, user_name: String) {
        client.state.status = ConnectionStatus::Connected;
        client.user_name = user_name;
        self.heap[client_index] = SlotState::Some(client);
    }
}
