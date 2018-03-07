use client::{Client, ConnectionStatus, Clients};
use codec::serialize_packet;
use mqtt3::{self, Packet, Connect, Connack, 
            Publish, Subscribe, Suback, 
            SubscribeReturnCodes, Topic, ConnectReturnCode,
            QoS, Unsubscribe, PacketIdentifier};
use futures::{Sink,Future, StartSend, Poll, AsyncSink};
use futures::sync::mpsc::Sender;
use futures::unsync;
use bytes::Bytes;
use futures;
use rand;
use rand::Rng;
use error::Error;
use subscription::{Subscription};
use load_balancer::{OutBuf, SinkTcp, SinkWebSocket};
use plugin_interface::{BrokerInterface, PluginObject, AuthzFut, AuthnFut};
use error;
use std;
use std::rc::Rc;
use std::sync::Arc;
use std::cell::{RefCell, Cell};
use std::time::Duration;
use tokio_core::reactor::{Handle};
use config::Config;
use worker::{SystemMessage, SystemMessageSender, PublishSender, SubscribeInfo};
use std::fmt;
use websocket::message::OwnedMessage;

pub struct Broker<T: Subscription>{
    worker_no   : usize,
    config      : Rc<Config>,
    clients     : Clients,
    publish_br  : PublishSender,
    subs        : T,
    handle      : Handle,
    system_message_senders: Vec<unsync::mpsc::Sender<SystemMessage>>,
}

impl <T>Broker<T> where T: Subscription{
    pub fn new(worker_no   : usize,
               handle      : Handle,
               config      : Rc<Config>,
               senders     : Vec<Sender<Arc<Publish>>>,
               system_message_senders: Vec<unsync::mpsc::Sender<SystemMessage>>) -> Broker<T>{
        Broker{
            worker_no,
            handle,
            clients     : Clients::with_capacity(1000),
            config      : config,
            publish_br  : senders,
            subs        : T::new(),
            system_message_senders : system_message_senders
        }
    }

    pub fn get_empty_client(&mut self) -> usize{
        self.clients.get_empty_client()
    }

    pub fn delete_empty_client(&mut self, client_index: usize) {
        self.clients.delete_empty_client(client_index);
    }

    pub fn broadcast_publish_to_other_threads(&mut self, packet: Publish) {
        let packet = Arc::new(packet);
        for p in self.publish_br.iter_mut(){
            p.start_send(packet.clone());
            p.poll_complete();
        }
    }
    
    pub fn broadcast_connected_message_to_other_threads(&mut self, user_name: String) {
        self.broadcast_system_message(SystemMessage::Connected(user_name.clone()));
    }

    pub fn broadcast_system_message(&mut self, system_message: SystemMessage){
        for system_message_sender in self.system_message_senders.iter_mut(){
            system_message_sender.start_send(system_message.clone());
        }
    }

    pub fn handle_connect(&mut self,
                          client_index : usize,
                          connect      : &Connect,
                          client       : Client,
                          user_name    : String) -> Result<(), Error> {

        let clean_session = connect.clean_session;
        let session_exists = self.add_client(client_index,
                                             connect,
                                             client,
                                             user_name)?;
        let connack = if clean_session {
            Connack {
                session_present: false,
                code: ConnectReturnCode::Accepted,
            }
        } else {
            Connack {
                session_present: session_exists,
                code: ConnectReturnCode::Accepted,
            }
        };

        let packet_serialized = serialize_packet(&Packet::Connack(connack)).expect("Serialize error!");

        let client = self.clients.get_client_mut(client_index).expect("Client bulunamadi!");
        client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size())?;

        Ok(())
    }

    pub fn auth_rejected(&self, client : &mut Client){
        let connack = Connack {
            session_present: false,
            code: ConnectReturnCode::NotAuthorized,
        };
        let packet_serialized = serialize_packet(&Packet::Connack(connack)).expect("Serialize error!");
        client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size()).unwrap();
    }

    pub fn handle_disconnect_packet(&mut self, ind: usize) -> Result<(), Error>{
        Err(Error::Graceful)
    }

    fn add_client(&mut self,
                  client_index  : usize,
                  connect       : &Connect,
                  mut client    : Client,
                  user_name     : String) -> Result<bool, Error> {
        if connect.client_id.starts_with(' ') || (!connect.clean_session && connect.client_id.is_empty()){
            let connack = Connack {
                session_present: false,
                code: ConnectReturnCode::BadUsernamePassword,
            };
            let packet_serialized = serialize_packet(&Packet::Connack(connack)).expect("Serialize error!");
            client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size());

            return Err(Error::WrongClientId);
        }

        if connect.password.is_none(){
            let connack = Connack {
                session_present: false,
                code: ConnectReturnCode::BadUsernamePassword,
            };
            let packet_serialized = serialize_packet(&Packet::Connack(connack)).expect("Serialize error!");
            client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size());
            return Err(Error::PasswordError);
        }

        client.set_keep_alive(connect.keep_alive);

        let mut session_exists = false;
        if !self.config.get_allow_mc() {
            // Multiconnect: Client zaten var ise
            if let Some(old_client_index) = self.clients.get_client_index_with_user_name(&user_name) {
                session_exists = true;
                // Eger clean session var ise eski clienti kullan 
                // taska ve ya baglantiya ozel ayarlarini degistir
                
                let status = {
                    let client = self.clients.get_client_mut(old_client_index).expect("Client bulunamadi!");
                    if client.clean_session {
                        client.clear();
                    }
                    client.status()
                };

                if status == ConnectionStatus::Connected {

                    self.clients.insert_client(client_index, client, user_name);

                    let client = self.clients.get_client_mut(old_client_index).expect("Client bulunamadi!");

                    client.end_task(Error::MulticonnectSameThread);
                    
                } else {
                    self.clients.insert_client(client_index, client, user_name);

                    let client = self.clients.get_client_mut(old_client_index).expect("Client bulunamadi!");
                }
  
                return Ok(session_exists);
            } else {
                self.clients.insert_client(client_index, client, user_name);
                return Ok(session_exists);
            };
        } else{
            self.clients.insert_client(client_index, client, user_name);
            return Ok(session_exists);
        }
    }

    pub fn handle_publish_packet(&mut self, ind: usize, publish: &Publish) -> Result<(), Error>{
        let packet_serialized = serialize_packet(&Packet::Puback(PacketIdentifier(0))).expect("Serialize error!");
        {
            let client = self.clients.get_client_mut(ind).expect("Client bulunamadi!");
            client.reset_last_control_at();
            client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size())?;
        }

        self.publish_to_all_clients(publish)?;
        Ok(())
    }

    pub fn publish_to_all_clients(&mut self, publish: &Publish) -> Result<(), Error> {
        // TODO kendisine gonder?
        let topic_name = publish.topic_name.clone();
        let publish_packet = Publish{
            dup        : false,
            retain     : false,
            qos        : QoS::AtMostOnce,
            topic_name : topic_name.clone(),
            pid        : None,
            payload    : publish.payload.clone()
        };

        let serialized = serialize_packet(&Packet::Publish(publish_packet)).expect("Deser error");
        let subscribers = self.subs.get_subscriptions(&topic_name);
        // println!("client sayisi {}", self.clients.len());
        // println!("subscription sayisi {}", subscribers.len());
        for i in subscribers.iter(){
            if self.clients.is_client_connected(*i){
                let client = self.clients.get_client_mut(*i).expect("Publish: Client bulunamadi");

                if let Err(_) = client.send(Message::Normal(serialized.clone()), self.config.get_chan_buf_size()){
                    client.end_task(Error::SlowSender);
                }
            }  
        }
        Ok(())
    }

    pub fn handle_subscribe_packet(&mut self, 
                                   client_index     : usize,
                                   subscribe        : &Subscribe, 
                                   topic_authz_res  : &Vec<bool> ) -> Result<(), Error>{
        let pid = subscribe.pid;
        let topics_count = subscribe.topics.len();

        let mut return_codes = Vec::with_capacity(topics_count);
        let mut successful_subscription_count = 0;
        for (i, t) in topic_authz_res.into_iter().enumerate(){
            if *t {
                successful_subscription_count += 1;
                self.subs.subscribe(client_index, subscribe.topics[i].topic_path.clone());
                self.get_client_mut(client_index).save_subscription(subscribe.topics[i].topic_path.clone());
                return_codes.push(SubscribeReturnCodes::Success(QoS::AtMostOnce))
            } else {
                return_codes.push(SubscribeReturnCodes::Failure)
            }
        }
        
        let suback = Suback{
            pid,
            return_codes
        };

        let packet_serialized = serialize_packet(&Packet::Suback(suback)).expect("Serialize error!");
        let mut topics = Vec::new();
        for (aut, topic) in topic_authz_res.iter().zip(subscribe.topics.iter()) {
            if *aut {
                topics.push(topic.topic_path.clone());
            }
        }
        let user_name = self.get_client(client_index).user_name.clone();
        let message = SystemMessage::Subscribed(SubscribeInfo::Topics(user_name, Arc::new(topics)));
        self.broadcast_system_message(message);
        self.clients.get_client_mut(client_index).unwrap().reset_last_control_at();
        self.clients.get_client_mut(client_index).unwrap().send(Message::Normal(packet_serialized), self.config.get_chan_buf_size())
    }

    pub fn handle_unsubscribe_packet(&mut self, ind: usize, unsubscribe : &Unsubscribe) -> Result<(), Error>{
        let client = self.clients.get_client_mut(ind).expect("Client bulunamadi!");
        for t in unsubscribe.topics.iter(){
            self.subs.unsubscribe(ind, t);
        }
        client.reset_last_control_at();
        client.delete_subscription(unsubscribe.topics.as_ref());

        let unsuback = Packet::Unsuback(unsubscribe.pid);

        let packet_serialized = serialize_packet(&unsuback).expect("Serialize error!");

        client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size())
    }

    pub fn handle_pingreq_packet(&mut self, ind: usize) -> Result<(), Error> {
        let client = self.clients.get_client_mut(ind).expect("Client bulunamadi!");

        client.reset_last_control_at();

        let pingresp = Packet::Pingresp;

        let packet_serialized = serialize_packet(&pingresp).expect("Serialize error!");

        client.send(Message::Normal(packet_serialized), self.config.get_chan_buf_size())
    }

    #[inline(always)]
    pub fn end_client_task(&mut self, ind: usize, err: Error) {
        let client = self.clients.get_client_mut(ind).expect("Client bulunamadi!");
        client.end_task(err);
    }

    pub fn get_keepalive(&self, client_index: usize) -> Duration{
        self.clients.get_client(client_index).unwrap().keep_alive.unwrap()
    }

    #[inline(always)]
    pub fn get_client_ind_with_user_name(&self, user_name: &str) -> Option<usize> {
        self.clients.get_client_index_with_user_name(user_name)
    }

    pub fn get_client(&self, ind: usize) -> &Client{
        self.clients.get_client(ind).expect("Client bulunamadi!")
    }

    pub fn get_client_mut(&mut self, ind: usize) -> &mut Client{
        self.clients.get_client_mut(ind).expect("Client bulunamadi")
    }

    #[inline(always)]
    pub fn get_user_name(&self, ind: usize) -> Option<&str>{
        self.clients.get_client(ind).map(|x| x.user_name.as_ref())
    }

    pub fn handle_close(&mut self, client_index: usize) {
        let message = SystemMessage::Disconnected;
        self.broadcast_system_message(message);
        self.unsubscribe_all(client_index);
        self.remove_client(client_index);
    }

    pub fn unsubscribe_all(&mut self, 
                           client_index : usize){
        let mut len = 0;
        if let Some(c) = self.clients.get_client_mut(client_index){
            for s in c.subscriptions.iter(){
                self.subs.unsubscribe(client_index, &s);
            }
            len = c.subscriptions.len();
        }
        let message = SystemMessage::Unsubscribed(len);
        self.broadcast_system_message(message);
    }

    #[inline(always)]
    pub fn remove_client(&mut self, ind: usize) {
        self.clients.remove_client(ind).expect("Error while removing client!");
    }
}

impl <T>BrokerInterface for Broker<T> where T:Subscription{
    fn subscribe(&mut self, client_index: usize, topics: &[String]){
        if !self.clients.is_client_connected(client_index){
            return;
        }

        for topic in topics{
            self.subs.subscribe(client_index, topic.to_owned());
        }
        {
            let client = self.get_client_mut(client_index);

            for topic in topics{
                client.save_subscription(topic.to_owned());
            }
        }
        let user_name = self.get_client(client_index).user_name.clone();
        let message = SystemMessage::Subscribed(SubscribeInfo::Topics(user_name, Arc::new(topics.to_owned())));
        self.broadcast_system_message(message);
    }

    fn unsubscribe(&mut self, client_index: usize, topics: &[String]){
        {
            for topic in topics{
                self.subs.unsubscribe(client_index, topic.as_ref());
            }

            let client = self.get_client_mut(client_index);
            client.delete_subscription(topics);
        }

        let message = SystemMessage::Unsubscribed(topics.len());
        self.broadcast_system_message(message);
    }
    
    fn publish(&mut self, topic: &str, payload: Vec<u8>){
        let publish_packet = make_publish_packet(topic.to_owned(), payload);
        self.publish_to_all_clients(&publish_packet);
    }

    fn publish_to_client(&mut self, client_index: usize , topic: &str, payload: Vec<u8>){
        let publish_packet = make_publish_packet(topic.to_owned(), payload);
        
        let packet_serialized =  serialize_packet(&Packet::Publish(publish_packet)).expect("Serialize error!");
        self.clients.get_client_mut(client_index).unwrap().send(Message::Normal(packet_serialized), self.config.get_chan_buf_size());

    }
}

#[inline(always)]
pub fn make_publish_packet(topic: String, payload: Vec<u8>) -> Publish{
    let payload = Arc::new(payload);
    let publish = Publish{
        dup : false,
        qos : mqtt3::QoS::AtMostOnce,
        retain : false, 
        topic_name : topic,
        pid : None,
        payload : payload
    };
    publish
}

fn gen_user_name() -> String {
    let random: String = rand::thread_rng()
                            .gen_ascii_chars()
                            .take(7)
                            .collect();
    format!("mtx-{}", random)
}

pub enum SocketWriterHalf{ // TODO Burdan tasi
    Tcp(SinkTcp),
    WebSocket(SinkWebSocket),
}

impl std::fmt::Debug for SocketWriterHalf{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self{
            &SocketWriterHalf::Tcp(_) => write!(f, "TcpSocket"),
            &SocketWriterHalf::WebSocket(_) => write!(f, "WebSocket"),
        }
    }
}

impl SocketWriterHalf{
    #[inline(always)]
    pub fn send(&mut self, packet : OutBuf) -> Result<(), Error>{
        match self{
            &mut SocketWriterHalf::Tcp(ref mut sink) => {
                sink.start_send(packet)
                    .map_err(|x| Error::ExternalDisconnect)?;

                sink.poll_complete().map_err(|_| Error::ExternalDisconnect)?;
            },
            &mut SocketWriterHalf::WebSocket(ref mut sink) => {
                sink.start_send(OwnedMessage::Binary(packet.to_vec()))
                    .map_err(|x| Error::ExternalDisconnect)?;
                
                sink.poll_complete().map_err(|_| Error::ExternalDisconnect)?;
            }
        }
        Ok(())
    }

    pub fn close(&mut self) {
        match self{
            &mut SocketWriterHalf::Tcp(ref mut sink) => {
            },
            &mut SocketWriterHalf::WebSocket(ref mut sink) =>{
                sink.start_send(OwnedMessage::Close(None));
                sink.poll_complete();
            }
        }
    }
}

impl Sink for SocketWriterHalf{
    type SinkItem = Message;
    type SinkError = Error;

    fn start_send(&mut self, 
                  item: Self::SinkItem) -> StartSend<Self::SinkItem, Self::SinkError> {
        match self{
            &mut SocketWriterHalf::Tcp(ref mut sink) => {
                match item {
                    Message::Normal(buf) => {
                        return sink.start_send(buf)
                                   .map(|x| x.map(|y| {
                                       Message::Normal(y)
                                   }))
                                   .map_err(|_| Error::ExternalDisconnect);

                    },
                    Message::Close => {
                        Ok(AsyncSink::Ready)
                    }
                }
            },
            &mut SocketWriterHalf::WebSocket(ref mut sink) => {
                match item {
                    Message::Normal(buf) => {
                        return sink.start_send(OwnedMessage::Binary(buf.to_vec()))
                                .map(|x| x.map(|y| {
                                    match y{
                                        OwnedMessage::Binary(z) => {
                                            Message::Normal(Bytes::from(z))
                                        } ,
                                        _ => {
                                            unreachable!()
                                        }
                                    }
                                    }))
                                .map_err(|_| Error::ExternalDisconnect);

                    },
                    Message::Close => {
                        return sink.start_send(OwnedMessage::Close(None))
                                .map(|x| x.map(|y| {
                                    match y{
                                        OwnedMessage::Close(_) => {
                                            Message::Close
                                        } ,
                                        _ => {
                                            unreachable!()
                                        }
                                    }
                                    }))
                                .map_err(|_| Error::ExternalDisconnect);
                    }
                }
            }
        }
    }

    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        match self{
            &mut SocketWriterHalf::Tcp(ref mut sink) => {
                return sink.poll_complete()
                           .map_err(|_| Error::ExternalDisconnect);
            },
            &mut SocketWriterHalf::WebSocket(ref mut sink) => {
                return sink.poll_complete()
                           .map_err(|_| Error::ExternalDisconnect);
            }
        }
    }
}

#[derive(Debug)]
pub enum Message{
    Normal(OutBuf),
    Close
}
