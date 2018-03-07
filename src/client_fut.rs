use futures::{Future, Stream, Sink, Poll, Async};
use tokio_core::reactor::{Handle, Timeout, Interval};
use plugin_interface::{PluginInterface, BrokerInterface, PluginObject, CloseReason, AuthzFut, AuthnFut};
use config::Config;
use std::rc::Rc;
use std::cell::RefCell;
use std::time::Duration;
use std::sync::Arc;
use mqtt3::{self, Packet};
use subscription::TrieSubscription;
use client::Client;
use broker::Broker;
use error::Error;
use futures::unsync;
use futures::sync;
use std;

enum ConnectState{
    BeforeConnect,
    Authentication(AuthnFut),
    AfterConnect(bool, String),
}

struct ConnectFut{
    connect_packet : mqtt3::Connect,
    client_index   : usize,
    state          : ConnectState,
    client         : Option<Client>,
    broker         : Rc<RefCell<Broker<TrieSubscription>>>,
    plugin         : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>,
}

impl Future for ConnectFut{
    type Item  = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error>{
        loop{
            let new_state = match self.state{
                ConnectState::BeforeConnect => {
                    debug!("ClientState::Connect::BeforeConnect");
                    let authn_fut = self.plugin.borrow_mut().handle_authn(self.client_index, self.broker.clone(), &self.connect_packet);
                    ConnectState::Authentication(authn_fut)
                },
                ConnectState::Authentication(ref mut authn_fut) => {
                    debug!("ClientState::Connect::Authentication");                    
                    match authn_fut.poll(){
                        Ok(Async::Ready(user_name)) => {
                            ConnectState::AfterConnect(true, user_name)
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(_) => {
                            ConnectState::AfterConnect(false, String::new())
                        }
                    }
                },
                ConnectState::AfterConnect(connected, ref user_name) => {
                    debug!("ClientState::Connect::AfterConnect");
                    if !connected {
                        if let Some(ref mut client) = self.client{
                            self.broker.borrow_mut().auth_rejected(client);
                        }
                        return Err(Error::AuthnFailed);
                    } else {
                        self.broker.borrow_mut().handle_connect(self.client_index, &self.connect_packet, self.client.take().unwrap(), user_name.clone()).unwrap();
                        self.broker.borrow_mut().broadcast_connected_message_to_other_threads(user_name.clone());
                        self.plugin.borrow_mut().handle_connect(self.client_index, self.broker.clone(), &self.connect_packet);
                        return Ok(Async::Ready(()));
                    }
                }
            };
            self.state = new_state;
        }
    }
}

enum SubscribeState{
    BeforeSubscribe,
    Authhorization(AuthzFut),
    AfterAuthorization(Vec<bool>),
}

struct SubscribeFut{
    subscribe_packet    : mqtt3::Subscribe,
    state               : SubscribeState,
    client_index        : usize,
    broker              : Rc<RefCell<Broker<TrieSubscription>>>,
    plugin              : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>
}

impl Future for SubscribeFut{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop{
            let new_state = match self.state{
                SubscribeState::BeforeSubscribe => {
                    debug!("ClientState::SubscribeState::BeforeSubscribe");
                    let authz_fut = self.plugin.borrow_mut().handle_subscribe_authz(self.client_index, self.broker.clone(), &self.subscribe_packet);
                    SubscribeState::Authhorization(authz_fut)
                },
                SubscribeState::Authhorization(ref mut authz_fut) => {
                    debug!("ClientState::SubscribeState::Authhorization");
                    match authz_fut.poll(){
                        Ok(Async::Ready(authz_results)) => {
                            SubscribeState::AfterAuthorization(authz_results)
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(err) => {
                            let authz_results = vec![false; self.subscribe_packet.topics.len()];
                            SubscribeState::AfterAuthorization(authz_results)
                        }
                    }
                },
                SubscribeState::AfterAuthorization(ref authz_results) => {
                    debug!("ClientState::SubscribeState::AfterSubscribe");
                    self.broker.borrow_mut().handle_subscribe_packet(self.client_index, &self.subscribe_packet, &authz_results);
                    self.plugin.borrow_mut().handle_subscribe(self.client_index, self.broker.clone(), &self.subscribe_packet, &authz_results);
                    return Ok(Async::Ready(()));
                }
            };
            self.state = new_state;
        }
    }
}


struct UnsubscribeFut{
    unsubscribe_packet  : mqtt3::Unsubscribe,
    client_index        : usize,
    broker              : Rc<RefCell<Broker<TrieSubscription>>>,
    plugin              : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>
}

impl Future for UnsubscribeFut{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.broker.borrow_mut().handle_unsubscribe_packet(self.client_index, &self.unsubscribe_packet);
        self.plugin.borrow_mut().handle_unsubscribe(self.client_index, self.broker.clone(), &self.unsubscribe_packet);
        Ok(Async::Ready(()))
    }
}


struct PingReqFut{
    client_index    : usize,
    broker          : Rc<RefCell<Broker<TrieSubscription>>>
}

impl Future for PingReqFut{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.broker.borrow_mut().handle_pingreq_packet(self.client_index);
        Ok(Async::Ready(()))
    }
}

struct DisconnectFut{
    client_index    : usize,
    broker          : Rc<RefCell<Broker<TrieSubscription>>>,
    plugin          : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>
}

impl Future for DisconnectFut{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // self.plugin.borrow_mut().handle_disconnect(self.client_index, self.broker.clone());
        // self.broker.borrow_mut().handle_disconnect(self.client_index);
        Ok(Async::Ready(()))
    }
}

enum CloseState{
    Prepare,
    Timeout(Timeout),
    Close
}

struct CloseFut{
    client_index        : usize,
    state               : CloseState,
    handle              : Handle,
    close_reason        : CloseReason,
    broker              : Rc<RefCell<Broker<TrieSubscription>>>,
    plugin              : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>,
    close_signal_sender : Option<sync::mpsc::Sender<()>>,
    writer_task_closer  : Option<unsync::oneshot::Sender<()>>,
    connected           : bool
}

impl Future for CloseFut{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let new_state = match self.state{
                CloseState::Prepare => {
                    match self.close_reason{
                        CloseReason::Multiconnect => {
                            // TODO send multiconnect message
                            if self.connected{
                                self.plugin.borrow_mut().handle_close(self.client_index, self.broker.clone(), CloseReason::Multiconnect);     
                            }
                            let timeout_duration = std::time::Duration::from_millis(200);
                            let timeout = Timeout::new(timeout_duration, &self.handle).unwrap();
                            CloseState::Timeout(timeout)
                        },
                        CloseReason::AuthnFailed => {
                            if self.connected{
                                self.plugin.borrow_mut().handle_close(self.client_index, self.broker.clone(), CloseReason::AuthnFailed);
                            }
                            // self.broker.borrow_mut().authn_rejected(self.client_index);                            
                            let timeout_duration = std::time::Duration::from_millis(200);
                            let timeout = Timeout::new(timeout_duration, &self.handle).unwrap();
                            CloseState::Timeout(timeout)
                        }
                        _ => {
                            if self.connected{
                                self.plugin.borrow_mut().handle_close(self.client_index, self.broker.clone(), CloseReason::Other);
                            }
                            CloseState::Close
                        }
                    }
                },
                CloseState::Timeout(ref mut timeout) => {
                    match timeout.poll() {
                        Ok(Async::Ready(_)) => {
                            CloseState::Close
                        }, 
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        }, 
                        Err(_) => {
                            CloseState::Close
                        }
                    }
                },
                CloseState::Close => {
                    if self.connected{
                        self.broker.borrow_mut().handle_close(self.client_index);
                    } else {
                        self.broker.borrow_mut().delete_empty_client(self.client_index);
                    }
                    let mut close_signal_sender = self.close_signal_sender.take().expect("");
                    close_signal_sender.start_send(());
                    close_signal_sender.poll_complete();
                    let mut writer_task_closer = self.writer_task_closer.take().expect("");
                    writer_task_closer.send(());
                    return Ok(Async::Ready(()));
                }
            };
            self.state = new_state;
        }
    }
}



enum State{
    WaitingFirstPacket,    
    Connect(ConnectFut),
    // Publish(PublishFut),
    Subscribe(SubscribeFut),
    Unsubscribe(UnsubscribeFut),
    Disconnect(DisconnectFut),
    PingReq(PingReqFut),
    Close(CloseFut),
    WaitingPacket,
}


pub struct ClientFut<S>{
    stream              : S,
    handle              : Handle,
    state               : State,
    client              : Option<Client>,
    keep_alive          : Option<Interval>,
    broker              : Rc<RefCell<Broker<TrieSubscription>>>,
    close_signal        : Option<unsync::oneshot::Receiver<Error>>,
    close_signal_sender : Option<sync::mpsc::Sender<()>>,
    writer_task_closer  : Option<unsync::oneshot::Sender<()>>,
    plugin              : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>,
    client_index        : usize,
    connected           : bool
}

impl <S>ClientFut<S> 
    where S : Stream<Item= Packet, Error= ()>{

    pub fn new(stream               : S,
               client               : Client,
               broker               : Rc<RefCell<Broker<TrieSubscription>>>,
               plugin               : Rc<RefCell<PluginObject<Broker<TrieSubscription>>>>,
               close_signal         : unsync::oneshot::Receiver<Error>,
               close_sig_sender     : sync::mpsc::Sender<()>,
               writer_task_closer   : unsync::oneshot::Sender<()>,
               handle               : Handle) -> Self {
        let client_index = broker.borrow_mut().get_empty_client();
        ClientFut {
            stream              : stream,
            state               : State::WaitingFirstPacket,
            plugin              : plugin,
            client              : Some(client),
            broker              : broker,
            keep_alive          : None,
            close_signal        : Some(close_signal),
            close_signal_sender : Some(close_sig_sender),
            writer_task_closer  : Some(writer_task_closer),
            handle              : handle,
            client_index        : client_index,
            connected           : false,
        }
    }
}

impl <S>ClientFut<S>
    where S : Stream<Item= Packet, Error= ()>{
    fn close_fut(&mut self, close_reason: CloseReason) -> CloseFut{
        self.close_signal = None;
        CloseFut{
            client_index        : self.client_index,
            state               : CloseState::Prepare,
            handle              : self.handle.clone(),
            close_reason        : close_reason,
            broker              : self.broker.clone(),
            plugin              : self.plugin.clone(),
            close_signal_sender : self.close_signal_sender.take(),
            writer_task_closer  : self.writer_task_closer.take(),
            connected           : self.connected
        }
    }

    fn poll_fut(&mut self) -> Poll<(), Error> {
        loop {
            let new_state = match self.state{
                State::WaitingFirstPacket => {
                    match self.stream.poll() {
                        Ok(Async::Ready(Some(packet))) =>{
                            if let mqtt3::Packet::Connect(connect_packet) = packet{
                                let connect_fut = ConnectFut{
                                    connect_packet,
                                    state  : ConnectState::BeforeConnect,
                                    client : self.client.take(),
                                    broker : self.broker.clone(),
                                    plugin : self.plugin.clone(),
                                    client_index : self.client_index,
                                };
                                State::Connect(connect_fut)
                            } else { 
                                State::Close(self.close_fut(CloseReason::Other))
                            }
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(_) | Ok(Async::Ready(None)) => {
                            State::Close(self.close_fut(CloseReason::Other))
                        }
                    }
                },
                State::WaitingPacket => {
                    match self.stream.poll() {
                        Ok(Async::Ready(Some(packet))) =>{
                            match packet {
                                mqtt3::Packet::Connect(_) => {
                                    State::Close(self.close_fut(CloseReason::Other))
                                },
                                mqtt3::Packet::Subscribe(subscribe_packet) => {
                                    let subscribe_fut = SubscribeFut{
                                        subscribe_packet,
                                        state  : SubscribeState::BeforeSubscribe,
                                        client_index : self.client_index,
                                        broker : self.broker.clone(),
                                        plugin : self.plugin.clone(),
                                    };
                                    State::Subscribe(subscribe_fut)
                                },
                                mqtt3::Packet::Unsubscribe(unsubscribe_packet) => {
                                    let unsubscribe_fut = UnsubscribeFut{
                                        unsubscribe_packet,
                                        client_index  : self.client_index,
                                        broker : self.broker.clone(),
                                        plugin : self.plugin.clone(),
                                    };
                                    State::Unsubscribe(unsubscribe_fut)
                                }
                                mqtt3::Packet::Disconnect => {
                                    let disconnect_fut = DisconnectFut{
                                        client_index : self.client_index,
                                        broker : self.broker.clone(),
                                        plugin : self.plugin.clone(),
                                    };
                                    State::Disconnect(disconnect_fut)
                                },
                                mqtt3::Packet::Pingreq => {
                                    let pingrec_fut = PingReqFut{
                                        client_index : self.client_index,
                                        broker       : self.broker.clone()
                                    };
                                    State::PingReq(pingrec_fut)
                                }
                                _ => {
                                    // TODO 
                                    State::WaitingPacket
                                }
                            }
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(_) | Ok(Async::Ready(None)) => {
                            State::Close(self.close_fut(CloseReason::Other))
                        }
                    }
                },
                State::Connect(ref mut connect_fut) => {
                    match connect_fut.poll() {
                        Ok(Async::Ready(client_index)) => {
                            self.connected = true;
                            if self.keep_alive.is_none(){       
                                let dur = self.broker.borrow().get_keepalive(self.client_index);
                                let timeout = Interval::new(dur, &self.handle).unwrap();
                                self.keep_alive = Some(timeout);
                            }
                            State::WaitingPacket
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(err) => {
                            let close_reason = match err{
                                Error::AuthnFailed => CloseReason::AuthnFailed,
                                _ => CloseReason::Other,
                            };
                            let close_fut = CloseFut{
                                client_index        : self.client_index,
                                state               : CloseState::Prepare,
                                handle              : self.handle.clone(),
                                close_reason        : close_reason,
                                broker              : self.broker.clone(),
                                plugin              : self.plugin.clone(),
                                close_signal_sender : self.close_signal_sender.take(),
                                writer_task_closer  : self.writer_task_closer.take(),
                                connected           : self.connected
                            };
                            self.close_signal =None;
                            State::Close(close_fut)
                        }
                    }
                },
                State::Subscribe(ref mut subscribe_fut) => {
                    match subscribe_fut.poll() {
                        Ok(Async::Ready(_)) => {
                            State::WaitingPacket
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(_) => {
                            State::WaitingPacket
                        }
                    }
                },
                State::Unsubscribe(ref mut unsubscribe_fut) => {
                    match unsubscribe_fut.poll() {
                        Ok(Async::Ready(_)) => {
                            State::WaitingPacket
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(_) => {
                            State::WaitingPacket
                        }
                    }
                },
                State::PingReq(ref mut pingrec_fut) => {
                    match pingrec_fut.poll() {
                        Ok(Async::Ready(_)) => {
                            State::WaitingPacket
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        },
                        Err(_) => {
                            State::WaitingPacket
                        }
                    }
                },
                // State::Publish(ref mut publish_fut) => {
                //     match publish_fut.poll() {
                //         Ok(Async::Ready(_)) => {
                //             State::WaitingPacket
                //         },
                //         Ok(Async::NotReady) => {
                //             return Ok(Async::NotReady);
                //         },
                //         Err(_) => {
                //             State::WaitingPacket
                //         }
                //     }
                // },
                State::Disconnect(ref mut disconnect_fut) => {
                    match disconnect_fut.poll() {
                        Ok(Async::Ready(_)) | Err(_)=> {
                            let close_fut = CloseFut{
                                client_index        : self.client_index,
                                state               : CloseState::Prepare,
                                handle              : self.handle.clone(),
                                close_reason        : CloseReason::Graceful,
                                broker              : self.broker.clone(),
                                plugin              : self.plugin.clone(),
                                close_signal_sender : self.close_signal_sender.take(),
                                writer_task_closer  : self.writer_task_closer.take(),
                                connected           : self.connected
                            };
                            self.close_signal = None;
                            State::Close(close_fut)
                        },
                        Ok(Async::NotReady) => {
                            return Ok(Async::NotReady);
                        }
                    }
                },
                State::Close(ref mut close_fut) =>{
                    match close_fut.poll() {
                        Ok(Async::Ready(_)) | Err(_) => {
                            return Ok(Async::Ready(()));
                        }
                        _ => return Ok(Async::NotReady),
                    }
                }
                _ => {
                    State::WaitingFirstPacket
                }
            };
            self.state = new_state;
        }
    }
}

impl <S>Future for ClientFut<S>
    where S : Stream<Item= Packet, Error= ()>{
    type Item = ();
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let close = if let Some(close_signal) = self.close_signal.as_mut(){
                match close_signal.poll() {
                    Ok(Async::Ready(err)) => {
                        match err {
                            Error::MulticonnectOtherThread | Error::MulticonnectSameThread => Some(CloseReason::Multiconnect),
                            _ => Some(CloseReason::Other),
                        }                        
                    },
                    Ok(Async::NotReady) =>{ None},
                    Err(_) => {
                        Some(CloseReason::Other)
                    }
                }
            } else {
                None
            };
            if let Some(close_reason) = close{
                self.close_signal = None;
                self.state = State::Close(self.close_fut(close_reason));
            }
            if !self.keep_alive.is_none(){
                loop {
                    match self.keep_alive.as_mut().unwrap().poll(){
                        Ok(Async::Ready(_))  => {
                            let mut broker_borrowed = self.broker.borrow_mut();
                            let client_mut = broker_borrowed.get_client_mut(self.client_index); 
                            if client_mut.has_exceeded_keep_alive(){
                                client_mut.end_task(Error::KeepAliveExpired);
                            }
                            continue;
                        },
                        _ =>{ break;}
                    }
                }
            }


            match self.poll_fut(){
                Ok(Async::NotReady) => {
                    return Ok(Async::NotReady);
                },
                Ok(Async::Ready(())) | Err(_) => {
                    return Ok(Async::Ready(()))
                }
            }
        }
    }
}