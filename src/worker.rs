use std;
use std::rc::Rc;
use std::cell::{RefCell, Cell};
use std::sync::Arc;
use futures::sync::mpsc::{Sender, Receiver, SendError};
use broker::{Broker, SocketWriterHalf};
use load_balancer::{Writer, OutBuf};
use error::Error;
use mqtt3::{Publish, SubscribeTopic};
use subscription::TrieSubscription;
use futures::unsync;
use client_fut::ClientFut;
use tokio_core::reactor::{Core, Handle, Timeout, Interval};
use futures::{Future, Stream, Sink};
use futures::future::{ok};
use futures::unsync::mpsc;
use plugin;
use config::Config;
use broker::Message;
use client::Client;
use std::time::Duration;


// TODO task end channel'i error tipi gondermeli 
// handle publish icinde error veren client 
// dogru error tipiyle sonlanmali

pub type ConReceiver            = Receiver<Writer>;
pub type SystemMessageSender    = Vec<Sender<SystemMessage>>;
pub type SystemMessageReceiver  = Receiver<SystemMessage>;
pub type PublishSender          = Vec<Sender<Arc<Publish>>>;
pub type PublishReceiver        = Receiver<Arc<Publish>>;

#[derive(Debug, Clone)]
pub enum SubscribeInfo{
    Count(usize),
    Topics(String, Arc<Vec<String>>)
}

#[derive(Debug, Clone)]
pub enum SystemMessage{
    Connected(String),
    Subscribed(SubscribeInfo),
    Unsubscribed(usize),
    Disconnected
}

pub struct Worker {
    worker_no         : usize,
    config            : Rc<Config>
}

impl Worker{
    pub fn new(worker_no : usize,
               config    : Config) -> Self{
        Worker{
            worker_no,
            config : Rc::new(config)
        }
    }

    pub fn start(&mut self,
                 connection_recv  : ConReceiver,
                 system_message_sender : SystemMessageSender,
                 system_message_recv   : SystemMessageReceiver,
                 publish_sender   : PublishSender,
                 publish_recv     : PublishReceiver) {
        let worker_no = self.worker_no;
        info!("Worker {}", worker_no);
        let mut core = Core::new().unwrap();
        let handle = core.handle();
        
        let plugin = plugin::new_plugin::<Broker<TrieSubscription>>(handle.clone(), worker_no);
        let plugin = Rc::new(RefCell::new(plugin));

        let mut system_message_forwarder_sinks = Vec::new();
        let mut system_message_forwarder_streams = Vec::new();
        for i in 0..system_message_sender.len(){
            let (system_message_forwarder_sink, system_message_forwarder_stream) = unsync::mpsc::channel(1024);
            system_message_forwarder_sinks.push(system_message_forwarder_sink);
            system_message_forwarder_streams.push(system_message_forwarder_stream);
        }

        
        let broker = Broker::new(worker_no,
                                 handle.clone(),
                                 self.config.clone(),
                                 publish_sender,
                                 system_message_forwarder_sinks);

        let broker = Rc::new(RefCell::new(broker));

        spawn_system_message_sender(worker_no, &handle, system_message_forwarder_streams, system_message_sender);
        spawn_system_message_receiver(worker_no, self.config.get_allow_mc(), system_message_recv, &handle, broker.clone());
        handle_publish_broadcast(worker_no, publish_recv, &handle, broker.clone());

        let chan_buf_size = self.config.get_chan_buf_size();
        let sink_buf_size = self.config.get_sink_buf_size();
        let mut i = 0;
        let channels = connection_recv.for_each(|(packet_recv, writer_half, conn_closer)|{
            info!("Worker {}: Baglanti geldi!", worker_no);
            i += 1;
            let (task_close_signal_sender, task_close_signal)  = unsync::oneshot::channel::<Error>();
            let (packet_sender, packet_forwarder)  = unsync::mpsc::channel::<Message>(chan_buf_size);
            
            let channel_len = Rc::new(Cell::new(0));

            let client = Client::new(packet_sender, task_close_signal_sender, channel_len.clone());

            // Writer task close signal 
            let (wt_close_sender, wt_close_recv) = unsync::oneshot::channel();

            spawn_writer_task(&handle, channel_len, writer_half, packet_forwarder, wt_close_recv);

            let broker2       = broker.clone();

            let client_fut = ClientFut::new(packet_recv, client, broker.clone(), plugin.clone(), task_close_signal, conn_closer, wt_close_sender, handle.clone());

            handle.spawn(client_fut
                            .then(|_| {
                                Ok::<(),()>(())
                            }));

            Ok(())
        })
        .map_err(|_| ())
        .then(|_| Ok::<(),()>(()));

        core.run(channels).expect("Error at worker event loop!");
    }
}

fn spawn_system_message_receiver(worker_no: usize,
                         allow_mc: bool,
                         system_message_recv : SystemMessageReceiver, 
                         handle: &Handle,
                         broker: Rc<RefCell<Broker<TrieSubscription>>>){
    let mc_recv = system_message_recv.for_each(move |system_message| {
        match system_message{
            SystemMessage::Connected(user_name) => {
                let ind_opt = broker.borrow().get_client_ind_with_user_name(&user_name);
                if let Some(ind) = ind_opt {
                    let is_connected = broker.borrow().get_client(ind).is_connected();
                    if is_connected && !allow_mc{
                        broker.borrow_mut().end_client_task(ind, Error::MulticonnectOtherThread);
                    }            
                }
            },
            _ => {
            }
        }

        ok::<(),()>(())
    });

    handle.spawn(mc_recv);
}

fn spawn_system_message_sender(worker_no : usize,
                               handle    : &Handle,
                               system_message_forwarders: Vec<unsync::mpsc::Receiver<SystemMessage>>,
                               mut system_message_senders  : SystemMessageSender){
    for (forwarder, system_message_sender) in system_message_forwarders.into_iter().zip(system_message_senders.into_iter()){
        let f = forwarder
            // .inspect(|x| println!("Forwarder {:?}", x))
            .forward(system_message_sender.sink_map_err(|_| ()))
            .then(|_| Ok::<(), ()>(()));
        handle.spawn(f);
    }
}

fn handle_publish_broadcast(worker_no    : usize,
                            publish_recv : PublishReceiver, 
                            handle       : &Handle, 
                            broker       : Rc<RefCell<Broker<TrieSubscription>>>){
    let pub_recv = publish_recv.for_each(move |publish_packet| {
        let start = std::time::Instant::now();
        broker.borrow_mut().publish_to_all_clients(publish_packet.as_ref());
        let end = std::time::Instant::now();
        // println!("Socket write suresi {}", (end - start).subsec_nanos());
        ok::<(),()>(())
    });
    handle.spawn(pub_recv);
}

fn spawn_writer_task(handle        : &Handle,
                     channel_len   : Rc<Cell<usize>>,
                     socket_writer : SocketWriterHalf,
                     receiver      : unsync::mpsc::Receiver<Message>,
                     close_signal  : unsync::oneshot::Receiver<()>) {
    let handle2      = handle.clone();
    let write_future =  close_signal.then(|_| Ok::<(),()>(()))
                                    .select(receiver.map_err(|_|())
                                        .inspect(move |_| {
                                            let l = channel_len.get() - 1;
                                            channel_len.set(l); 
                                            () 
                                        })
                                        .forward(socket_writer
                                                        .sink_map_err(|_| ()))
                                        .then(|_| Ok::<(),()>(())))
                                    .then(|_| {
                                        Ok::<(),()>(())
                                    });    
    handle.spawn(write_future);
}
