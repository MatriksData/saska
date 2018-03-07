use tokio_core::reactor::{Core, Handle, Timeout, Interval};
use tokio_core::net::{UdpSocket, TcpListener};
use tokio_io::{AsyncRead};
use futures::{self, Future, Stream, Sink};
use futures::sync::mpsc::{channel, Sender, SendError};
use futures::unsync;
use bytes::{Bytes};
use websocket::message::OwnedMessage;
use websocket::server::InvalidConnection;
use websocket::async::Server;
use mqtt3::{self, Publish};
use error;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use std::sync::Arc;
use std::time::{Instant, Duration};
use std::string::ToString;
use std::net::{Ipv4Addr, SocketAddr};
use plugin::{new_proxy_plugin};
use plugin_interface::ProxyPluginInterface;
use load_balancer::{LoadBalancer, LoadBalancingStrategy, Writer, OutBuf};
use broker::{SocketWriterHalf, make_publish_packet};
use codec::{accumulate_and_decode, NopCodec, SystemMessageUdpCodec, usize_to_vec};
use tokio_io::codec::LinesCodec;
use worker::{SystemMessage, SystemMessageSender, SystemMessageReceiver, PublishSender, SubscribeInfo};
use config::Config;

pub fn proxy(config                 : Config,
             senders                : Vec<Sender<Writer>>,
             publish_packet_sender  : PublishSender,
             system_message_sender  : SystemMessageSender,
             system_message_recv    : SystemMessageReceiver){
    // Core
    let mut core = Core::new().unwrap();
    let handle = core.handle();
    let load_balancer = Rc::new(RefCell::new(LoadBalancer::new(senders, LoadBalancingStrategy::SocketCount)));
    let first_packet_timeout = config.get_first_packet_timeout();
    let tcp_socket     = format!("0.0.0.0:{}", config.get_tcp_port());
    let ws_socket      = format!("0.0.0.0:{}", config.get_ws_port());
    let listener       = TcpListener::bind(&tcp_socket.parse().unwrap(), &handle).unwrap();
    let ws_listener    = Server::bind(&ws_socket, &handle).unwrap();

    spawn_watch_listener(&handle, load_balancer.clone(), config.get_db_port());

    let load_balancer2 = load_balancer.clone();
    let handle2 = core.handle();

	// // WEBSOCKET LISTENER TODO WSS
	let f = ws_listener.incoming()
        .map_err(|InvalidConnection { error, .. }| error)
        .then(|res| {
            match res{
                Ok(s) => Ok(Some(s)),
                Err(e) => Ok(None),
            }
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .for_each(move |(upgrade, addr)| {
            info!("Gelen websocket baglantisi {}", addr);
           
            let load_balancer3 = load_balancer.clone();
            
            let handle3 = handle.clone();

            let f = upgrade
                .use_protocol("mqttv3.1")
                .accept()
                .map_err(|_| ())
                .and_then(move |(s, _)| {
                    let (sink, stream) = s.split();
                    let mut buffer = Vec::with_capacity(2048);
                    let sink = SocketWriterHalf::WebSocket(sink);
                    let (packet_sender, receiver) = channel(1024);

                    let (mut trigger, closer) = futures::sync::mpsc::channel::<()>(2);
                    let worker_no = load_balancer3.borrow_mut().connect((receiver, sink, trigger.clone()));
                    let trigger_cl = trigger.clone();
                    let first_packet_came = Rc::new(Cell::new(false));
                    let first_packet_came_cl = first_packet_came.clone();
                    let ws_stream = stream
                        .take_while(|m| Ok(!m.is_close()))
                        .filter_map(|m| {
                            match m {
                                OwnedMessage::Text(p) =>{
                                    Some(Bytes::from(p.into_bytes()))
                                },
                                OwnedMessage::Binary(b) => {
                                    Some(Bytes::from(b))
                                },
                                _ =>  {
                                    None
                                }
                            }
                        })
                        .map_err(move |_| ())
                        .and_then(move |x| {
                            accumulate_and_decode(x, &mut buffer)
                                .map_err(|_| ())
                        })
                        .filter(|packet| packet.is_some())
                        .map(|packet| packet.unwrap())
                        .inspect(move |_| first_packet_came.set(true))
                        .forward(packet_sender.sink_map_err(|_| ()))
                        .map(move |_| {let _ = trigger_cl;()})
                        .map_err(|_| ());

                    let ws_connection = closer
                        .into_future()
                        .map(|_| ())
                        .map_err(|_| ())
                        .select(ws_stream)
                        .then(move |_| { // Baglanti kopmus ise burasi calisacak
                            load_balancer3.borrow_mut().disconnect(worker_no);
                            info!("Websocket baglantisi kapatildi {}", addr);
                            Ok::<(), ()>(())
                        });
                    
                    let timeout = Timeout::new(Duration::from_millis(first_packet_timeout), &handle3).unwrap();

                    let timeout_fut = timeout.and_then(move |_| {
                            if !first_packet_came_cl.get(){
                                trigger.start_send(()); // Baglanti timeout sona ermeden kapanmis olabileceginden unwrap edilmedi
                                trigger.poll_complete();
                            }
                            Ok(())
                        }
                    )
                    .map_err(|_| ());

                    handle3.spawn(timeout_fut);
                    handle3.spawn(ws_connection);
                    Ok(())
                });

            handle.spawn(f);
            Ok(())
        })
        .map_err(|_:()| ())
        .map(|_| ());

    handle2.spawn(f);

    let (system_message_publish_sender, system_message_publish_receiver) = unsync::mpsc::channel(1024);
    let proxy_plugin = Rc::new(RefCell::new(new_proxy_plugin(handle2.clone())));
    spawn_authorized_publisher(&mut core, proxy_plugin.clone(), publish_packet_sender, system_message_publish_receiver);

    // TCP LISTENER
    let proxy = listener.incoming().for_each(|(sock, addr)|{
        info!("Incoming tcp connection {}", addr);
        let codec = NopCodec; 

        let mut buffer = Vec::with_capacity(2048);
        let framed = sock.framed(codec);
        let (sink, stream) = framed.split();

        let (packet_sender, receiver) = channel(1024);
        let sink = SocketWriterHalf::Tcp(sink);                    

        let (mut trigger, closer) = futures::sync::mpsc::channel::<()>(2);
        let worker_no = load_balancer2.borrow_mut().connect((receiver, sink, trigger.clone()));
        let load_balancer5 = load_balancer2.clone();
        // Trigger dusunce closer error ile tetiklendiginden triggeri mpsc yapip senderin dusmemesini sagliyoruz

        let first_packet_came = Rc::new(Cell::new(false));
        let first_packet_came_cl = first_packet_came.clone();
        let trigger_cl = trigger.clone();

        let f = stream
            .map_err(|_| ())
            .and_then(move |x| {
                println!("Geldi");
                accumulate_and_decode(x, &mut buffer)
                    .map_err(|_| ())
            })
            .filter(|packet| packet.is_some())
            .map(|packet| packet.unwrap())
            .inspect(move |_| { first_packet_came.set(true)})
            .forward(packet_sender.sink_map_err(|_| ()))
            .map(move |_| {let _ = trigger_cl; ()}) // Trigger dusmesin diye eklendi
            .map_err(|_| ());

        let tcp_connection = closer
            .into_future()
            .map(|_| ())
            .map_err(|_| ())
            .select(f)
            .then(move |_| {
                info!("Tcp baglantisi sonlandi {}", addr);
                load_balancer5.borrow_mut().disconnect(worker_no);
                Ok::<(), ()>(())
            });
        
        let timeout = Timeout::new(Duration::from_millis(first_packet_timeout), &handle2).unwrap();

        let timeout_fut = timeout
            .map_err(|_| ())
            .and_then(move |_|  {
                    if !first_packet_came_cl.get(){
                        trigger.start_send(()); // Baglanti timeout sona ermeden kapanmis olabileceginden unwrap edilmedi
                        trigger.poll_complete();
                    }
                    Ok(())
                }
            );

        handle2.spawn(tcp_connection);
        handle2.spawn(timeout_fut);
        Ok(())
    });

    core.run(proxy).unwrap();
}

// Spawns authorized publisher stream. Solely "publish" packets come from this stream.
// And authorization is not requested. Publish packets stream directly to clients.
fn spawn_authorized_publisher(core                          : &mut Core,
                              proxy_plugin                  : Rc<RefCell<Box<ProxyPluginInterface>>>,
                              mut publish_packet_senders    : PublishSender,
                              system_message_pub_recv       : unsync::mpsc::Receiver<Arc<Publish>>) {
    let stream = proxy_plugin.borrow_mut().spawn_authorized_publisher();
    let handle = core.handle();
    let mut forwarders = Vec::new();
    let mut publish_forwarder_streams = Vec::new();
    let worker_count = publish_packet_senders.len();
    for i in 0..worker_count{
        let (mut publish_forwarder, publish_forwarder_stream) = unsync::mpsc::channel(4092);
        forwarders.push(publish_forwarder);
        publish_forwarder_streams.push(publish_forwarder_stream);
    }

    let fut = stream
        .map_err(|_| ())
        .and_then(move |(topic, payload)|{
            let publish = make_publish_packet(topic, payload);
            let packet = Arc::new(publish);
            Ok(packet)
        })
        .select(system_message_pub_recv)
        .map(move |_packet| {
                for forwarder in forwarders.iter_mut(){
                    forwarder.start_send(_packet.clone());
                }
            ()
        })
        .for_each(move |_|{
            Ok(())
        })
        .map_err(|_| ());

    handle.spawn(fut);

    for (forwarder_stream, packet_sender) in publish_forwarder_streams.into_iter().zip(publish_packet_senders.into_iter()).into_iter(){
        let f  = forwarder_stream.forward(packet_sender.sink_map_err(|_| ()))
            .then(|_| Ok::<(),()>(()));
        handle.spawn(f);
    }
}

fn spawn_watch_listener(handle: &Handle, 
                        lb    : Rc<RefCell<LoadBalancer>>,
                        port  : u16){
    let handle_debug        = handle.clone();
    let db_socket = format!("0.0.0.0:{}", port);
    let debug_listener = TcpListener::bind(&db_socket.parse().unwrap(), &handle).unwrap();

    let watch_listener = debug_listener.incoming().for_each(move |(sock, addr)|{
        info!("Gelen debug baglantisi {}", addr);
        let codec = LinesCodec::new(); 
        let framed = sock.framed(codec);

        let lb = lb.clone();

        let (sink, stream) = framed.split();

        let watch = stream.and_then(move |x| {
            let debug_data = if x.starts_with("lb"){
                format!("{}", lb.borrow())
            } else{
                String::new()
            };
            Ok(debug_data)
        })
        .map_err(|_| ())
        .forward(sink.sink_map_err(|_| ()))
        .and_then(move |_| {
            info!("Tcp debug connection is closed {}", addr);
            Ok(())
        });

        handle_debug.spawn(watch);

        Ok(())
    })
    .map_err(|_| ())
    .map(|_| ());

    handle.spawn(watch_listener);
}


struct SystemMetrics{
    connected_user_count    : usize,
    subscription_count      : usize,
    cl_connected_user_count : usize,
    cl_subscription_count   : usize,
}