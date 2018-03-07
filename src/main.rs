extern crate tokio_core;
extern crate tokio_io;
extern crate futures;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate simplelog;
extern crate websocket;
extern crate mqtt3;
extern crate rand;
extern crate radix_trie;
extern crate trust_dns_resolver;
extern crate plugin;
extern crate plugin_interface;
extern crate toml;
extern crate serde;
#[macro_use]
extern crate serde_derive;

mod proxy;
mod load_balancer;
mod codec;
mod client;
mod broker;
mod subscription;
mod error;
mod mq;
// mod broker_fut;
mod client_fut;
mod config;
mod worker;

use std::fs::File;
use simplelog::{Config, TermLogger, WriteLogger, CombinedLogger, LogLevelFilter};
use std::rc::Rc;
use mq::Multiqueue;
use worker::Worker;
use futures::sync::mpsc::{channel, Sender, Receiver};

fn main() {
    // Build Configs
    let config = config::Config::build();
    
    let term_log_level = config.get_terminal_log_level();
    let log_level      = config.get_log_level();

    CombinedLogger::init(
        vec![
            // TermLogger::new(term_log_level, Config::default()).unwrap(),
            WriteLogger::new(log_level, 
                             Config::default(), 
                             File::create(config.get_log_path()).unwrap()),
        ]
    ).unwrap();

    let mut client_conn_senders = vec![];

    let worker_count = config.get_worker_count();

    // publish pakedi geldiginde diger threadlerdeki ilgili clientlerin alabilmesi icin
    let mut mq = Multiqueue::new(worker_count+1);

    // Publish packet broadcaster for authorized input stream.
    let mut pb = Multiqueue::new(worker_count);

    let publish_packet_sender_auth = pb.get_all_senders();

    for i in 0..worker_count{
        let (con_sender, con_receiver) = channel(2048);

        let (system_message_sender, system_message_receiver) = mq.pop().expect("Error: mq");
        let (pb_sendr,  pb_recv)  = pb.pop().expect("Error: pb");

        client_conn_senders.push(con_sender);

        let config_cl = config.clone();

        let _ = std::thread::spawn(move || {
            let mut worker = Worker::new(i, config_cl);
            worker.start(con_receiver, system_message_sender, system_message_receiver, pb_sendr, pb_recv);
        });
    
    }
    
    std::thread::sleep_ms(1000);
    
    let (system_message_sender, system_message_receiver) = mq.pop().expect("Error: mq");
    
    proxy::proxy(config.clone(), client_conn_senders, publish_packet_sender_auth, system_message_sender, system_message_receiver);
}