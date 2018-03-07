extern crate futures;
extern crate tokio_core;
extern crate mqtt3;

use futures::{Future, Stream, Poll, Async};
use futures::future::{FutureResult, result};
use std::io;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use std::convert::Into;
use tokio_core::reactor::{Core, Handle};
use mqtt3::{Connect, Publish, Subscribe, Unsubscribe};

// Uygulamada kullanilacak plugin icin arayuz 
// Bu traiti implemente etmis yapilar uygulamadan direk erisilmeyip
// trait objesi haline getirilip kullanilacak(Box<PluginInterface>)

pub type PluginObject<T> = Box<PluginInterface<T>>;
pub type SystemMetricsPluginObject = Box<ProxyPluginInterface>;
pub type AuthorizedPublisher = Box<Stream<Item = (String, Vec<u8>), Error= ()>>;

type NewPluginFn<T>    = extern fn(&mut Core) -> PluginObject<T>;
type SystemMetricsPluginFn = extern fn() -> SystemMetricsPluginObject;
type NewAuthzPublishFn = extern fn(&mut Core) -> AuthorizedPublisher;

pub trait PluginInterface<T: BrokerInterface>{
    fn new(handle : Handle, plugin_no: usize) -> Self where Self: Sized;
    fn handle_authn(&mut self, client_index: usize, broker: Rc<RefCell<T>>, connect_packet: &Connect) -> AuthnFut;
    fn handle_subscribe_authz(&mut self, client_index: usize, broker: Rc<RefCell<T>>, connect_packet: &Subscribe) -> AuthzFut;
    fn handle_publish_authz(&mut self, client_index: usize, broker: Rc<RefCell<T>>, connect_packet: &Publish) -> AuthzFut;
    fn handle_connect(&mut self, client_index: usize, broker: Rc<RefCell<T>>, connect_packet: &Connect);
    fn handle_subscribe(&mut self, client_index: usize, broker: Rc<RefCell<T>>, subscribe_packet: &Subscribe, authz_results: &[bool]);
    fn handle_unsubscribe(&mut self, client_index: usize, broker: Rc<RefCell<T>>, unsubscribe_packet: &Unsubscribe);
    fn handle_disconnect(&mut self, client_index: usize, broker: Rc<RefCell<T>>);
    fn handle_close(&mut self, client_index: usize, broker: Rc<RefCell<T>>, close_reason: CloseReason);
}

pub trait BrokerInterface{
    fn subscribe(&mut self, client_index: usize, topics: &[String]);
    fn unsubscribe(&mut self, client_index: usize, topics: &[String]);
    fn publish(&mut self, topic: &str, payload: Vec<u8>);
    fn publish_to_client(&mut self, client_index: usize, topic: &str, payload: Vec<u8>);
}

pub enum AuthnFut{
    BoxedFuture(Box<Future<Item = String, Error=()>>),
    ResolvedFuture(FutureResult<String, ()>)
}

impl Future for AuthnFut{
    type Item  = String;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error>{
        match self{
            &mut AuthnFut::BoxedFuture(ref mut fut) => {
                return fut.poll();
            }
            &mut AuthnFut::ResolvedFuture(ref mut fut) => {
                return fut.poll();
            }
        }
    }
}

impl AuthnFut{
    pub fn err() -> Self{
        AuthnFut::ResolvedFuture(result(Err(())))
    }
}

pub enum AuthzFut{
    BoxedFuture(Box<Future<Item = Vec<bool>, Error=()>>),
    ResolvedFuture(FutureResult<Vec<bool>, ()>)
}

impl Future for AuthzFut{
    type Item  = Vec<bool>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error>{
        match self{
            &mut AuthzFut::BoxedFuture(ref mut fut) => {
                return fut.poll();
            }
            &mut AuthzFut::ResolvedFuture(ref mut fut) => {
                return fut.poll();
            }
        }
    }
}

impl AuthzFut{
    pub fn ok(topic_count: usize) -> Self{
        AuthzFut::ResolvedFuture(result(Ok(vec![true; topic_count])))
    }
    
    pub fn err(topic_count: usize) -> Self{
        AuthzFut::ResolvedFuture(result(Ok(vec![false; topic_count])))
    }
}

#[derive(Debug, Clone)]
pub enum CloseReason{
    Graceful,
    Timeout, 
    AuthnFailed,
    Multiconnect,
    Other
}

pub trait ProxyPluginInterface {
    fn spawn_authorized_publisher(&mut self) -> AuthorizedPublisher;
    fn handle_subscribe(&mut self, user_name: &str, topics: Arc<Vec<String>>);
    fn update_connected_user_count(&mut self, connected_user_count: usize);
    fn update_subscription_count(&mut self, subscription_count: usize);
    fn update_cl_connected_user_count(&mut self, cl_connected_user_count: usize);
    fn update_cl_subscription_count(&mut self, cl_subscription_count: usize);
}