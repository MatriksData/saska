use radix_trie::Trie;
use std;
use std::collections::HashMap;

pub trait Subscription{
    fn new() -> Self;
    
    fn subscribe(&mut self, ind: usize, mut path: String);
    
    fn unsubscribe(&mut self, ind: usize, mut path: &str);

    fn get_subscriptions(&self, path: &str) -> Vec<usize>;

    fn is_subscribed(&self, path: &str, ind: usize) -> bool{
        let subs = self.get_subscriptions(&path);
        for a in subs{
            if a == ind {
                return true;
            }            
        }
        false
    }
}

pub struct TrieSubscription{
    trie: Trie<String, Vec<usize>>
}

impl Subscription for TrieSubscription{
    fn new() -> TrieSubscription{
        TrieSubscription{
            trie: Trie::new()
        }
    }

    fn subscribe(&mut self, ind: usize, mut path: String){
        if path.ends_with('/'){
            path.pop();
        }
        let levels = path.split('/').collect::<Vec<&str>>();

        let mut pat = String::with_capacity(path.len());
        
        for i in 0..levels.len(){
            pat.push_str(levels[i]);
            
            if i == levels.len() - 1 {
                let mut exist = false;
                self.trie.get_mut(&pat).map(|x| { 
                    exist = true;
                    x.push(ind)
                });
                if !exist{
                    let mut v = Vec::with_capacity(50);
                    v.push(ind);
                    self.trie.insert(pat.clone(), v);
                }
            }
            pat.push('/');
        }
    }

    fn unsubscribe(&mut self, ind: usize, path: &str){
        self.trie
            .get_mut(path)
            .map(|x| *x = x.iter_mut()
                           .filter( |s| **s != ind)
                           .map(|a| *a)
                           .collect::<Vec<usize>>());
    }

    fn get_subscriptions(&self, path: &str) -> Vec<usize>{
        
        // Bu pathi kapsayan butun kombinasyonlari bul
        // level sayisi = N = path'teki 'slash' sayisi + 1
        // Olabilecek kombinasyon sayisi
            // # icin  N (wildcard)
            // + icin  2 ^ N (single wildcard)
            // pathin kendisi "single wildcard" olasiliklarinin icinde
            // Toplam N + 2^N 
        let levels = path.split('/').collect::<Vec<&str>>();
        let n = levels.len();
        let mut subscriptions  = Vec::with_capacity(n + 2u16.pow(n as u32) as usize);

        // Multi wildcard kombinasyonlari
        let mut pat = String::with_capacity(path.len());
        for i in 0..n{            
            pat.push('#');
            if let Some(s) = self.trie.get(&pat){
                subscriptions.extend(s);
            }
            pat.pop();
            pat.push_str(levels[i]);
            pat.push('/');
        }

        for i in 0..2u16.pow(n as u32){
            let mut pat = String::with_capacity(path.len());
            for k in 0..levels.len(){
                if ((i >> k) & 0x1) == 1{
                    pat.push_str(levels[k]);                    
                } else {
                    pat.push('+');
                }
                pat.push('/');
            }
            pat.pop();
            if let Some(s) = self.trie.get(&pat){
                subscriptions.extend(s);
            }
        } 

        subscriptions
    }
}

#[test]
fn test_subscribe(){
    let mut subs = TrieSubscription::new();
    let topic = "a/b/c";
    subs.subscribe(12, topic.to_owned());
    subs.subscribe(11, topic.to_owned());
    subs.subscribe(10, topic.to_owned());
    subs.subscribe(13, topic.to_owned());
    subs.subscribe(14, topic.to_owned());
    
    let clients = subs.get_subscriptions(topic);

    assert!(subs.is_subscribed(&topic, 10));
    assert!(subs.is_subscribed(&topic, 11));
    assert!(subs.is_subscribed(&topic, 12));
    assert!(subs.is_subscribed(&topic, 13));
    assert!(subs.is_subscribed(&topic, 14));
    assert!(!subs.is_subscribed(&topic, 15));
}

#[test]
fn test_subscribe2(){
    let mut subs = TrieSubscription::new();
    let topic = "a/+/c";
    subs.subscribe(12, topic.to_owned());
    subs.subscribe(11, topic.to_owned());

    let topic = "+/b/c";
    subs.subscribe(10, topic.to_owned());
    subs.subscribe(13, topic.to_owned());
    subs.subscribe(14, topic.to_owned());
    
    let topic = "a/b/c";
    let clients = subs.get_subscriptions(topic);

    assert!(subs.is_subscribed(&topic, 10));
    assert!(subs.is_subscribed(&topic, 11));
    assert!(subs.is_subscribed(&topic, 12));
    assert!(subs.is_subscribed(&topic, 13));
    assert!(subs.is_subscribed(&topic, 14));
    assert!(!subs.is_subscribed(&topic, 15));
}

#[test]
fn test_subscribe3(){
    let mut subs = TrieSubscription::new();
    let topic = "a/+/c";
    subs.subscribe(12, topic.to_owned());
    subs.subscribe(11, topic.to_owned());

    let topic = "+/b/c";
    subs.subscribe(10, topic.to_owned());
    subs.subscribe(13, topic.to_owned());
    subs.subscribe(14, topic.to_owned());
    
    let topic = "a/b/d";
    let clients = subs.get_subscriptions(topic);

    assert!(!subs.is_subscribed(&topic, 10));
    assert!(!subs.is_subscribed(&topic, 11));
    assert!(!subs.is_subscribed(&topic, 12));
    assert!(!subs.is_subscribed(&topic, 13));
}

#[test]
fn test_subscribe4(){
    let mut subs = TrieSubscription::new();
    let topic = "a/#";
    subs.subscribe(12, topic.to_owned());
    subs.subscribe(11, topic.to_owned());

    let topic = "+/b/c";
    subs.subscribe(10, topic.to_owned());
    subs.subscribe(13, topic.to_owned());
    subs.subscribe(14, topic.to_owned());
    
    let topic = "a/b/d";
    let clients = subs.get_subscriptions(topic);

    assert!(!subs.is_subscribed(&topic, 10));
    assert!(subs.is_subscribed(&topic, 11));
    assert!(subs.is_subscribed(&topic, 12));
    assert!(!subs.is_subscribed(&topic, 13));
}

#[test]
fn test_subscribe5(){
    let mut subs = TrieSubscription::new();
    let topic = "a/#";
    subs.subscribe(1, topic.to_owned());
    let topic = "a/";
    assert!(subs.is_subscribed(&topic, 1));
    let topic = "a";
    assert!(!subs.is_subscribed(&topic, 1));
}
