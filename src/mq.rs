use futures::sync::mpsc::{channel, Receiver, Sender};

pub struct Multiqueue<T>{
    senders     : Vec<Vec<Sender<T>>>,
    receivers   : Vec<Receiver<T>>,
    all_senders : Vec<Sender<T>> 
}


impl <T> Multiqueue <T>{
    pub fn new(node: usize) -> Multiqueue<T>{
        let mut senders     = vec![];
        let mut receivers   = vec![];
        let mut all_senders = vec![];

        for _ in 0..node {
            let (s, r) = channel(2048);
            all_senders.push(s);
            receivers.push(r);
        } 
        
        for i in 0..node {
            let mut _senders = vec![];
            for k in 0..node {
                if i != k {
                    _senders.push(all_senders[k].clone());
                }
            }
            senders.push(_senders);
        }

        Multiqueue{
            senders,
            receivers,
            all_senders
        }
    }

    pub fn pop(&mut self) -> Option<(Vec<Sender<T>>, Receiver<T>)> {
        if let Some(x) =  self.senders.pop(){
            if let Some(y) = self.receivers.pop(){
                return Some((x, y));
            } else{
                None
            }            
        } else {
            None
        }
    }

    pub fn get_all_senders(&self) -> Vec<Sender<T>>{
        return self.all_senders.clone();
    }
}

