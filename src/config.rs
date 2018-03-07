#[macro_use]
use serde_derive;
use toml::{from_str};
use serde;
use std::env;
use std::str::FromStr;
use std::fs::File;
use std::path::Path;
use std::net::{SocketAddr, SocketAddrV4};
use std::io::Read;
use trust_dns_resolver::Resolver;
use trust_dns_resolver::config::*;
use simplelog::LogLevelFilter;

#[derive(Serialize, Deserialize, Clone)]
pub struct Multicast {
    multicast_addr   : String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Config{
    worker_thread_count  : usize,
    first_packet_timeout : u64, 
    allow_mc             : bool,
    tcp_port             : u16,
    ws_port              : u16,
    db_port              : u16,
    mqtt_keep_alive_ms   : u64,
    system_message_update_period : u64, 
    log_path             : String,
    // multicast            : Multicast,
    log_level            : String,
    terminal_log_level   : String,
    broker_url           : String,
    broker_type          : String,
    sink_buf_size        : usize,
    chan_buf_size        : usize,
    multicast_port       : String,
    multicast_ip         : String
}

impl Config{
    pub fn build() -> Config{
        let _path = env::current_dir().expect("Couldn't get current dir!");
        let mut config_file = File::open(_path.join("config.toml")).expect("Config file is not found!");
        let mut config_str = String::new();
        config_file.read_to_string(&mut config_str).expect("Couldn't read config file!");
        let decoded: Config = from_str(&config_str).expect("Couldn't decode config file!");
        decoded
    }

    pub fn get_multicast_port(&self) -> &str{
        &self.multicast_port
    }
    pub fn get_multicast_ip(&self) -> &str{
        &self.multicast_ip
    }

    pub fn get_broker_url(&self) -> &str{
        &self.broker_url
    }

    pub fn get_broker_type(&self) -> &str{
        &self.broker_type
    }
    
    pub fn get_worker_count(&self) -> usize{
        self.worker_thread_count
    }

    pub fn get_allow_mc(&self) -> bool{
        self.allow_mc
    }

    pub fn get_log_path(&self) -> &str{
        &self.log_path
    }

    pub fn get_tcp_port(&self) -> u16{
        self.tcp_port
    }

    pub fn get_ws_port(&self) -> u16{
        self.ws_port
    }

    pub fn get_db_port(&self) -> u16{
        self.db_port
    }

    pub fn get_mqtt_keep_alive(&self) -> Option<u64> {
        let v = self.mqtt_keep_alive_ms;
        if v == 0 {
            return None;
        } else {
            return Some(v);
        }
    }

    pub fn get_sink_buf_size(&self) -> usize{
        self.sink_buf_size
    }

    pub fn get_chan_buf_size(&self) -> usize{
        self.chan_buf_size
    }

    pub fn get_terminal_log_level(&self) -> LogLevelFilter{
        match self.terminal_log_level.as_ref(){
            "Debug" | "debug" =>{
                LogLevelFilter::Debug
            },
            "Info" | "info" => {
                LogLevelFilter::Info
            },
            "Warn" | "warn" => {
                LogLevelFilter::Warn
            },
            "Error" | "error" => {
                LogLevelFilter::Error
            },
            _ => {
                LogLevelFilter::Error
            }
        }
    }

    pub fn get_log_level(&self) -> LogLevelFilter{
        match self.log_level.as_ref(){
            "Debug" | "debug" => {
                LogLevelFilter::Debug
            },
            "Info" | "info"   => {
                LogLevelFilter::Info
            },
            "Warn" | "warn"   => {
                LogLevelFilter::Warn
            },
            "Error" | "error" => {
                LogLevelFilter::Error
            },
            _ => {
                LogLevelFilter::Error
            }
        }
    }

    pub fn get_first_packet_timeout(&self) -> u64{
        self.first_packet_timeout
    }

    pub fn get_system_message_update_period(&self) -> u64{
        self.system_message_update_period
    }
}