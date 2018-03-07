#[derive(Debug)]
pub enum Error{
    AuthnFailed,
    AuthzFailed,
    ConnectEventFailed,
    MulticonnectSameThread,
    MulticonnectOtherThread,
    Graceful,
    EventError,
    StreamClosed,
    ExternalDisconnect,
    ConnectAfterConnect,
    WrongFirstPacket,
    ParsingError,
    WrongClientId,
    PasswordError,
    KeepAliveExpired,
    KeepAliveTimerError,
    UnSupportedPacket,
    SlowSender,
    WriterError,
    Other
}

