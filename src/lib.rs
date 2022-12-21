use eva_common::value::Value;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::net::IpAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use log::debug;

const ERR_UNSUPPORTED_ENCODING: &str = "Unsupported data encoding";
const ERR_INVALID_REQUEST: &str = "Invalid request";

#[path = "prelude.rs"]
pub mod prelude;

use eva_common::ERR_CODE_INTERNAL_RPC;
use eva_common::ERR_CODE_INVALID_PARAMS;
use eva_common::ERR_CODE_INVALID_REQUEST;
use eva_common::ERR_CODE_METHOD_NOT_FOUND;

#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(msg) = self.message.as_ref() {
            write!(f, "{}", msg)
        } else {
            write!(f, "JSON RPC Error")
        }
    }
}

impl From<eva_common::Error> for JsonRpcError {
    fn from(e: eva_common::Error) -> Self {
        Self {
            code: e.kind() as i16,
            message: e.message().map(ToOwned::to_owned),
        }
    }
}

impl JsonRpcError {
    pub fn new<T: fmt::Display>(code: i16, message: Option<T>) -> Self {
        Self {
            code,
            message: message.map(|v| v.to_string()),
        }
    }
    pub fn invalid_request<T: fmt::Display>(message: T) -> Self {
        Self::new(ERR_CODE_INVALID_REQUEST, Some(message))
    }
    pub fn method_not_found<T: fmt::Display>(message: T) -> Self {
        Self::new(ERR_CODE_METHOD_NOT_FOUND, Some(message))
    }
    pub fn invalid_params<T: fmt::Display>(message: T) -> Self {
        Self::new(ERR_CODE_INVALID_PARAMS, Some(message))
    }
    pub fn internal_rpc<T: fmt::Display>(message: T) -> Self {
        Self::new(ERR_CODE_INTERNAL_RPC, Some(message))
    }
}

pub type Credentials = (String, String);

#[derive(Debug, Clone)]
pub struct JsonRpcRequestMeta {
    source: RequestSource,
    credentials: Option<Credentials>,
}

#[derive(Debug, Clone)]
pub enum RequestSource {
    UnixSocket,
    Socket(IpAddr),
    Http(IpAddr, String),
    Internal(String),
    Mqtt(String),
    Custom(String),
}

#[allow(clippy::must_use_candidate)]
impl JsonRpcRequestMeta {
    #[cfg(not(target_os = "windows"))]
    pub fn unix_socket() -> Self {
        Self {
            source: RequestSource::UnixSocket,
            credentials: None,
        }
    }
    pub fn socket(ip: IpAddr) -> Self {
        Self {
            source: RequestSource::Socket(ip),
            credentials: None,
        }
    }
    pub fn http(ip: IpAddr, agent: String, credentials: Option<Credentials>) -> Self {
        Self {
            source: RequestSource::Http(ip, agent),
            credentials,
        }
    }
    pub fn internal(id: String) -> Self {
        Self {
            source: RequestSource::Internal(id),
            credentials: None,
        }
    }
    pub fn mqtt(id: String) -> Self {
        Self {
            source: RequestSource::Mqtt(id),
            credentials: None,
        }
    }
    pub fn custom(id: String) -> Self {
        Self {
            source: RequestSource::Custom(id),
            credentials: None,
        }
    }

    pub fn ip(&self) -> Option<IpAddr> {
        match self.source {
            RequestSource::Socket(addr) | RequestSource::Http(addr, _) => Some(addr),
            _ => None,
        }
    }
    pub fn credentials(&self) -> Option<&Credentials> {
        self.credentials.as_ref()
    }
    pub fn agent(&self) -> Option<&String> {
        match self.source {
            RequestSource::Http(_, ref agent) => Some(agent),
            _ => None,
        }
    }
}

const ENCODING_JSON: u8 = 1;
const ENCODING_MSGPACK: u8 = 2;

#[derive(PartialEq, Debug, Copy, Clone)]
#[repr(u8)]
pub enum Encoding {
    Json = ENCODING_JSON,
    MsgPack = ENCODING_MSGPACK,
}

impl TryFrom<u8> for Encoding {
    type Error = JsonRpcError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            x if x == Encoding::Json as u8 => Ok(Encoding::Json),
            x if x == Encoding::MsgPack as u8 => Ok(Encoding::MsgPack),
            _ => Err(JsonRpcError::invalid_request(ERR_UNSUPPORTED_ENCODING)),
        }
    }
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::Json
    }
}

#[macro_export]
macro_rules! jrpc_q {
    ($req: expr) => {
        if !$req.response_required() {
            return None;
        };
    };
}

macro_rules! impl_err_data {
    ($e: path) => {
        impl From<$e> for JsonRpcError {
            fn from(e: $e) -> JsonRpcError {
                JsonRpcError::internal_rpc(e)
            }
        }
    };
}

impl_err_data!(rmp_serde::decode::Error);
impl_err_data!(rmp_serde::encode::Error);
impl_err_data!(serde_json::Error);

fn unpack_data<'de, V: Deserialize<'de>>(src: &'de [u8], enc: Encoding) -> Result<V, JsonRpcError> {
    Ok(match enc {
        Encoding::Json => serde_json::from_slice(src)?,
        Encoding::MsgPack => rmp_serde::from_slice(src)?,
    })
}

fn pack_data<V: Serialize>(data: &V, enc: Encoding) -> Result<Vec<u8>, JsonRpcError> {
    Ok(match enc {
        Encoding::Json => serde_json::to_vec(data)?,
        Encoding::MsgPack => rmp_serde::to_vec_named(data)?,
    })
}

impl fmt::Display for Encoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Encoding::Json => "application/json",
                Encoding::MsgPack => "application/msgpack",
            }
        )
    }
}

#[derive(Debug)]
pub struct JrpcException {
    code: u16,
    headers: Vec<(String, String)>,
    content: String,
}

impl JrpcException {
    pub fn new(code: u16, content: String) -> Self {
        Self {
            code,
            headers: Vec::new(),
            content,
        }
    }
    #[inline]
    pub fn set_header(&mut self, header: String, value: String) {
        self.headers.push((header, value));
    }
    #[inline]
    pub fn code(&self) -> u16 {
        self.code
    }
    #[inline]
    pub fn content(&self) -> &str {
        &self.content
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcResponse {
    jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    #[serde(skip, default)]
    pub encoding: Encoding,
    #[serde(skip, default)]
    pub jexception: Option<JrpcException>,
}

#[allow(clippy::must_use_candidate)]
impl JsonRpcResponse {
    /// # Errors
    ///
    /// Will return `Err` if failed to pack the response
    pub fn pack(&self) -> Result<Vec<u8>, JsonRpcError> {
        pack_data(self, self.encoding)
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to unpack the response
    pub fn unpack(src: &[u8], encoding: Encoding) -> Result<Self, JsonRpcError> {
        let response: Self = unpack_data(src, encoding)?;
        if response.is_valid() {
            Ok(response)
        } else {
            Err(JsonRpcError::invalid_request(ERR_INVALID_REQUEST))
        }
    }

    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.jsonrpc == "2.0"
    }

    pub fn is_ok(&self) -> bool {
        self.error.is_none()
    }

    pub fn is_err(&self) -> bool {
        self.error.is_some()
    }
    /// jexceptions special exception
    ///
    /// If jexceptions, batches stop processing
    /// JrpcException is
    pub fn jexception(&mut self, jexception: JrpcException) {
        self.jexception = Some(jexception);
    }
    pub fn take_jexception(&mut self) -> Option<JrpcException> {
        self.jexception.take()
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct JsonRpcRequest {
    jsonrpc: String,
    id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    #[serde(skip, default)]
    encoding: Encoding,
}

#[allow(clippy::must_use_candidate)]
impl<'a> JsonRpcRequest {
    #[inline]
    pub fn new(id: Option<Value>, method: &str, params: Option<Value>, encoding: Encoding) -> Self {
        Self {
            id,
            jsonrpc: "2.0".to_owned(),
            method: method.to_owned(),
            params,
            encoding,
        }
    }
    #[inline]
    pub fn set_params(&mut self, value: Value) {
        self.params = Some(value);
    }
    #[inline]
    pub fn response_required(&self) -> bool {
        self.id.is_some()
    }
    #[inline]
    pub fn respond(&self, result: Value) -> Option<JsonRpcResponse> {
        self.id.as_ref().map(|id| JsonRpcResponse {
            jsonrpc: self.jsonrpc.clone(),
            id: id.clone(),
            result: Some(result),
            error: None,
            encoding: self.encoding,
            jexception: None,
        })
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to unpack the request
    #[inline]
    pub fn unpack(src: &[u8], encoding: Encoding) -> Result<Self, JsonRpcError> {
        let request: JsonRpcRequest = unpack_data(src, encoding)?;
        if request.is_valid() {
            Ok(request)
        } else {
            Err(JsonRpcError::invalid_request(ERR_INVALID_REQUEST))
        }
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to pack the request
    #[inline]
    pub fn pack(&self) -> Result<Vec<u8>, JsonRpcError> {
        pack_data(self, self.encoding)
    }

    #[inline]
    pub fn respond_ok(&self) -> Option<JsonRpcResponse> {
        let mut map = BTreeMap::new();
        map.insert(Value::String("ok".to_owned()), Value::Bool(true));
        self.respond(Value::Map(map))
    }

    #[inline]
    pub fn is_valid(&self) -> bool {
        self.jsonrpc == "2.0"
    }

    #[inline]
    pub fn error(&self, err: JsonRpcError) -> Option<JsonRpcResponse> {
        self.id.as_ref().map(|id| JsonRpcResponse {
            jsonrpc: self.jsonrpc.clone(),
            id: id.clone(),
            result: None,
            error: Some(err),
            encoding: self.encoding,
            jexception: None,
        })
    }

    #[inline]
    pub fn jexception(&self, jexception: JrpcException) -> Option<JsonRpcResponse> {
        self.id.as_ref().map(|id| JsonRpcResponse {
            jsonrpc: self.jsonrpc.clone(),
            id: id.clone(),
            result: None,
            error: None,
            encoding: self.encoding,
            jexception: Some(jexception),
        })
    }

    #[inline]
    pub fn method_not_found(&self) -> Option<JsonRpcResponse> {
        self.error(JsonRpcError::method_not_found(&self.method))
    }

    #[inline]
    pub fn invalid_params(&'a self, message: &'a str) -> Option<JsonRpcResponse> {
        self.error(JsonRpcError::invalid_params(message))
    }

    #[inline]
    pub fn take_params(&'a mut self) -> Option<Value> {
        self.params.take()
    }
}

/*
 * Universal binary proto header (4 bytes)
 * Byte 0: data encoding (1 - Json, 2 - MsgPack), u8
 * Byte 1-3: frame length, u32
 */
#[derive(Debug)]
pub struct JsonRpcBatch {
    pub single: bool,
    requests: Vec<JsonRpcRequest>,
    pub responses: Vec<JsonRpcResponse>,
    pub encoding: Encoding,
    id: u32,
    meta: JsonRpcRequestMeta,
    jexception: Option<JrpcException>,
}

impl JsonRpcBatch {
    #[must_use]
    pub fn new(meta: JsonRpcRequestMeta) -> Self {
        JsonRpcBatch {
            single: true,
            requests: Vec::new(),
            responses: Vec::new(),
            encoding: Encoding::Json,
            id: 0,
            meta,
            jexception: None,
        }
    }

    pub fn add(&mut self, req: JsonRpcRequest) {
        if !self.requests.is_empty() {
            self.single = false;
        }
        self.requests.push(req);
    }

    pub fn prepare(&mut self, method: &str, params: Option<Value>) -> u32 {
        self.id += 1;
        let req = JsonRpcRequest::new(Some(Value::U32(self.id)), method, params, self.encoding);
        self.add(req);
        self.id
    }

    pub fn prepare_muted(&mut self, method: &str, params: Option<Value>) -> u32 {
        let req = JsonRpcRequest::new(None, method, params, self.encoding);
        self.add(req);
        self.id
    }

    pub async fn process<F, Fut>(&mut self, mut processor: F)
    where
        F: FnMut(JsonRpcRequest, JsonRpcRequestMeta) -> Fut,
        Fut: std::future::Future<Output = Option<JsonRpcResponse>>,
    {
        while let Some(req) = self.requests.pop() {
            if let Some(mut v) = processor(req, self.meta.clone()).await {
                if let Some(jexception) = v.take_jexception() {
                    self.jexception = Some(jexception);
                    break;
                }
                self.responses.push(v);
            }
        }
    }

    pub fn take_jexception(&mut self) -> Option<JrpcException> {
        self.jexception.take()
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to serialize request(s)
    pub fn pack_requests(
        &self,
        encoding: Option<Encoding>,
    ) -> Result<Option<Vec<u8>>, JsonRpcError> {
        let enc = encoding.unwrap_or(self.encoding);
        Ok(if self.requests.is_empty() {
            None
        } else if self.single {
            if let Some(v) = self.requests.get(0) {
                Some(pack_data(v, enc)?)
            } else {
                None
            }
        } else {
            Some(pack_data(&self.requests, enc)?)
        })
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to serialize response(s)
    pub fn pack_responces(
        &self,
        encoding: Option<Encoding>,
    ) -> Result<Option<Vec<u8>>, JsonRpcError> {
        let enc = encoding.unwrap_or(self.encoding);
        Ok(if self.responses.is_empty() {
            None
        } else if self.single {
            if let Some(v) = self.responses.get(0) {
                Some(pack_data(&v, enc)?)
            } else {
                None
            }
        } else {
            Some(pack_data(&self.responses, enc)?)
        })
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to deserialize requests(s)
    pub fn unpack_requests(
        &mut self,
        src: &[u8],
        encoding: Option<Encoding>,
    ) -> Result<(), JsonRpcError> {
        let enc = encoding.unwrap_or(self.encoding);
        match enc {
            Encoding::Json => {
                if let Ok(v) = serde_json::from_slice(src) {
                    self.requests = v;
                    self.single = false;
                } else {
                    let request: JsonRpcRequest = serde_json::from_slice(src)?;
                    self.requests.clear();
                    self.requests.push(request);
                    self.single = true;
                }
            }
            Encoding::MsgPack => {
                if let Ok(v) = rmp_serde::from_slice(src) {
                    self.requests = v;
                    self.single = false;
                } else {
                    let request: JsonRpcRequest = rmp_serde::from_slice(src)?;
                    self.requests.clear();
                    self.requests.push(request);
                    self.single = true;
                }
            }
        };
        for r in &self.requests {
            if !r.is_valid() {
                return Err(JsonRpcError::invalid_request(ERR_INVALID_REQUEST));
            }
        }
        Ok(())
    }

    /// # Errors
    ///
    /// Will return `Err` if failed to deserialize response(s)
    pub fn unpack_responses(
        &mut self,
        src: &[u8],
        encoding: Option<Encoding>,
    ) -> Result<(), JsonRpcError> {
        if src.is_empty() {
            return Ok(());
        }
        let enc = encoding.unwrap_or(self.encoding);
        match enc {
            Encoding::Json => {
                if let Ok(v) = serde_json::from_slice(src) {
                    self.responses = v;
                    self.single = false;
                } else {
                    let response: JsonRpcResponse = serde_json::from_slice(src)?;
                    self.responses.clear();
                    self.responses.push(response);
                    self.single = true;
                }
            }
            Encoding::MsgPack => {
                if let Ok(v) = rmp_serde::from_slice(src) {
                    self.responses = v;
                    self.single = false;
                } else {
                    let response: JsonRpcResponse = rmp_serde::from_slice(src)?;
                    self.responses.clear();
                    self.responses.push(response);
                    self.single = true;
                }
            }
        };
        Ok(())
    }
}

#[cfg(feature = "http")]
#[path = "http.rs"]
pub mod http;

macro_rules! parse_request_header {
    ($stream:expr, $buf:expr, $encoding:expr, $len:expr) => {
        match $stream.read_exact(&mut $buf).await {
            Ok(_) => {
                $encoding = match $buf[0].try_into() {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("{}", e);
                        break;
                    }
                };
                $len = u32::from_le_bytes([$buf[1], $buf[2], $buf[3], $buf[4]]);
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(e) => {
                debug!("API read error {}", e);
                break;
            }
        }
    };
}

pub async fn jrpc_stream_worker<T, F, Fut>(
    stream: &mut T,
    meta: JsonRpcRequestMeta,
    mut processor: F,
) where
    F: FnMut(JsonRpcRequest, JsonRpcRequestMeta) -> Fut,
    Fut: std::future::Future<Output = Option<JsonRpcResponse>>,
    T: AsyncReadExt + AsyncWriteExt + Unpin,
{
    loop {
        let mut header = [0_u8; 5];
        let encoding: Encoding;
        let frame_len: u32;
        parse_request_header!(stream, header, encoding, frame_len);
        let mut buf: Vec<u8> = vec![0; frame_len as usize];
        match stream.read_exact(&mut buf).await {
            Ok(_) => {
                let mut batch = JsonRpcBatch::new(meta.clone());
                batch.encoding = encoding;
                match batch.unpack_requests(&buf, None) {
                    Ok(_) => {}
                    Err(e) => {
                        debug!("JSON RPC payload unpack error {}", e);
                        break;
                    }
                }
                #[allow(clippy::redundant_closure)]
                batch.process(|x, m| processor(x, m)).await;
                if let Some(_jexception) = batch.take_jexception() {
                    // can't send jexception to socket stream
                    // just closing it
                    break;
                }
                let response_buf = match batch.pack_responces(None) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!("JSON RPC response serialize error {}", e);
                        break;
                    }
                };
                if let Some(resp) = response_buf {
                    let mut response = vec![header[0]];
                    #[allow(clippy::cast_possible_truncation)]
                    response.extend((resp.len() as u32).to_le_bytes());
                    response.extend(resp);
                    match stream.write_all(&response).await {
                        Ok(_) => {}
                        Err(e) => {
                            debug!("JSON RPC API response write error {}", e);
                            break;
                        }
                    };
                }
            }
            Err(e) => {
                debug!("Socket error {}", e);
                break;
            }
        }
    }
}

impl From<std::net::SocketAddr> for JsonRpcRequestMeta {
    fn from(addr: std::net::SocketAddr) -> Self {
        JsonRpcRequestMeta::socket(addr.ip())
    }
}

#[cfg(not(target_os = "windows"))]
impl From<tokio::net::unix::SocketAddr> for JsonRpcRequestMeta {
    fn from(_addr: tokio::net::unix::SocketAddr) -> Self {
        JsonRpcRequestMeta::unix_socket()
    }
}

#[macro_export]
macro_rules! jrpc_stream_server {
    ($listener: expr, $processor: expr) => {
        loop {
            match $listener.accept().await {
                Ok((mut stream, addr)) => {
                    debug!("JSON RPC server new connection: {:?}", addr);
                    let meta = addr.into();
                    tokio::spawn(async move {
                        jrpc_stream_worker(&mut stream, meta, processor).await;
                    });
                }
                Err(e) => {
                    debug!("JSON RPC server errror: {}", e);
                }
            }
        }
    };
}
