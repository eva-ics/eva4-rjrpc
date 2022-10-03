use crate::prelude::*;
use crate::{JsonRpcError, JsonRpcRequestMeta};
use eva_common::value::Value;
use hyper::{
    client::connect::HttpConnector, header::HeaderValue, http, Body, Client, Method, Request,
    Response, StatusCode,
};
use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::net::IpAddr;
use std::time::Duration;

macro_rules! response {
    ($code: expr, $e: expr) => {
        Response::builder().status($code).body(Body::from($e))
    };
}

impl TryFrom<&HeaderValue> for Encoding {
    type Error = JsonRpcError;
    fn try_from(value: &HeaderValue) -> Result<Self, Self::Error> {
        match value.to_str().unwrap_or("") {
            "application/json" => Ok(Encoding::Json),
            "application/msgpack" | "application/x-msgpack" => Ok(Encoding::MsgPack),
            _ => Err(JsonRpcError::invalid_request(
                crate::ERR_UNSUPPORTED_ENCODING,
            )),
        }
    }
}

pub struct HyperJsonRpcClient {
    client: Client<HttpConnector>,
    uri: String,
    encoding: Encoding,
    id: u32,
    timeout: Duration,
    http2: bool,
    http2_keep_alive_interval: Option<Duration>,
}

macro_rules! http_request {
    ($client: expr, $uri: expr, $encoding: expr, $payload: expr) => {
        match Request::builder()
            .method(Method::POST)
            .header(hyper::header::CONTENT_TYPE, $encoding.to_string())
            .uri($uri)
            .body(Body::from($payload))
        {
            Ok(req) => match $client.request(req).await {
                Ok(v) => match v.status() {
                    StatusCode::OK => match hyper::body::to_bytes(v).await {
                        Ok(v) => Some(v),
                        Err(e) => {
                            return Err(JsonRpcError::internal_rpc(e));
                        }
                    },
                    StatusCode::NO_CONTENT => None,
                    _ => {
                        return Err(JsonRpcError::internal_rpc(format!(
                            "Server HTTP response: {}",
                            v.status()
                        )));
                    }
                },
                Err(e) => {
                    return Err(JsonRpcError::internal_rpc(e));
                }
            },
            Err(e) => {
                return Err(JsonRpcError::internal_rpc(e));
            }
        }
    };
}

impl HyperJsonRpcClient {
    #[must_use]
    pub fn new(uri: &str, encoding: Encoding) -> Self {
        Self {
            uri: uri.to_owned(),
            encoding,
            id: 1,
            client: Client::new(),
            timeout: Duration::from_secs(3600),
            http2: false,
            http2_keep_alive_interval: None,
        }
    }

    pub fn set_timeout(&mut self, timeout: Duration) {
        self.timeout = timeout;
        self.rebuild_client();
    }

    fn rebuild_client(&mut self) {
        self.client = Client::builder()
            .http2_only(self.http2)
            .http2_keep_alive_interval(self.http2_keep_alive_interval)
            .pool_idle_timeout(self.timeout)
            .build_http();
    }

    pub fn force_http2(&mut self, keep_alive_interval: Duration) {
        self.http2 = true;
        self.http2_keep_alive_interval = Some(keep_alive_interval);
        self.rebuild_client();
    }

    /// # Errors
    ///
    /// Will return `Err` on server and serde errors
    pub async fn call_batch(&self, batch: &mut JsonRpcBatch) -> Result<(), JsonRpcError> {
        match batch.pack_requests(Some(self.encoding))? {
            Some(data) => {
                let data = match http_request!(self.client, &self.uri, self.encoding, data) {
                    Some(v) => v,
                    None => return Ok(()),
                };
                batch.unpack_responses(&data, Some(self.encoding))
            }
            None => Ok(()),
        }
    }

    /// # Errors
    ///
    /// Will return `Err` on server and serde errors
    pub async fn call(
        &mut self,
        method: &str,
        params: Option<Value>,
    ) -> Result<JsonRpcResponse, JsonRpcError> {
        if self.id == std::u32::MAX {
            self.id = 0;
        }
        self.id += 1;
        let req = JsonRpcRequest::new(Some(Value::U32(self.id)), method, params, self.encoding);
        let payload = match http_request!(self.client, &self.uri, self.encoding, req.pack()?) {
            Some(v) => v,
            None => {
                return Err(JsonRpcError::internal_rpc("Empty response received"));
            }
        };
        let response = JsonRpcResponse::unpack(&payload, self.encoding)?;
        Ok(response)
    }
}

pub struct HyperJsonRpcServer {
    uris: Vec<String>,
    get_uris: Option<Vec<String>>,
}

impl Default for HyperJsonRpcServer {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperJsonRpcServer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            uris: Vec::new(),
            get_uris: None,
        }
    }

    /// # Panics
    ///
    /// Will panic if the specified method is neither GET nor POST
    pub fn serve_at(&mut self, uri: &str, method: &Method) {
        match *method {
            Method::GET => {
                if let Some(ref mut v) = self.get_uris {
                    v.push(uri.to_owned());
                } else {
                    let v = vec![uri.to_owned()];
                    self.get_uris = Some(v);
                }
            }
            Method::POST => self.uris.push(uri.to_owned()),
            _ => unimplemented!(),
        }
    }

    pub fn matches(&self, parts: &http::request::Parts) -> bool {
        let path = parts.uri.path();
        match parts.method {
            Method::GET => match &self.get_uris {
                Some(uris) => {
                    for u in uris {
                        if u.as_str() == path {
                            return true;
                        }
                    }
                    false
                }
                None => false,
            },
            Method::POST => {
                for u in &self.uris {
                    if u.as_str() == path {
                        return true;
                    }
                }
                false
            }
            _ => false,
        }
    }

    /// # Panics
    ///
    /// Should not panic
    ///
    /// # Errors
    ///
    /// Will return `Err` if failed to build the http response
    #[allow(clippy::too_many_lines)]
    pub async fn process<'de, F, Fut>(
        &self,
        mut processor: F,
        parts: &http::request::Parts,
        body: Body,
        ip: IpAddr,
    ) -> Result<Response<Body>, http::Error>
    where
        F: FnMut(JsonRpcRequest, JsonRpcRequestMeta) -> Fut,
        Fut: std::future::Future<Output = Option<JsonRpcResponse>>,
    {
        let mut batch;
        let encoding;
        let user_agent: String = parts
            .headers
            .get("user-agent")
            .map_or_else(|| "".to_owned(), |v| v.to_str().unwrap_or("").to_owned());
        let credentials = if let Some(authorization) = parts.headers.get("authorization") {
            if let Ok(auth) = authorization.to_str() {
                let mut sp = auth.splitn(2, ' ');
                let scheme = sp.next().unwrap();
                if let Some(params) = sp.next() {
                    if scheme.to_lowercase() == "basic" {
                        match base64::decode(params) {
                            Ok(ref v) => match std::str::from_utf8(v) {
                                Ok(s) => {
                                    let mut sp = s.splitn(2, ':');
                                    let username = sp.next().unwrap();
                                    if let Some(password) = sp.next() {
                                        Some((username.to_owned(), password.to_owned()))
                                    } else {
                                        return response!(
                                            StatusCode::BAD_REQUEST,
                                            "Basic authorization error: password not specified"
                                        );
                                    }
                                }
                                Err(e) => {
                                    return response!(
                                        StatusCode::BAD_REQUEST,
                                        format!("Unable to parse credentials string: {}", e)
                                    );
                                }
                            },
                            Err(e) => {
                                return response!(
                                    StatusCode::BAD_REQUEST,
                                    format!("Unable to decode credentials: {}", e)
                                );
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    return response!(StatusCode::BAD_REQUEST, "Invalid authorization header");
                }
            } else {
                return response!(
                    StatusCode::BAD_REQUEST,
                    "Unable to decode authorization header"
                );
            }
        } else {
            None
        };
        let meta = JsonRpcRequestMeta::http(ip, user_agent, credentials);
        match parts.method {
            Method::GET => {
                encoding = Encoding::Json;
                batch = JsonRpcBatch::new(meta);
                batch.encoding = encoding;
                let mut qs: BTreeMap<String, String> = match parts.uri.query().map(|v| {
                    url::form_urlencoded::parse(v.as_bytes())
                        .into_owned()
                        .collect()
                }) {
                    Some(v) => v,
                    None => {
                        return response!(StatusCode::BAD_REQUEST, "Query string error");
                    }
                };
                #[allow(clippy::redundant_closure)]
                let id = qs.remove("i").map(|id| Value::String(id));
                let method = match qs.remove("m") {
                    Some(m) => m,
                    None => {
                        return response!(StatusCode::BAD_REQUEST, "Method not specified");
                    }
                };
                let params: BTreeMap<String, Value> = match qs.remove("p") {
                    Some(p) => match serde_json::from_str(&p) {
                        Ok(v) => v,
                        Err(e) => {
                            return response!(
                                StatusCode::BAD_REQUEST,
                                format!("Parameters parse error: {}", e)
                            );
                        }
                    },
                    None => BTreeMap::new(),
                };
                let request = JsonRpcRequest {
                    id,
                    jsonrpc: String::new(),
                    method,
                    encoding,
                    params: Some(match eva_common::value::to_value(params) {
                        Ok(v) => v,
                        Err(e) => {
                            return response!(
                                StatusCode::BAD_REQUEST,
                                format!("Parameters encode error: {}", e)
                            );
                        }
                    }),
                };
                batch.add(request);
            }
            Method::POST => {
                let b = match hyper::body::to_bytes(body).await {
                    Ok(v) => v,
                    Err(e) => {
                        return response!(StatusCode::BAD_REQUEST, format!("{}", e));
                    }
                };
                encoding = match parts.headers.get(hyper::header::CONTENT_TYPE) {
                    None => Encoding::Json,
                    Some(v) => match v.try_into() {
                        Ok(encoding) => encoding,
                        Err(e) => {
                            return response!(StatusCode::BAD_REQUEST, e.to_string());
                        }
                    },
                };
                batch = JsonRpcBatch::new(meta);
                batch.encoding = encoding;
                match batch.unpack_requests(b.as_ref(), None) {
                    Ok(_) => {}
                    Err(e) => {
                        return response!(StatusCode::BAD_REQUEST, format!("{}", e));
                    }
                };
            }
            _ => {
                return response!(StatusCode::NOT_IMPLEMENTED, "Method not implemented");
            }
        };
        #[allow(clippy::redundant_closure)]
        batch.process(|x, m| processor(x, m)).await;
        if let Some(mut jexception) = batch.take_jexception() {
            let mut builder = Response::builder().status(
                StatusCode::from_u16(jexception.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            );
            while let Some((h, v)) = jexception.headers.pop() {
                builder = builder.header(&h, v);
            }
            #[allow(clippy::needless_question_mark)]
            return Ok(builder.body(Body::from(jexception.content))?);
        }
        let buf = match batch.pack_responces(None) {
            Ok(v) => v,
            Err(e) => {
                return response!(StatusCode::INTERNAL_SERVER_ERROR, format!("{}", e));
            }
        };
        Ok(if let Some(v) = buf {
            Response::builder()
                .header(hyper::header::CONTENT_TYPE, encoding.to_string())
                .header(hyper::header::EXPIRES, "0")
                .header(hyper::header::CACHE_CONTROL, "no-cache, no-store")
                .header(hyper::header::PRAGMA, "no-cache")
                .body(Body::from(v))?
        } else {
            Response::builder()
                .status(StatusCode::NO_CONTENT)
                .body(Body::from(String::new()))?
        })
    }
}
