use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use crate::barrier::BranchBarrier;
use crate::constants::*;
use crate::error::{DtmError, Result};
use crate::types::TransBase;

pub mod pb {
    tonic::include_proto!("dtmgimp");
}

fn may_replace_localhost(host: &str) -> String {
    if std::env::var("IS_DOCKER").is_ok_and(|v| !v.is_empty()) {
        host.replace("localhost", "host.docker.internal")
            .replace("127.0.0.1", "host.docker.internal")
    } else {
        host.to_string()
    }
}

fn endpoint_uri(server: &str) -> Result<String> {
    let server = may_replace_localhost(server);
    if server.starts_with("http://") || server.starts_with("https://") {
        return Ok(server);
    }
    Ok(format!("http://{server}"))
}

static CHANNELS: OnceLock<Mutex<HashMap<String, tonic::transport::Channel>>> = OnceLock::new();

async fn get_channel(server: &str) -> Result<tonic::transport::Channel> {
    let uri = endpoint_uri(server)?;
    if let Some(ch) = CHANNELS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .ok()
        .and_then(|m| m.get(&uri).cloned())
    {
        return Ok(ch);
    }

    let endpoint = tonic::transport::Endpoint::from_shared(uri.clone())
        .map_err(|e| DtmError::InvalidInput {
            message: e.to_string(),
        })?
        .timeout(Duration::from_secs(30));
    let channel = endpoint.connect().await.map_err(|e| DtmError::Other {
        message: e.to_string(),
    })?;

    if let Ok(mut guard) = CHANNELS.get_or_init(|| Mutex::new(HashMap::new())).lock() {
        guard.insert(uri, channel.clone());
    }
    Ok(channel)
}

pub fn dtm_error_to_grpc_status(err: &DtmError) -> tonic::Status {
    if err.is_failure() {
        tonic::Status::aborted(err.to_string())
    } else if err.is_ongoing() {
        tonic::Status::failed_precondition(err.to_string())
    } else {
        tonic::Status::internal(err.to_string())
    }
}

pub fn grpc_status_to_dtm_error(status: tonic::Status) -> DtmError {
    match status.code() {
        tonic::Code::Aborted => DtmError::failure(status.message().to_string()),
        tonic::Code::FailedPrecondition => DtmError::ongoing(status.message().to_string()),
        _ => DtmError::Other {
            message: status.to_string(),
        },
    }
}

pub async fn gen_gid(server: &str) -> Result<String> {
    let channel = get_channel(server).await?;
    let mut client = pb::dtm_client::DtmClient::new(channel);
    let reply = client
        .new_gid(tonic::Request::new(()))
        .await
        .map_err(grpc_status_to_dtm_error)?
        .into_inner();
    Ok(reply.gid)
}

pub async fn must_gen_gid(server: &str) -> String {
    gen_gid(server).await.expect("must_gen_gid failed")
}

fn dtm_request_from_transbase(tb: &TransBase) -> Result<pb::DtmRequest> {
    let steps = serde_json::to_string(&tb.steps)?;
    Ok(pb::DtmRequest {
        gid: tb.gid.clone(),
        trans_type: tb.trans_type.clone(),
        trans_options: Some(pb::DtmTransOptions {
            wait_result: tb.options.wait_result,
            timeout_to_fail: tb.options.timeout_to_fail,
            retry_interval: tb.options.retry_interval,
            branch_headers: tb.options.branch_headers.clone(),
            request_timeout: tb.options.request_timeout,
            retry_limit: tb.options.retry_limit,
        }),
        customed_data: tb.custom_data.clone().unwrap_or_default(),
        bin_payloads: tb.bin_payloads.clone(),
        query_prepared: tb.query_prepared.clone(),
        steps,
        req_extra: HashMap::new(),
        rollback_reason: tb.rollback_reason.clone(),
    })
}

async fn dtm_grpc_call(tb: &TransBase, operation: &str) -> Result<()> {
    let channel = get_channel(&tb.dtm).await?;
    let mut client = pb::dtm_client::DtmClient::new(channel);
    let req = dtm_request_from_transbase(tb)?;
    let request = tonic::Request::new(req);
    match operation {
        "Submit" => {
            let _ = client
                .submit(request)
                .await
                .map_err(grpc_status_to_dtm_error)?;
        }
        "Prepare" => {
            let _ = client
                .prepare(request)
                .await
                .map_err(grpc_status_to_dtm_error)?;
        }
        "Abort" => {
            let _ = client
                .abort(request)
                .await
                .map_err(grpc_status_to_dtm_error)?;
        }
        _ => {
            return Err(DtmError::InvalidInput {
                message: format!("unknown dtm grpc operation: {operation}"),
            });
        }
    }
    Ok(())
}

fn parse_grpc_url(url: &str) -> Result<(String, String)> {
    // Accept:
    // - "localhost:58081/busi.Busi/TransOut"
    // - "grpc://localhost:58081/busi.Busi/TransOut"
    let url = url.strip_prefix("grpc://").unwrap_or(url);
    let (server, path) = url.split_once('/').ok_or_else(|| DtmError::InvalidInput {
        message: format!("invalid grpc url (missing '/'): {url}"),
    })?;
    Ok((server.to_string(), format!("/{path}")))
}

fn apply_trans_info_metadata<T>(
    req: &mut tonic::Request<T>,
    tb: &TransBase,
    branch_id: &str,
    op: &str,
    phase2_url: Option<&str>,
) {
    let md = req.metadata_mut();
    if let Ok(v) = tb.gid.parse() {
        let _ = md.insert("dtm-gid", v);
    }
    if let Ok(v) = tb.trans_type.parse() {
        let _ = md.insert("dtm-trans_type", v);
    }
    if let Ok(v) = branch_id.parse() {
        let _ = md.insert("dtm-branch_id", v);
    }
    if let Ok(v) = op.parse() {
        let _ = md.insert("dtm-op", v);
    }
    if let Ok(v) = tb.dtm.parse() {
        let _ = md.insert("dtm-dtm", v);
    }

    if let Some(url) = phase2_url
        && let Ok(v) = url.parse()
    {
        let _ = md.insert("dtm-phase2_url", v);
    }

    for (k, v) in &tb.options.branch_headers {
        if let (Ok(k), Ok(v)) = (
            k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
            v.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
        ) {
            let _ = md.insert(k, v);
        }
    }
}

pub async fn invoke_branch<Req, Res>(
    tb: &TransBase,
    url: &str,
    branch_id: &str,
    op: &str,
    msg: Req,
) -> Result<Res>
where
    Req: prost::Message + Default + Send + 'static,
    Res: prost::Message + Default + Send + 'static,
{
    let (server, method) = parse_grpc_url(url)?;
    let channel = get_channel(&server).await?;
    let mut grpc = tonic::client::Grpc::new(channel);
    let mut request = tonic::Request::new(msg);
    let phase2_url = if tb.trans_type == "xa" {
        Some(url)
    } else {
        None
    };
    apply_trans_info_metadata(&mut request, tb, branch_id, op, phase2_url);

    let path = tonic::codegen::http::uri::PathAndQuery::try_from(method).map_err(|e| {
        DtmError::InvalidInput {
            message: e.to_string(),
        }
    })?;
    let codec = tonic_prost::ProstCodec::default();
    let response = grpc
        .unary(request, path, codec)
        .await
        .map_err(grpc_status_to_dtm_error)?;
    Ok(response.into_inner())
}

pub fn barrier_from_grpc_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<BranchBarrier> {
    BranchBarrier::from_grpc_metadata(metadata)
}

#[derive(Clone, Debug)]
pub struct SagaGrpc {
    pub tb: TransBase,
    orders: HashMap<i32, Vec<i32>>,
}

impl SagaGrpc {
    pub fn new(server: impl Into<String>, gid: impl Into<String>) -> Self {
        Self {
            tb: TransBase::new(gid, "saga", server, ""),
            orders: HashMap::new(),
        }
    }

    pub fn with_branch_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.tb.options.branch_headers = headers;
        self
    }

    pub fn add<Msg: prost::Message>(
        &mut self,
        action: impl Into<String>,
        compensate: impl Into<String>,
        payload: Msg,
    ) -> Result<&mut Self> {
        self.tb.steps.push(HashMap::from([
            ("action".to_string(), action.into()),
            ("compensate".to_string(), compensate.into()),
        ]));
        self.tb.bin_payloads.push(payload.encode_to_vec());
        Ok(self)
    }

    pub fn add_branch_order(&mut self, branch: i32, pre_branches: Vec<i32>) -> &mut Self {
        self.orders.insert(branch, pre_branches);
        self
    }

    pub fn enable_concurrent(&mut self) -> &mut Self {
        self.tb.options.concurrent = true;
        self
    }

    fn build_custom_options(&mut self) -> Result<()> {
        if self.tb.options.concurrent {
            self.tb.custom_data = Some(serde_json::to_string(&serde_json::json!({
                "orders": self.orders,
                "concurrent": true,
            }))?);
        }
        Ok(())
    }

    pub async fn submit(&mut self) -> Result<()> {
        self.build_custom_options()?;
        dtm_grpc_call(&self.tb, "Submit").await
    }
}

#[derive(Clone, Debug)]
pub struct MsgGrpc {
    pub tb: TransBase,
    delay: u64,
}

impl MsgGrpc {
    pub fn new(server: impl Into<String>, gid: impl Into<String>) -> Self {
        Self {
            tb: TransBase::new(gid, "msg", server, ""),
            delay: 0,
        }
    }

    pub fn with_branch_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.tb.options.branch_headers = headers;
        self
    }

    pub fn add<Msg: prost::Message>(
        &mut self,
        action: impl Into<String>,
        payload: Msg,
    ) -> Result<&mut Self> {
        self.tb
            .steps
            .push(HashMap::from([("action".to_string(), action.into())]));
        self.tb.bin_payloads.push(payload.encode_to_vec());
        Ok(self)
    }

    pub fn add_topic<Msg: prost::Message>(
        &mut self,
        topic: impl AsRef<str>,
        payload: Msg,
    ) -> Result<&mut Self> {
        let action = format!("{MSG_TOPIC_PREFIX}{}", topic.as_ref());
        self.add(action, payload)
    }

    pub fn set_delay(&mut self, delay_seconds: u64) -> &mut Self {
        self.delay = delay_seconds;
        self
    }

    pub async fn prepare(&mut self, query_prepared: &str) -> Result<()> {
        if !query_prepared.is_empty() {
            self.tb.query_prepared = query_prepared.to_string();
        }
        dtm_grpc_call(&self.tb, "Prepare").await
    }

    fn build_custom_options(&mut self) -> Result<()> {
        if self.delay > 0 {
            self.tb.custom_data = Some(serde_json::to_string(&serde_json::json!({
                "delay": self.delay
            }))?);
        }
        Ok(())
    }

    pub async fn submit(&mut self) -> Result<()> {
        self.build_custom_options()?;
        dtm_grpc_call(&self.tb, "Submit").await
    }

    pub async fn do_and_submit<F, Fut>(
        &mut self,
        query_prepared: &str,
        mut busi_call: F,
    ) -> Result<()>
    where
        F: FnMut(&mut BranchBarrier) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut bb = BranchBarrier::from(
            self.tb.trans_type.clone(),
            self.tb.gid.clone(),
            MSG_DO_BRANCH_0,
            MSG_DO_OP,
        )?;
        self.prepare(query_prepared).await?;

        let errb = busi_call(&mut bb).await;
        let mut err_from_query_prepared: Option<DtmError> = None;
        if let Err(e) = &errb
            && !e.is_failure()
            && !query_prepared.is_empty()
        {
            let branch_id = &bb.branch_id;
            let op = &bb.op;
            let r = invoke_branch::<(), ()>(&self.tb, query_prepared, branch_id, op, ()).await;
            if let Err(err) = r {
                err_from_query_prepared = Some(err);
            }
        }

        let should_abort = errb.as_ref().is_err_and(|e| e.is_failure())
            || err_from_query_prepared
                .as_ref()
                .is_some_and(|e| e.is_failure());
        if should_abort {
            let _ = dtm_grpc_call(&self.tb, "Abort").await;
        } else if err_from_query_prepared.is_none() && errb.is_ok() {
            self.submit().await?;
        }
        errb
    }
}

#[derive(Clone, Debug)]
pub struct TccGrpc {
    pub tb: TransBase,
}

impl TccGrpc {
    pub async fn global_transaction<F, Fut, R>(
        dtm: impl Into<String>,
        gid: impl Into<String>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut TccGrpc) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        Self::global_transaction_with_custom(dtm, gid, |_| {}, f).await
    }

    pub async fn global_transaction_with_custom<FCustom, F, Fut, R>(
        dtm: impl Into<String>,
        gid: impl Into<String>,
        custom: FCustom,
        f: F,
    ) -> Result<R>
    where
        FCustom: FnOnce(&mut TccGrpc),
        F: FnOnce(&mut TccGrpc) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let mut tcc = TccGrpc {
            tb: TransBase::new(gid, "tcc", dtm, ""),
        };
        custom(&mut tcc);
        dtm_grpc_call(&tcc.tb, "Prepare").await?;

        let r = f(&mut tcc).await;
        match r {
            Ok(v) => {
                dtm_grpc_call(&tcc.tb, "Submit").await?;
                Ok(v)
            }
            Err(e) => {
                tcc.tb.rollback_reason = e.to_string();
                let _ = dtm_grpc_call(&tcc.tb, "Abort").await;
                Err(e)
            }
        }
    }

    pub fn from_grpc_metadata(metadata: &tonic::metadata::MetadataMap) -> Result<Self> {
        let get = |k: &str| -> String {
            metadata
                .get(k)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        let gid = get("dtm-gid");
        let dtm = get("dtm-dtm");
        let branch_id = get("dtm-branch_id");
        if dtm.is_empty() || gid.is_empty() {
            return Err(DtmError::InvalidInput {
                message: format!("bad tcc info. dtm: {dtm}, gid: {gid} branchid: {branch_id}"),
            });
        }
        Ok(Self {
            tb: TransBase::new(gid, "tcc", dtm, branch_id),
        })
    }

    pub async fn call_branch<Req, Res>(
        &mut self,
        busi_msg: Req,
        try_url: &str,
        confirm_url: &str,
        cancel_url: &str,
    ) -> Result<Res>
    where
        Req: prost::Message + Default + Send + 'static,
        Res: prost::Message + Default + Send + 'static,
    {
        let branch_id = self.tb.new_sub_branch_id()?;
        let channel = get_channel(&self.tb.dtm).await?;
        let mut dtm = pb::dtm_client::DtmClient::new(channel);
        let busi_payload = busi_msg.encode_to_vec();
        dtm.register_branch(tonic::Request::new(pb::DtmBranchRequest {
            gid: self.tb.gid.clone(),
            trans_type: self.tb.trans_type.clone(),
            branch_id: branch_id.clone(),
            op: String::new(),
            data: HashMap::from([
                (OP_CONFIRM.to_string(), confirm_url.to_string()),
                (OP_CANCEL.to_string(), cancel_url.to_string()),
            ]),
            busi_payload,
        }))
        .await
        .map_err(grpc_status_to_dtm_error)?;

        invoke_branch(&self.tb, try_url, &branch_id, OP_TRY, busi_msg).await
    }
}

#[derive(Clone, Debug)]
pub struct XaGrpc {
    pub tb: TransBase,
    pub phase2_url: String,
}

impl XaGrpc {
    pub async fn global_transaction<F, Fut, R>(
        dtm: impl Into<String>,
        gid: impl Into<String>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut XaGrpc) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        Self::global_transaction_with_custom(dtm, gid, |_| {}, f).await
    }

    pub async fn global_transaction_with_custom<FCustom, F, Fut, R>(
        dtm: impl Into<String>,
        gid: impl Into<String>,
        custom: FCustom,
        f: F,
    ) -> Result<R>
    where
        FCustom: FnOnce(&mut XaGrpc),
        F: FnOnce(&mut XaGrpc) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let mut xa = XaGrpc {
            tb: TransBase::new(gid, "xa", dtm, ""),
            phase2_url: String::new(),
        };
        custom(&mut xa);
        dtm_grpc_call(&xa.tb, "Prepare").await?;

        let r = f(&mut xa).await;
        match r {
            Ok(v) => {
                dtm_grpc_call(&xa.tb, "Submit").await?;
                Ok(v)
            }
            Err(e) => {
                let _ = dtm_grpc_call(&xa.tb, "Abort").await;
                Err(e)
            }
        }
    }

    pub fn from_grpc_metadata(metadata: &tonic::metadata::MetadataMap) -> Result<Self> {
        let get = |k: &str| -> String {
            metadata
                .get(k)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        let gid = get("dtm-gid");
        let trans_type = get("dtm-trans_type");
        let dtm = get("dtm-dtm");
        let branch_id = get("dtm-branch_id");
        let op = get("dtm-op");
        let phase2_url = get("dtm-phase2_url");
        if gid.is_empty() || branch_id.is_empty() || op.is_empty() {
            return Err(DtmError::InvalidInput {
                message: format!(
                    "bad xa info: gid: {gid} branchid: {branch_id} op: {op} phase2_url: {phase2_url}"
                ),
            });
        }
        let mut tb = TransBase::new(gid, trans_type, dtm, branch_id);
        tb.op = op;
        Ok(Self { tb, phase2_url })
    }

    pub async fn call_branch<Req, Res>(&mut self, msg: Req, url: &str) -> Result<Res>
    where
        Req: prost::Message + Default + Send + 'static,
        Res: prost::Message + Default + Send + 'static,
    {
        let branch_id = self.tb.new_sub_branch_id()?;
        invoke_branch(&self.tb, url, &branch_id, OP_ACTION, msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn parse_grpc_url_accepts_plain_host_port() {
        let (server, method) = parse_grpc_url("localhost:1/svc/method").unwrap();
        assert_eq!(server, "localhost:1");
        assert_eq!(method, "/svc/method");
    }

    #[derive(Default)]
    struct TestDtm {
        last_submit: std::sync::Mutex<Option<pb::DtmRequest>>,
    }

    #[tonic::async_trait]
    impl pb::dtm_server::Dtm for TestDtm {
        async fn new_gid(
            &self,
            _request: tonic::Request<()>,
        ) -> std::result::Result<tonic::Response<pb::DtmGidReply>, tonic::Status> {
            Ok(tonic::Response::new(pb::DtmGidReply {
                gid: "gid-123".to_string(),
            }))
        }

        async fn submit(
            &self,
            request: tonic::Request<pb::DtmRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            *self.last_submit.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(request.into_inner());
            Ok(tonic::Response::new(()))
        }

        async fn prepare(
            &self,
            _request: tonic::Request<pb::DtmRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            Ok(tonic::Response::new(()))
        }

        async fn abort(
            &self,
            _request: tonic::Request<pb::DtmRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            Ok(tonic::Response::new(()))
        }

        async fn register_branch(
            &self,
            _request: tonic::Request<pb::DtmBranchRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            Ok(tonic::Response::new(()))
        }

        async fn prepare_workflow(
            &self,
            _request: tonic::Request<pb::DtmRequest>,
        ) -> std::result::Result<tonic::Response<pb::DtmProgressesReply>, tonic::Status> {
            Ok(tonic::Response::new(pb::DtmProgressesReply {
                transaction: None,
                progresses: Vec::new(),
            }))
        }

        async fn subscribe(
            &self,
            _request: tonic::Request<pb::DtmTopicRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            Ok(tonic::Response::new(()))
        }

        async fn unsubscribe(
            &self,
            _request: tonic::Request<pb::DtmTopicRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            Ok(tonic::Response::new(()))
        }

        async fn delete_topic(
            &self,
            _request: tonic::Request<pb::DtmTopicRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            Ok(tonic::Response::new(()))
        }
    }

    async fn start_test_dtm_server(svc: Arc<TestDtm>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind listener");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(pb::dtm_server::DtmServer::from_arc(svc))
                .serve_with_incoming(incoming)
                .await
                .expect("serve dtm grpc");
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        format!("127.0.0.1:{}", addr.port())
    }

    #[tokio::test]
    async fn gen_gid_calls_new_gid() -> Result<()> {
        let addr = start_test_dtm_server(Arc::new(TestDtm::default())).await;
        let gid = gen_gid(&addr).await?;
        assert_eq!(gid, "gid-123");
        Ok(())
    }

    #[tokio::test]
    async fn saga_submit_sends_dtm_request() -> Result<()> {
        #[derive(Clone, PartialEq, ::prost::Message)]
        struct TestPayload {
            #[prost(string, tag = "1")]
            v: String,
        }

        let svc = Arc::new(TestDtm::default());
        let addr = start_test_dtm_server(svc.clone()).await;

        let mut saga = SagaGrpc::new(&addr, "gid-xyz");
        saga.add(
            "busi.Busi/TransOut",
            "busi.Busi/TransOutRevert",
            TestPayload { v: "hello".into() },
        )?;
        saga.submit().await?;

        let req = svc
            .last_submit
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("missing submit request");
        assert_eq!(req.gid, "gid-xyz");
        assert_eq!(req.trans_type, "saga");
        assert_eq!(req.bin_payloads.len(), 1);

        let steps: serde_json::Value = serde_json::from_str(&req.steps).expect("parse steps json");
        let steps = steps.as_array().expect("steps array");
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0]["action"], "busi.Busi/TransOut");
        assert_eq!(steps[0]["compensate"], "busi.Busi/TransOutRevert");

        Ok(())
    }
}
