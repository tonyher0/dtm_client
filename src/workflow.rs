use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};

use base64::Engine as _;

use crate::barrier::BranchBarrier;
use crate::constants::*;
use crate::dtmgrpc;
use crate::error::{DtmError, Result};
use crate::types::{BranchIdGen, TransBase};

pub mod pb {
    tonic::include_proto!("workflow");
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

type WorkflowHandler =
    dyn Fn(Workflow, Vec<u8>) -> BoxFuture<Result<Vec<u8>>> + Send + Sync + 'static;
type Phase2Fn = dyn Fn(Workflow, BranchBarrier) -> BoxFuture<Result<()>> + Send + Sync + 'static;

#[derive(Clone, Debug, Default)]
pub struct Options {
    pub compensate_error_branch: bool,
}

#[derive(Clone)]
pub struct Workflow {
    inner: std::sync::Arc<Mutex<WorkflowInner>>,
}

impl std::fmt::Debug for Workflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Workflow").finish_non_exhaustive()
    }
}

struct WorkflowInner {
    options: Options,
    tb: TransBase,
    protocol: String,

    http_client: reqwest::Client,

    id_gen: BranchIdGen,

    current_action_added: bool,
    current_commit_added: bool,
    current_rollback_added: bool,

    progresses: HashMap<String, StepResult>,
    succeeded_ops: Vec<Phase2Item>,
    failed_ops: Vec<Phase2Item>,
}

#[derive(Clone)]
struct Phase2Item {
    branch_id: String,
    fn_: std::sync::Arc<Phase2Fn>,
}

#[derive(Clone, Debug)]
struct StepResult {
    status: String, // succeed | failed | ""
    data: Vec<u8>,  // succeed => result bytes; failed => error message bytes
}

fn wf_error_to_status(err: Option<&DtmError>) -> String {
    match err {
        None => STATUS_SUCCEED.to_string(),
        Some(e) if e.is_failure() => STATUS_FAILED.to_string(),
        _ => String::new(),
    }
}

fn decode_base64(s: &str) -> Result<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| DtmError::Other {
            message: e.to_string(),
        })
}

fn encode_base64(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

fn grpc_endpoint_uri(server: &str) -> String {
    let server = if std::env::var("IS_DOCKER").is_ok_and(|v| !v.is_empty()) {
        server
            .replace("localhost", "host.docker.internal")
            .replace("127.0.0.1", "host.docker.internal")
    } else {
        server.to_string()
    };
    if server.starts_with("http://") || server.starts_with("https://") {
        server
    } else {
        format!("http://{server}")
    }
}

#[derive(Debug, serde::Deserialize)]
struct ProgressesReplyHttp {
    transaction: TransactionHttp,
    progresses: Vec<ProgressHttp>,
}

#[derive(Debug, serde::Deserialize)]
struct TransactionHttp {
    status: String,
    rollback_reason: String,
    result: String,
}

#[derive(Debug, serde::Deserialize)]
struct ProgressHttp {
    status: String,
    bin_data: String,
    branch_id: String,
    op: String,
}

#[derive(Default)]
struct WorkflowFactory {
    protocol: String,
    http_dtm: String,
    http_callback: String,
    grpc_dtm: String,
    grpc_callback: String,
    handlers: HashMap<String, RegisteredWorkflow>,
}

#[derive(Clone)]
struct RegisteredWorkflow {
    handler: std::sync::Arc<WorkflowHandler>,
}

static FACTORY: OnceLock<Mutex<WorkflowFactory>> = OnceLock::new();

fn factory() -> &'static Mutex<WorkflowFactory> {
    FACTORY.get_or_init(|| Mutex::new(WorkflowFactory::default()))
}

pub fn init_http(http_dtm: impl Into<String>, callback: impl Into<String>) {
    let mut f = factory().lock().unwrap_or_else(|e| e.into_inner());
    f.protocol = PROTOCOL_HTTP.to_string();
    f.http_dtm = http_dtm.into();
    f.http_callback = callback.into();
}

pub fn init_grpc(grpc_dtm: impl Into<String>, client_host: impl Into<String>) {
    let mut f = factory().lock().unwrap_or_else(|e| e.into_inner());
    f.protocol = PROTOCOL_GRPC.to_string();
    f.grpc_dtm = grpc_dtm.into();
    f.grpc_callback = format!("{}/workflow.Workflow/Execute", client_host.into());
}

pub fn register<F, Fut>(name: &str, handler: F) -> Result<()>
where
    F: Fn(Workflow, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    register2(name, move |wf, data| {
        let fut = handler(wf, data);
        async move {
            fut.await?;
            Ok(Vec::new())
        }
    })
}

pub fn register2<F, Fut>(name: &str, handler: F) -> Result<()>
where
    F: Fn(Workflow, Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
{
    let mut f = factory().lock().unwrap_or_else(|e| e.into_inner());
    if f.handlers.contains_key(name) {
        return Err(DtmError::InvalidInput {
            message: format!("a handler already exists for {name}"),
        });
    }
    let boxed = std::sync::Arc::new(
        move |wf: Workflow, data: Vec<u8>| -> BoxFuture<Result<Vec<u8>>> {
            Box::pin(handler(wf, data))
        },
    ) as std::sync::Arc<WorkflowHandler>;
    f.handlers
        .insert(name.to_string(), RegisteredWorkflow { handler: boxed });
    Ok(())
}

pub async fn execute(name: &str, gid: &str, data: Vec<u8>) -> Result<Vec<u8>> {
    let reg = {
        let f = factory().lock().unwrap_or_else(|e| e.into_inner());
        f.handlers
            .get(name)
            .cloned()
            .ok_or_else(|| DtmError::InvalidInput {
                message: format!("workflow '{name}' not registered"),
            })?
    };
    let wf = new_workflow(name, gid, data.clone())?;
    wf.process(reg.handler, data).await
}

pub async fn execute_by_query_string(query: &str, body: Vec<u8>) -> Result<Vec<u8>> {
    let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let name = params.get("op").cloned().unwrap_or_default();
    let gid = params.get("gid").cloned().unwrap_or_default();
    execute(&name, &gid, body).await
}

fn new_workflow(name: &str, gid: &str, data: Vec<u8>) -> Result<Workflow> {
    let (protocol, dtm, callback) = {
        let f = factory().lock().unwrap_or_else(|e| e.into_inner());
        if f.protocol == PROTOCOL_GRPC {
            (
                f.protocol.clone(),
                f.grpc_dtm.clone(),
                f.grpc_callback.clone(),
            )
        } else {
            (
                f.protocol.clone(),
                f.http_dtm.clone(),
                f.http_callback.clone(),
            )
        }
    };

    if protocol.is_empty() || dtm.is_empty() || callback.is_empty() {
        return Err(DtmError::InvalidInput {
            message:
                "workflow not initialized: call workflow::init_http or workflow::init_grpc first"
                    .to_string(),
        });
    }

    let mut tb = TransBase::new(gid.to_string(), "workflow", dtm, "");
    tb.protocol = protocol.clone();
    tb.query_prepared = callback;
    tb.custom_data = Some(serde_json::to_string(&serde_json::json!({
        "name": name,
        "data": data,
    }))?);

    Ok(Workflow {
        inner: std::sync::Arc::new(Mutex::new(WorkflowInner {
            options: Options::default(),
            tb,
            protocol,
            http_client: reqwest::Client::new(),
            id_gen: BranchIdGen::new(""),
            current_action_added: false,
            current_commit_added: false,
            current_rollback_added: false,
            progresses: HashMap::new(),
            succeeded_ops: Vec::new(),
            failed_ops: Vec::new(),
        })),
    })
}

impl Workflow {
    pub fn options(&self) -> Options {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .options
            .clone()
    }

    pub fn set_compensate_error_branch(&self, v: bool) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .options
            .compensate_error_branch = v;
    }

    pub fn new_branch(&self) -> Result<WorkflowBranch> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.id_gen.new_sub_branch_id()?;
        let branch = inner.id_gen.current_sub_branch_id();
        inner.current_action_added = false;
        inner.current_commit_added = false;
        inner.current_rollback_added = false;
        Ok(WorkflowBranch {
            wf: self.clone(),
            branch_id: branch,
        })
    }

    async fn get_progress(&self) -> Result<Progresses> {
        let (tb, protocol, http_client) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (
                inner.tb.clone(),
                inner.protocol.clone(),
                inner.http_client.clone(),
            )
        };

        if protocol == PROTOCOL_GRPC {
            let uri = grpc_endpoint_uri(&tb.dtm);
            let channel = tonic::transport::Endpoint::from_shared(uri)
                .map_err(|e| DtmError::InvalidInput {
                    message: e.to_string(),
                })?
                .connect()
                .await
                .map_err(|e| DtmError::Other {
                    message: e.to_string(),
                })?;
            let mut client = dtmgrpc::pb::dtm_client::DtmClient::new(channel);
            let req = dtmgrpc_dtm_request_from_transbase(&tb)?;
            let reply = client
                .prepare_workflow(tonic::Request::new(req))
                .await
                .map_err(dtmgrpc::grpc_status_to_dtm_error)?
                .into_inner();
            return Ok(Progresses::Grpc(reply));
        }

        let url = format!("{}/prepareWorkflow", tb.dtm.trim_end_matches('/'));
        let resp = http_client.post(url).json(&tb).send().await?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(DtmError::HttpStatus { status, body: text });
        }
        let parsed: ProgressesReplyHttp = serde_json::from_str(&text)?;
        Ok(Progresses::Http(parsed))
    }

    fn init_progress(&self, progresses: Vec<ProgressRecord>) -> Result<()> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.progresses.clear();
        for p in progresses {
            let sr = StepResult {
                status: p.status.clone(),
                data: p.data.clone(),
            };
            inner
                .progresses
                .insert(format!("{}-{}", p.branch_id, p.op), sr);
        }
        Ok(())
    }

    fn get_step_result(&self, branch_id: &str, op: &str) -> Option<StepResult> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.progresses.get(&format!("{branch_id}-{op}")).cloned()
    }

    async fn save_result(&self, branch_id: &str, op: &str, sr: &StepResult) -> Result<()> {
        if sr.status.is_empty() {
            return Ok(());
        }
        self.register_branch(&sr.data, branch_id, op, &sr.status)
            .await?;
        Ok(())
    }

    async fn process(
        &self,
        handler: std::sync::Arc<WorkflowHandler>,
        data: Vec<u8>,
    ) -> Result<Vec<u8>> {
        let progresses = self.get_progress().await?;
        match progresses.status()? {
            Some(STATUS_SUCCEED) => return progresses.result_bytes(),
            Some(STATUS_FAILED) => {
                return Err(DtmError::failure(
                    progresses.rollback_reason().unwrap_or_default(),
                ));
            }
            _ => {}
        }

        self.init_progress(progresses.progress_records()?)?;

        let r = (handler)(self.clone(), data).await;
        let (result_bytes, mut err) = match r {
            Ok(v) => (v, None),
            Err(e) => (Vec::new(), Some(e)),
        };

        if err.as_ref().is_some_and(|e| !e.is_failure()) {
            return Err(err.take().unwrap());
        }

        err = self.process_phase2(err).await?;

        if err.is_none() || err.as_ref().is_some_and(|e| e.is_failure()) {
            self.submit(&result_bytes, err.as_ref()).await?;
        }

        match err {
            None => Ok(result_bytes),
            Some(e) => Err(e),
        }
    }

    async fn process_phase2(&self, err: Option<DtmError>) -> Result<Option<DtmError>> {
        let (op, mut ops) = {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if err.is_none() {
                (
                    OP_COMMIT.to_string(),
                    std::mem::take(&mut inner.succeeded_ops),
                )
            } else {
                (
                    OP_ROLLBACK.to_string(),
                    std::mem::take(&mut inner.failed_ops),
                )
            }
        };
        ops.reverse();
        for item in ops {
            let e = self
                .call_phase2(&item.branch_id, &op, item.fn_.clone())
                .await;
            if let Err(e) = e {
                return Ok(Some(e));
            }
        }
        Ok(err)
    }

    async fn call_phase2(
        &self,
        branch_id: &str,
        op: &str,
        f: std::sync::Arc<Phase2Fn>,
    ) -> Result<()> {
        let bb = BranchBarrier::from(
            "workflow",
            self.gid(),
            branch_id.to_string(),
            op.to_string(),
        )?;
        let wf = self.clone();
        let f = f.clone();
        let _ = self
            .recorded_step(branch_id, op, move || async move {
                (f)(wf, bb).await?;
                Ok(Vec::new())
            })
            .await?;
        Ok(())
    }

    fn gid(&self) -> String {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .tb
            .gid
            .clone()
    }

    async fn recorded_step<F, Fut>(&self, branch_id: &str, op: &str, f: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<Vec<u8>>> + Send + 'static,
    {
        let is_action = op == OP_ACTION;
        if is_action {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if inner.current_action_added {
                return Err(DtmError::InvalidInput {
                    message: "one branch can have only one action".to_string(),
                });
            }
            inner.current_action_added = true;
        }

        if let Some(sr) = self.get_step_result(branch_id, op) {
            if sr.status == STATUS_SUCCEED {
                return Ok(sr.data);
            }
            if sr.status == STATUS_FAILED {
                return Err(DtmError::failure(
                    String::from_utf8_lossy(&sr.data).to_string(),
                ));
            }
        }

        let r = f().await;
        match r {
            Ok(data) => {
                let sr = StepResult {
                    status: STATUS_SUCCEED.to_string(),
                    data,
                };
                self.save_result(branch_id, op, &sr).await?;
                Ok(sr.data)
            }
            Err(e) if e.is_failure() => {
                if is_action {
                    let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                    if !inner.options.compensate_error_branch
                        && inner
                            .failed_ops
                            .last()
                            .is_some_and(|it| it.branch_id == branch_id)
                    {
                        inner.failed_ops.pop();
                    }
                }
                let sr = StepResult {
                    status: STATUS_FAILED.to_string(),
                    data: e.to_string().into_bytes(),
                };
                self.save_result(branch_id, op, &sr).await?;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn http_post_json<T: serde::Serialize>(
        &self,
        branch_id: &str,
        op: &str,
        url: &str,
        body: &T,
    ) -> Result<Vec<u8>> {
        let (gid, trans_type, client) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (
                inner.tb.gid.clone(),
                inner.tb.trans_type.clone(),
                inner.http_client.clone(),
            )
        };
        let body_value = serde_json::to_value(body)?;
        let url = reqwest::Url::parse(url)?;
        let mut url = url;
        url.query_pairs_mut()
            .append_pair("gid", &gid)
            .append_pair("trans_type", &trans_type)
            .append_pair("branch_id", branch_id)
            .append_pair("op", op);

        self.recorded_step(branch_id, op, move || async move {
            let resp = client.post(url).json(&body_value).send().await?;
            let status = resp.status().as_u16();
            let bytes = resp.bytes().await.unwrap_or_default().to_vec();
            let text = String::from_utf8_lossy(&bytes).to_string();
            crate::dtmcli::http_resp_to_dtm_error(status, &text)?;
            Ok(bytes)
        })
        .await
    }

    pub async fn grpc_invoke<Req, Res>(
        &self,
        branch_id: &str,
        op: &str,
        url: &str,
        msg: Req,
    ) -> Result<Res>
    where
        Req: prost::Message + Default + Send + 'static,
        Res: prost::Message + Default + Send + 'static,
    {
        if let Some(sr) = self.get_step_result(branch_id, op) {
            if sr.status == STATUS_SUCCEED {
                return Ok(<Res as prost::Message>::decode(sr.data.as_slice())?);
            }
            if sr.status == STATUS_FAILED {
                return Err(DtmError::failure(
                    String::from_utf8_lossy(&sr.data).to_string(),
                ));
            }
        }

        let (tb, branch_id, op, url) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (
                inner.tb.clone(),
                branch_id.to_string(),
                op.to_string(),
                url.to_string(),
            )
        };
        let response = dtmgrpc::invoke_branch::<Req, Res>(&tb, &url, &branch_id, &op, msg).await;
        match response {
            Ok(reply) => {
                let data = reply.encode_to_vec();
                let sr = StepResult {
                    status: STATUS_SUCCEED.to_string(),
                    data: data.clone(),
                };
                self.save_result(&branch_id, &op, &sr).await?;
                Ok(reply)
            }
            Err(e) if e.is_failure() => {
                let sr = StepResult {
                    status: STATUS_FAILED.to_string(),
                    data: e.to_string().into_bytes(),
                };
                self.save_result(&branch_id, &op, &sr).await?;
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn register_branch(
        &self,
        data: &[u8],
        branch_id: &str,
        op: &str,
        status: &str,
    ) -> Result<()> {
        let (tb, protocol) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (inner.tb.clone(), inner.protocol.clone())
        };

        if protocol == PROTOCOL_GRPC {
            let channel = tonic::transport::Endpoint::from_shared(grpc_endpoint_uri(&tb.dtm))
                .map_err(|e| DtmError::InvalidInput {
                    message: e.to_string(),
                })?
                .connect()
                .await
                .map_err(|e| DtmError::Other {
                    message: e.to_string(),
                })?;
            let mut client = dtmgrpc::pb::dtm_client::DtmClient::new(channel);
            client
                .register_branch(tonic::Request::new(dtmgrpc::pb::DtmBranchRequest {
                    gid: tb.gid,
                    trans_type: tb.trans_type,
                    branch_id: branch_id.to_string(),
                    op: String::new(),
                    data: HashMap::from([
                        ("status".to_string(), status.to_string()),
                        ("op".to_string(), op.to_string()),
                    ]),
                    busi_payload: data.to_vec(),
                }))
                .await
                .map_err(dtmgrpc::grpc_status_to_dtm_error)?;
            return Ok(());
        }

        let url = format!("{}/registerBranch", tb.dtm.trim_end_matches('/'));
        let payload = HashMap::from([
            ("gid", tb.gid),
            ("trans_type", tb.trans_type),
            ("data", String::from_utf8_lossy(data).to_string()),
            ("branch_id", branch_id.to_string()),
            ("op", op.to_string()),
            ("status", status.to_string()),
        ]);
        let resp = reqwest::Client::new()
            .post(url)
            .json(&payload)
            .send()
            .await?;
        let status_code = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status_code != 200 {
            return Err(DtmError::HttpStatus {
                status: status_code,
                body: text,
            });
        }
        Ok(())
    }

    async fn submit(&self, result: &[u8], err: Option<&DtmError>) -> Result<()> {
        let (tb, protocol) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (inner.tb.clone(), inner.protocol.clone())
        };

        let status = wf_error_to_status(err);
        let rollback_reason = err.map(|e| e.to_string()).unwrap_or_default();
        let extra = HashMap::from([
            ("status".to_string(), status),
            ("rollback_reason".to_string(), rollback_reason),
            ("result".to_string(), encode_base64(result)),
        ]);

        if protocol == PROTOCOL_GRPC {
            let channel = tonic::transport::Endpoint::from_shared(grpc_endpoint_uri(&tb.dtm))
                .map_err(|e| DtmError::InvalidInput {
                    message: e.to_string(),
                })?
                .connect()
                .await
                .map_err(|e| DtmError::Other {
                    message: e.to_string(),
                })?;
            let mut client = dtmgrpc::pb::dtm_client::DtmClient::new(channel);
            let mut req = dtmgrpc_dtm_request_from_transbase(&tb)?;
            req.req_extra = extra;
            client
                .submit(tonic::Request::new(req))
                .await
                .map_err(dtmgrpc::grpc_status_to_dtm_error)?;
            return Ok(());
        }

        let url = format!("{}/submit", tb.dtm.trim_end_matches('/'));
        let payload = serde_json::json!({
            "gid": tb.gid,
            "trans_type": tb.trans_type,
            "req_extra": extra,
        });
        let resp = reqwest::Client::new()
            .post(url)
            .json(&payload)
            .send()
            .await?;
        let status_code = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status_code != 200 {
            return Err(DtmError::HttpStatus {
                status: status_code,
                body: text,
            });
        }
        Ok(())
    }
}

impl WorkflowInner {
    fn add_phase2(
        &mut self,
        branch_id: &str,
        is_commit: bool,
        f: std::sync::Arc<Phase2Fn>,
    ) -> Result<()> {
        if is_commit {
            if self.current_commit_added {
                return Err(DtmError::InvalidInput {
                    message: "one branch can only add one commit callback".to_string(),
                });
            }
            self.current_commit_added = true;
            self.succeeded_ops.push(Phase2Item {
                branch_id: branch_id.to_string(),
                fn_: f,
            });
        } else {
            if self.current_rollback_added {
                return Err(DtmError::InvalidInput {
                    message: "one branch can only add one rollback callback".to_string(),
                });
            }
            self.current_rollback_added = true;
            self.failed_ops.push(Phase2Item {
                branch_id: branch_id.to_string(),
                fn_: f,
            });
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct WorkflowBranch {
    wf: Workflow,
    branch_id: String,
}

impl WorkflowBranch {
    pub fn branch_id(&self) -> &str {
        &self.branch_id
    }

    pub fn on_commit<F, Fut>(&self, f: F) -> Result<&Self>
    where
        F: Fn(Workflow, BranchBarrier) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let mut inner = self.wf.inner.lock().unwrap_or_else(|e| e.into_inner());
        let branch_id = self.branch_id.clone();
        let cb = std::sync::Arc::new(
            move |wf: Workflow, bb: BranchBarrier| -> BoxFuture<Result<()>> { Box::pin(f(wf, bb)) },
        ) as std::sync::Arc<Phase2Fn>;
        inner.add_phase2(&branch_id, true, cb)?;
        Ok(self)
    }

    pub fn on_rollback<F, Fut>(&self, f: F) -> Result<&Self>
    where
        F: Fn(Workflow, BranchBarrier) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let mut inner = self.wf.inner.lock().unwrap_or_else(|e| e.into_inner());
        let branch_id = self.branch_id.clone();
        let cb = std::sync::Arc::new(
            move |wf: Workflow, bb: BranchBarrier| -> BoxFuture<Result<()>> { Box::pin(f(wf, bb)) },
        ) as std::sync::Arc<Phase2Fn>;
        inner.add_phase2(&branch_id, false, cb)?;
        Ok(self)
    }

    pub async fn http_post_json<T: serde::Serialize>(
        &self,
        url: &str,
        body: &T,
    ) -> Result<Vec<u8>> {
        self.wf
            .http_post_json(&self.branch_id, OP_ACTION, url, body)
            .await
    }

    pub async fn grpc_invoke<Req, Res>(&self, url: &str, msg: Req) -> Result<Res>
    where
        Req: prost::Message + Default + Send + 'static,
        Res: prost::Message + Default + Send + 'static,
    {
        self.wf
            .grpc_invoke(&self.branch_id, OP_ACTION, url, msg)
            .await
    }
}

enum Progresses {
    Http(ProgressesReplyHttp),
    Grpc(dtmgrpc::pb::DtmProgressesReply),
}

struct ProgressRecord {
    status: String,
    data: Vec<u8>,
    branch_id: String,
    op: String,
}

impl Progresses {
    fn status(&self) -> Result<Option<&str>> {
        Ok(match self {
            Progresses::Http(r) => Some(r.transaction.status.as_str()),
            Progresses::Grpc(r) => r.transaction.as_ref().map(|t| t.status.as_str()),
        })
    }

    fn rollback_reason(&self) -> Option<String> {
        match self {
            Progresses::Http(r) => Some(r.transaction.rollback_reason.clone()),
            Progresses::Grpc(r) => r.transaction.as_ref().map(|t| t.rollback_reason.clone()),
        }
    }

    fn result_bytes(&self) -> Result<Vec<u8>> {
        match self {
            Progresses::Http(r) => decode_base64(&r.transaction.result),
            Progresses::Grpc(r) => {
                let result = r
                    .transaction
                    .as_ref()
                    .map(|t| t.result.clone())
                    .unwrap_or_default();
                decode_base64(&result)
            }
        }
    }

    fn progress_records(&self) -> Result<Vec<ProgressRecord>> {
        match self {
            Progresses::Http(r) => r
                .progresses
                .iter()
                .map(|p| {
                    let data = decode_base64(&p.bin_data)?;
                    Ok(ProgressRecord {
                        status: p.status.clone(),
                        data,
                        branch_id: p.branch_id.clone(),
                        op: p.op.clone(),
                    })
                })
                .collect(),
            Progresses::Grpc(r) => Ok(r
                .progresses
                .iter()
                .map(|p| ProgressRecord {
                    status: p.status.clone(),
                    data: p.bin_data.clone(),
                    branch_id: p.branch_id.clone(),
                    op: p.op.clone(),
                })
                .collect()),
        }
    }
}

fn dtmgrpc_dtm_request_from_transbase(tb: &TransBase) -> Result<dtmgrpc::pb::DtmRequest> {
    let steps = serde_json::to_string(&tb.steps)?;
    Ok(dtmgrpc::pb::DtmRequest {
        gid: tb.gid.clone(),
        trans_type: tb.trans_type.clone(),
        trans_options: Some(dtmgrpc::pb::DtmTransOptions {
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

pub fn grpc_service() -> pb::workflow_server::WorkflowServer<WorkflowGrpcService> {
    pb::workflow_server::WorkflowServer::new(WorkflowGrpcService {})
}

pub struct WorkflowGrpcService {}

#[tonic::async_trait]
impl pb::workflow_server::Workflow for WorkflowGrpcService {
    async fn execute(
        &self,
        request: tonic::Request<pb::WorkflowData>,
    ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
        let md = request.metadata();
        let name = md
            .get("dtm-op")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let gid = md
            .get("dtm-gid")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let data = request.into_inner().data;

        match execute(&name, &gid, data).await {
            Ok(_) => Ok(tonic::Response::new(())),
            Err(e) => Err(dtmgrpc::dtm_error_to_grpc_status(&e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static WF_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static WF_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    fn next_workflow_name(prefix: &str) -> String {
        let n = WF_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("{prefix}-{n}")
    }

    #[tokio::test]
    async fn execute_skips_recorded_action_steps() -> Result<()> {
        let _guard = WF_TEST_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let dtm = httpmock::MockServer::start_async().await;
        let busi = httpmock::MockServer::start_async().await;

        let m_prepare = dtm
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/prepareWorkflow");
                then.status(200).json_body(serde_json::json!({
                    "transaction": {
                        "status": "",
                        "rollback_reason": "",
                        "result": ""
                    },
                    "progresses": [{
                        "status": STATUS_SUCCEED,
                        "bin_data": "",
                        "branch_id": "01",
                        "op": OP_ACTION
                    }]
                }));
            })
            .await;

        let m_register = dtm
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/registerBranch");
                then.status(200).body("OK");
            })
            .await;

        let m_submit = dtm
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/submit");
                then.status(200).body("OK");
            })
            .await;

        let m_trans_out = busi
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/TransOut");
                then.status(200).body("OK");
            })
            .await;
        let m_trans_in = busi
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/TransIn");
                then.status(200).body("OK");
            })
            .await;

        init_http(dtm.url(""), "http://callback.example/workflowResume");

        let wf_name = next_workflow_name("wf-http");
        let trans_out_url = busi.url("/TransOut");
        let trans_in_url = busi.url("/TransIn");
        register2(&wf_name, move |wf, data| {
            let trans_out_url = trans_out_url.clone();
            let trans_in_url = trans_in_url.clone();
            async move {
                let req: serde_json::Value = serde_json::from_slice(&data)?;
                let branch = wf.new_branch()?;
                let _ = branch.http_post_json(&trans_out_url, &req).await?;
                let branch = wf.new_branch()?;
                let _ = branch.http_post_json(&trans_in_url, &req).await?;
                Ok(Vec::new())
            }
        })?;

        let _ = execute(
            &wf_name,
            "gid-1",
            serde_json::to_vec(&serde_json::json!({ "amount": 30 }))?,
        )
        .await?;

        m_prepare.assert_async().await;
        m_submit.assert_async().await;
        assert_eq!(m_trans_out.calls_async().await, 0);
        assert_eq!(m_trans_in.calls_async().await, 1);
        assert_eq!(m_register.calls_async().await, 1);

        Ok(())
    }
}
