use std::collections::HashMap;
use std::time::Duration;

use crate::barrier::BranchBarrier;
use crate::constants::*;
use crate::error::{DtmError, Result};
use crate::types::{JsonRpcRequest, JsonRpcResponse, TransBase};

fn may_replace_localhost(host: &str) -> String {
    if std::env::var("IS_DOCKER").is_ok_and(|v| !v.is_empty()) {
        host.replace("localhost", "host.docker.internal")
            .replace("127.0.0.1", "host.docker.internal")
    } else {
        host.to_string()
    }
}

fn trim_suffix_once<'a>(s: &'a str, suffix: &str) -> &'a str {
    s.strip_suffix(suffix).unwrap_or(s)
}

pub fn map_success() -> serde_json::Value {
    serde_json::json!({ "dtm_result": RESULT_SUCCESS })
}

pub fn map_failure() -> serde_json::Value {
    serde_json::json!({ "dtm_result": RESULT_FAILURE })
}

pub fn http_resp_to_dtm_error(status: u16, body: &str) -> Result<()> {
    if status == 425 || body.contains(RESULT_ONGOING) {
        return Err(DtmError::ongoing(body.to_string()));
    }
    if status == 409 || body.contains(RESULT_FAILURE) {
        return Err(DtmError::failure(body.to_string()));
    }
    if status != 200 {
        return Err(DtmError::HttpStatus {
            status,
            body: body.to_string(),
        });
    }
    Ok(())
}

pub fn result_to_http_json<T: serde::Serialize>(result: Result<T>) -> (u16, serde_json::Value) {
    match result {
        Ok(v) => (
            200,
            serde_json::to_value(v).unwrap_or_else(|_| serde_json::json!({})),
        ),
        Err(e) => {
            let status = if e.is_failure() {
                409
            } else if e.is_ongoing() {
                425
            } else {
                500
            };
            (status, serde_json::json!({ "error": e.to_string() }))
        }
    }
}

async fn trans_call_dtm_ext<T: serde::Serialize>(
    client: &reqwest::Client,
    tb: &TransBase,
    body: &T,
    operation: &str,
) -> Result<String> {
    let dtm = may_replace_localhost(&tb.dtm);
    let req_timeout = if tb.options.request_timeout > 0 {
        Some(Duration::from_secs(tb.options.request_timeout as u64))
    } else {
        None
    };

    if tb.protocol == JSON_RPC {
        let rpc = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: "no-use".to_string(),
            method: operation.to_string(),
            params: body,
        };

        let mut req = client.post(dtm).json(&rpc);
        if let Some(t) = req_timeout {
            req = req.timeout(t);
        }
        let resp = req.send().await?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        if status != 200 {
            return Err(DtmError::HttpStatus { status, body: text });
        }

        let parsed: JsonRpcResponse = serde_json::from_str(&text).unwrap_or_default();
        if let Some(err) = parsed.error {
            if err.code == JSON_RPC_CODE_FAILURE {
                return Err(DtmError::failure(text));
            }
            if err.code == JSON_RPC_CODE_ONGOING {
                return Err(DtmError::ongoing(text));
            }
            return Err(DtmError::Other { message: text });
        }
        return Ok(text);
    }

    let url = format!(
        "{}/{}",
        trim_suffix_once(&dtm, "/"),
        trim_suffix_once(operation, "/")
    );
    let mut req = client.post(url).json(body);
    if let Some(t) = req_timeout {
        req = req.timeout(t);
    }

    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    if status != 200 || text.contains(RESULT_FAILURE) {
        return Err(DtmError::Other { message: text });
    }
    Ok(text)
}

async fn trans_call_dtm(client: &reqwest::Client, tb: &TransBase, operation: &str) -> Result<()> {
    let _ = trans_call_dtm_ext(client, tb, tb, operation).await?;
    Ok(())
}

async fn trans_register_branch(
    client: &reqwest::Client,
    tb: &TransBase,
    added: HashMap<&str, String>,
    operation: &str,
) -> Result<()> {
    let mut m = HashMap::<&str, String>::new();
    m.insert("gid", tb.gid.clone());
    m.insert("trans_type", tb.trans_type.clone());
    for (k, v) in added {
        m.insert(k, v);
    }
    let _ = trans_call_dtm_ext(client, tb, &m, operation).await?;
    Ok(())
}

async fn trans_request_branch<T: serde::Serialize>(
    client: &reqwest::Client,
    tb: &TransBase,
    method: reqwest::Method,
    body: Option<&T>,
    branch_id: &str,
    op: &str,
    url: &str,
) -> Result<(u16, String)> {
    if url.is_empty() {
        return Ok((200, String::new()));
    }

    let mut query: HashMap<&str, String> = HashMap::from([
        ("dtm", tb.dtm.clone()),
        ("gid", tb.gid.clone()),
        ("branch_id", branch_id.to_string()),
        ("trans_type", tb.trans_type.clone()),
        ("op", op.to_string()),
    ]);
    if tb.trans_type == "xa" {
        query.insert("phase2_url", url.to_string());
    }

    let mut req = client
        .request(method, may_replace_localhost(url))
        .query(&query);
    if let Some(body) = body {
        req = req.json(body);
    }
    for (k, v) in &tb.options.branch_headers {
        req = req.header(k, v);
    }

    let resp = req.send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    Ok((status, text))
}

pub async fn gen_gid(server: &str) -> Result<String> {
    let client = reqwest::Client::new();
    let url = format!(
        "{}/newGid",
        trim_suffix_once(&may_replace_localhost(server), "/")
    );
    let resp = client.get(url).send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap_or_default();
    if status != 200 {
        return Err(DtmError::HttpStatus { status, body: text });
    }
    let parsed: serde_json::Value = serde_json::from_str(&text)?;
    let gid = parsed
        .get("gid")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    if gid.is_empty() {
        return Err(DtmError::Other {
            message: format!("newGid error: {text}"),
        });
    }
    Ok(gid)
}

pub async fn must_gen_gid(server: &str) -> String {
    gen_gid(server).await.expect("must_gen_gid failed")
}

#[derive(Clone, Debug)]
pub struct Saga {
    pub tb: TransBase,
    orders: HashMap<i32, Vec<i32>>,
}

impl Saga {
    pub fn new(server: impl Into<String>, gid: impl Into<String>) -> Self {
        Self {
            tb: TransBase::new(gid, "saga", server, ""),
            orders: HashMap::new(),
        }
    }

    pub fn add<T: serde::Serialize>(
        &mut self,
        action: impl Into<String>,
        compensate: impl Into<String>,
        post_data: &T,
    ) -> Result<&mut Self> {
        self.tb.steps.push(HashMap::from([
            ("action".to_string(), action.into()),
            ("compensate".to_string(), compensate.into()),
        ]));
        self.tb.payloads.push(serde_json::to_string(post_data)?);
        Ok(self)
    }

    pub fn add_branch_order(&mut self, branch: i32, pre_branches: Vec<i32>) -> &mut Self {
        self.orders.insert(branch, pre_branches);
        self
    }

    pub fn set_concurrent(&mut self) -> &mut Self {
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
        let client = reqwest::Client::new();
        trans_call_dtm(&client, &self.tb, "submit").await
    }
}

#[derive(Clone, Debug)]
pub struct Msg {
    pub tb: TransBase,
    delay: u64,
}

impl Msg {
    pub fn new(server: impl Into<String>, gid: impl Into<String>) -> Self {
        Self {
            tb: TransBase::new(gid, "msg", server, ""),
            delay: 0,
        }
    }

    pub fn add<T: serde::Serialize>(
        &mut self,
        action: impl Into<String>,
        post_data: &T,
    ) -> Result<&mut Self> {
        self.tb
            .steps
            .push(HashMap::from([("action".to_string(), action.into())]));
        self.tb.payloads.push(serde_json::to_string(post_data)?);
        Ok(self)
    }

    pub fn add_topic<T: serde::Serialize>(
        &mut self,
        topic: impl AsRef<str>,
        post_data: &T,
    ) -> Result<&mut Self> {
        let action = format!("{MSG_TOPIC_PREFIX}{}", topic.as_ref());
        self.add(action, post_data)
    }

    pub fn set_delay(&mut self, delay_seconds: u64) -> &mut Self {
        self.delay = delay_seconds;
        self
    }

    pub async fn prepare(&mut self, query_prepared: &str) -> Result<()> {
        if !query_prepared.is_empty() {
            self.tb.query_prepared = query_prepared.to_string();
        }
        let client = reqwest::Client::new();
        trans_call_dtm(&client, &self.tb, "prepare").await
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
        let client = reqwest::Client::new();
        trans_call_dtm(&client, &self.tb, "submit").await
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

        let mut err_from_query_prepared: Option<DtmError> = None;
        let errb = busi_call(&mut bb).await;
        if let Err(e) = &errb
            && !e.is_failure()
            && !query_prepared.is_empty()
        {
            let client = reqwest::Client::new();
            let (status, text) = trans_request_branch::<serde_json::Value>(
                &client,
                &self.tb,
                reqwest::Method::GET,
                None,
                &bb.branch_id,
                &bb.op,
                query_prepared,
            )
            .await?;
            if let Err(e2) = http_resp_to_dtm_error(status, &text) {
                err_from_query_prepared = Some(e2);
            }
        }

        let should_abort = errb.as_ref().is_err_and(|e| e.is_failure())
            || err_from_query_prepared
                .as_ref()
                .is_some_and(|e| e.is_failure());
        if should_abort {
            let client = reqwest::Client::new();
            let _ = trans_call_dtm(&client, &self.tb, "abort").await;
        } else if err_from_query_prepared.is_none() && errb.is_ok() {
            self.submit().await?;
        }

        errb
    }
}

#[derive(Clone, Debug)]
pub struct Tcc {
    pub tb: TransBase,
}

impl Tcc {
    pub fn from_query_string(query: &str) -> Result<Self> {
        let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let gid = params.get("gid").cloned().unwrap_or_default();
        let trans_type = params.get("trans_type").cloned().unwrap_or_default();
        let dtm = params.get("dtm").cloned().unwrap_or_default();
        let branch_id = params.get("branch_id").cloned().unwrap_or_default();

        if dtm.is_empty() || gid.is_empty() {
            return Err(DtmError::InvalidInput {
                message: format!("bad tcc info. dtm: {dtm}, gid: {gid} parentID: {branch_id}"),
            });
        }

        Ok(Self {
            tb: TransBase::new(gid, trans_type, dtm, branch_id),
        })
    }

    pub async fn global_transaction<F, Fut, R>(
        dtm: impl Into<String>,
        gid: impl Into<String>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Tcc) -> Fut,
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
        FCustom: FnOnce(&mut Tcc),
        F: FnOnce(&mut Tcc) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let mut tcc = Tcc {
            tb: TransBase::new(gid, "tcc", dtm, ""),
        };
        custom(&mut tcc);
        let client = reqwest::Client::new();
        trans_call_dtm(&client, &tcc.tb, "prepare").await?;

        let r = f(&mut tcc).await;
        match r {
            Ok(v) => {
                trans_call_dtm(&client, &tcc.tb, "submit").await?;
                Ok(v)
            }
            Err(e) => {
                tcc.tb.rollback_reason = e.to_string();
                let _ = trans_call_dtm(&client, &tcc.tb, "abort").await;
                Err(e)
            }
        }
    }

    pub async fn call_branch<T: serde::Serialize>(
        &mut self,
        body: &T,
        try_url: &str,
        confirm_url: &str,
        cancel_url: &str,
    ) -> Result<Vec<u8>> {
        let branch_id = self.tb.new_sub_branch_id()?;
        let payload = serde_json::to_string(body)?;
        let client = reqwest::Client::new();
        trans_register_branch(
            &client,
            &self.tb,
            HashMap::from([
                ("data", payload),
                ("branch_id", branch_id.clone()),
                (OP_CONFIRM, confirm_url.to_string()),
                (OP_CANCEL, cancel_url.to_string()),
            ]),
            "registerBranch",
        )
        .await?;

        let (status, text) = trans_request_branch(
            &client,
            &self.tb,
            reqwest::Method::POST,
            Some(body),
            &branch_id,
            OP_TRY,
            try_url,
        )
        .await?;
        http_resp_to_dtm_error(status, &text)?;
        Ok(text.into_bytes())
    }
}

#[derive(Clone, Debug)]
pub struct Xa {
    pub tb: TransBase,
    pub phase2_url: String,
}

impl Xa {
    pub fn from_query_string(query: &str) -> Result<Self> {
        let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        let gid = params.get("gid").cloned().unwrap_or_default();
        let trans_type = params.get("trans_type").cloned().unwrap_or_default();
        let dtm = params.get("dtm").cloned().unwrap_or_default();
        let branch_id = params.get("branch_id").cloned().unwrap_or_default();
        let op = params.get("op").cloned().unwrap_or_default();
        let phase2_url = params.get("phase2_url").cloned().unwrap_or_default();

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

    pub async fn global_transaction<F, Fut, R>(
        dtm: impl Into<String>,
        gid: impl Into<String>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Xa) -> Fut,
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
        FCustom: FnOnce(&mut Xa),
        F: FnOnce(&mut Xa) -> Fut,
        Fut: std::future::Future<Output = Result<R>>,
    {
        let mut xa = Xa {
            tb: TransBase::new(gid, "xa", dtm, ""),
            phase2_url: String::new(),
        };
        custom(&mut xa);
        let client = reqwest::Client::new();
        trans_call_dtm(&client, &xa.tb, "prepare").await?;

        let r = f(&mut xa).await;
        match r {
            Ok(v) => {
                trans_call_dtm(&client, &xa.tb, "submit").await?;
                Ok(v)
            }
            Err(e) => {
                let _ = trans_call_dtm(&client, &xa.tb, "abort").await;
                Err(e)
            }
        }
    }

    pub async fn call_branch<T: serde::Serialize>(
        &mut self,
        body: &T,
        url: &str,
    ) -> Result<Vec<u8>> {
        let branch_id = self.tb.new_sub_branch_id()?;
        let client = reqwest::Client::new();
        let (status, text) = trans_request_branch(
            &client,
            &self.tb,
            reqwest::Method::POST,
            Some(body),
            &branch_id,
            OP_ACTION,
            url,
        )
        .await?;
        http_resp_to_dtm_error(status, &text)?;
        Ok(text.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn gen_gid_calls_new_gid() -> Result<()> {
        let server = httpmock::MockServer::start_async().await;
        let m = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/newGid");
                then.status(200)
                    .json_body(serde_json::json!({ "gid": "gid-123" }));
            })
            .await;

        let gid = gen_gid(&server.url("")).await?;
        assert_eq!(gid, "gid-123");
        m.assert_async().await;
        Ok(())
    }

    #[tokio::test]
    async fn saga_submit_posts_to_submit() -> Result<()> {
        let server = httpmock::MockServer::start_async().await;
        let m = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST)
                    .path("/submit")
                    .json_body_partial(r#"{"gid":"gid-123","trans_type":"saga"}"#);
                then.status(200).body("SUCCESS");
            })
            .await;

        let mut saga = Saga::new(server.url(""), "gid-123");
        saga.add(
            "http://busi.example/TransOut",
            "http://busi.example/TransOutCompensate",
            &serde_json::json!({ "amount": 30 }),
        )?;
        saga.submit().await?;

        m.assert_async().await;
        Ok(())
    }

    #[test]
    fn result_to_http_json_maps_errors() {
        let (status, _) = result_to_http_json::<serde_json::Value>(Err(DtmError::failure("x")));
        assert_eq!(status, 409);
        let (status, _) = result_to_http_json::<serde_json::Value>(Err(DtmError::ongoing("x")));
        assert_eq!(status, 425);
    }
}
