use std::collections::HashMap;

use crate::error::{DtmError, Result};

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

#[derive(Clone, Debug, Default)]
pub struct BranchIdGen {
    pub branch_id: String,
    sub_branch_id: u8,
}

impl BranchIdGen {
    pub fn new(branch_id: impl Into<String>) -> Self {
        Self {
            branch_id: branch_id.into(),
            sub_branch_id: 0,
        }
    }

    pub fn new_sub_branch_id(&mut self) -> Result<String> {
        if self.sub_branch_id >= 99 {
            return Err(DtmError::InvalidInput {
                message: "branch id is larger than 99".to_string(),
            });
        }
        if self.branch_id.len() >= 20 {
            return Err(DtmError::InvalidInput {
                message: "total branch id is longer than 20".to_string(),
            });
        }
        self.sub_branch_id = self.sub_branch_id.saturating_add(1);
        Ok(self.current_sub_branch_id())
    }

    pub fn current_sub_branch_id(&self) -> String {
        format!("{}{:02}", self.branch_id, self.sub_branch_id)
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TransOptions {
    #[serde(default, skip_serializing_if = "is_false", rename = "wait_result")]
    pub wait_result: bool,

    #[serde(
        default,
        skip_serializing_if = "is_zero_i64",
        rename = "timeout_to_fail"
    )]
    pub timeout_to_fail: i64,

    #[serde(
        default,
        skip_serializing_if = "is_zero_i64",
        rename = "request_timeout"
    )]
    pub request_timeout: i64,

    #[serde(
        default,
        skip_serializing_if = "is_zero_i64",
        rename = "retry_interval"
    )]
    pub retry_interval: i64,

    #[serde(
        default,
        skip_serializing_if = "HashMap::is_empty",
        rename = "branch_headers"
    )]
    pub branch_headers: HashMap<String, String>,

    #[serde(default, skip_serializing_if = "is_false", rename = "concurrent")]
    pub concurrent: bool,

    #[serde(default, skip_serializing_if = "is_zero_i64", rename = "retry_limit")]
    pub retry_limit: i64,

    #[serde(default, skip_serializing_if = "is_zero_i64", rename = "retry_count")]
    pub retry_count: i64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TransBase {
    #[serde(rename = "gid")]
    pub gid: String,

    #[serde(rename = "trans_type")]
    pub trans_type: String,

    #[serde(skip)]
    pub dtm: String,

    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "custom_data"
    )]
    pub custom_data: Option<String>,

    #[serde(flatten)]
    pub options: TransOptions,

    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "steps")]
    pub steps: Vec<HashMap<String, String>>,

    #[serde(default, skip_serializing_if = "Vec::is_empty", rename = "payloads")]
    pub payloads: Vec<String>,

    #[serde(skip)]
    pub bin_payloads: Vec<Vec<u8>>,

    #[serde(skip)]
    pub id_gen: BranchIdGen,

    #[serde(skip)]
    pub op: String,

    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        rename = "query_prepared"
    )]
    pub query_prepared: String,

    #[serde(default, skip_serializing_if = "String::is_empty", rename = "protocol")]
    pub protocol: String,

    #[serde(
        default,
        skip_serializing_if = "String::is_empty",
        rename = "rollback_reason"
    )]
    pub rollback_reason: String,
}

impl TransBase {
    pub fn new(
        gid: impl Into<String>,
        trans_type: impl Into<String>,
        dtm: impl Into<String>,
        branch_id: impl Into<String>,
    ) -> Self {
        Self {
            gid: gid.into(),
            trans_type: trans_type.into(),
            dtm: dtm.into(),
            custom_data: None,
            options: TransOptions::default(),
            steps: Vec::new(),
            payloads: Vec::new(),
            bin_payloads: Vec::new(),
            id_gen: BranchIdGen::new(branch_id),
            op: String::new(),
            query_prepared: String::new(),
            protocol: String::new(),
            rollback_reason: String::new(),
        }
    }

    pub fn with_global_trans_request_timeout(&mut self, timeout_seconds: i64) {
        self.options.request_timeout = timeout_seconds;
    }

    pub fn with_retry_limit(&mut self, retry_limit: i64) {
        self.options.retry_limit = retry_limit;
    }

    pub fn new_sub_branch_id(&mut self) -> Result<String> {
        self.id_gen.new_sub_branch_id()
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcRequest<T> {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    pub params: T,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct JsonRpcResponse {
    pub error: Option<JsonRpcError>,
}
