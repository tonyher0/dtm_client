pub const STATUS_PREPARED: &str = "prepared";
pub const STATUS_SUBMITTED: &str = "submitted";
pub const STATUS_SUCCEED: &str = "succeed";
pub const STATUS_FAILED: &str = "failed";
pub const STATUS_ABORTING: &str = "aborting";

pub const RESULT_SUCCESS: &str = "SUCCESS";
pub const RESULT_FAILURE: &str = "FAILURE";
pub const RESULT_ONGOING: &str = "ONGOING";

pub const OP_TRY: &str = "try";
pub const OP_CONFIRM: &str = "confirm";
pub const OP_CANCEL: &str = "cancel";
pub const OP_ACTION: &str = "action";
pub const OP_COMPENSATE: &str = "compensate";
pub const OP_COMMIT: &str = "commit";
pub const OP_ROLLBACK: &str = "rollback";

pub const DB_TYPE_MYSQL: &str = "mysql";
pub const DB_TYPE_POSTGRES: &str = "postgres";
pub const DB_TYPE_REDIS: &str = "redis";

pub const PROTOCOL_GRPC: &str = "grpc";
pub const PROTOCOL_HTTP: &str = "http";

pub const JSON_RPC: &str = "json-rpc";
pub const JSON_RPC_CODE_FAILURE: i64 = -32901;
pub const JSON_RPC_CODE_ONGOING: i64 = -32902;

pub const MSG_DO_BRANCH_0: &str = "00";
pub const MSG_DO_BARRIER_1: &str = "01";
pub const MSG_DO_OP: &str = "msg";
pub const MSG_TOPIC_PREFIX: &str = "topic://";

pub const XA_BARRIER_1: &str = "01";
