use std::collections::HashMap;

use crate::constants::*;
use crate::error::{DtmError, Result};

#[derive(Clone, Debug)]
pub struct BranchBarrier {
    pub trans_type: String,
    pub gid: String,
    pub branch_id: String,
    pub op: String,
    barrier_id: u32,
}

impl BranchBarrier {
    pub fn from(
        trans_type: impl Into<String>,
        gid: impl Into<String>,
        branch_id: impl Into<String>,
        op: impl Into<String>,
    ) -> Result<Self> {
        let trans_type = trans_type.into();
        let gid = gid.into();
        let branch_id = branch_id.into();
        let op = op.into();
        if trans_type.is_empty() || gid.is_empty() || branch_id.is_empty() || op.is_empty() {
            return Err(DtmError::InvalidInput {
                message: format!(
                    "invalid trans info: trans_type={trans_type} gid={gid} branch_id={branch_id} op={op}"
                ),
            });
        }
        Ok(Self {
            trans_type,
            gid,
            branch_id,
            op,
            barrier_id: 0,
        })
    }

    fn new_barrier_id(&mut self) -> String {
        self.barrier_id += 1;
        format!("{:02}", self.barrier_id)
    }

    #[cfg(feature = "http")]
    pub fn from_query_string(query: &str) -> Result<Self> {
        let params: HashMap<String, String> = url::form_urlencoded::parse(query.as_bytes())
            .into_owned()
            .collect();
        Self::from(
            params.get("trans_type").cloned().unwrap_or_default(),
            params.get("gid").cloned().unwrap_or_default(),
            params.get("branch_id").cloned().unwrap_or_default(),
            params.get("op").cloned().unwrap_or_default(),
        )
    }

    #[cfg(feature = "grpc")]
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
        let branch_id = get("dtm-branch_id");
        let op = get("dtm-op");
        Self::from(trans_type, gid, branch_id, op)
    }
}

#[cfg(feature = "barrier-redis")]
impl BranchBarrier {
    pub async fn redis_check_adjust_amount<C: redis::aio::ConnectionLike + Send>(
        &mut self,
        conn: &mut C,
        key: &str,
        amount: i64,
        barrier_expire_seconds: usize,
    ) -> Result<()> {
        let bid = self.new_barrier_id();
        let bkey1 = format!("{}-{}-{}-{}", self.gid, self.branch_id, self.op, bid);
        let origin_op = match self.op.as_str() {
            OP_CANCEL => OP_TRY,
            OP_COMPENSATE => OP_ACTION,
            _ => "",
        };
        let bkey2 = format!("{}-{}-{}-{}", self.gid, self.branch_id, origin_op, bid);

        let script = redis::Script::new(
            r#" -- RedisCheckAdjustAmount
local v = redis.call('GET', KEYS[1])
local e1 = redis.call('GET', KEYS[2])

if v == false or v + ARGV[1] < 0 then
	return 'FAILURE'
end

if e1 ~= false then
	return 'DUPLICATE'
end

redis.call('SET', KEYS[2], 'op', 'EX', ARGV[3])

if ARGV[2] ~= '' then
	local e2 = redis.call('GET', KEYS[3])
	if e2 == false then
		redis.call('SET', KEYS[3], 'rollback', 'EX', ARGV[3])
		return
	end
end
redis.call('INCRBY', KEYS[1], ARGV[1])
"#,
        );

        let v: Option<String> = script
            .key(key)
            .key(&bkey1)
            .key(&bkey2)
            .arg(amount)
            .arg(origin_op)
            .arg(barrier_expire_seconds)
            .invoke_async(conn)
            .await?;

        if self.op == MSG_DO_OP && v.as_deref() == Some("DUPLICATE") {
            return Err(DtmError::duplicated("DUPLICATED"));
        }
        if v.as_deref() == Some(RESULT_FAILURE) {
            return Err(DtmError::failure("FAILURE"));
        }
        Ok(())
    }

    pub async fn redis_query_prepared<C: redis::aio::ConnectionLike + Send>(
        &mut self,
        conn: &mut C,
        barrier_expire_seconds: usize,
    ) -> Result<()> {
        let bkey1 = format!(
            "{}-{}-{}-{}",
            self.gid, MSG_DO_BRANCH_0, MSG_DO_OP, MSG_DO_BARRIER_1
        );
        let script = redis::Script::new(
            r#" -- RedisQueryPrepared
local v = redis.call('GET', KEYS[1])
if v == false then
	redis.call('SET', KEYS[1], 'rollback', 'EX', ARGV[1])
	v = 'rollback'
end
if v == 'rollback' then
	return 'FAILURE'
end
"#,
        );
        let v: Option<String> = script
            .key(&bkey1)
            .arg(barrier_expire_seconds)
            .invoke_async(conn)
            .await?;
        if v.as_deref() == Some(RESULT_FAILURE) {
            return Err(DtmError::failure("FAILURE"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn barrier_new_barrier_id_is_two_digits() {
        let mut bb = BranchBarrier::from("msg", "gid", "00", "msg").unwrap();
        assert_eq!(bb.new_barrier_id(), "01");
        assert_eq!(bb.new_barrier_id(), "02");
    }
}
