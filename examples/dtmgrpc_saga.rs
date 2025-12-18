use dtm_client::dtmgrpc;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BusiReq {
    #[prost(int64, tag = "1")]
    pub amount: i64,
    #[prost(int64, tag = "2")]
    pub user_id: i64,
    #[prost(string, tag = "3")]
    pub trans_out_result: String,
    #[prost(string, tag = "4")]
    pub trans_in_result: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BusiReply {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[tokio::main]
async fn main() -> dtm_client::Result<()> {
    let dtm = std::env::var("DTM_GRPC_ADDR").unwrap_or_else(|_| "localhost:36790".to_string());
    let busi = std::env::var("BUSI_GRPC_ADDR").unwrap_or_else(|_| "localhost:50589".to_string());

    let gid = match std::env::var("GID") {
        Ok(gid) if !gid.is_empty() => gid,
        _ => dtmgrpc::gen_gid(&dtm).await?,
    };

    let req = BusiReq {
        amount: 30,
        user_id: 0,
        trans_out_result: String::new(),
        trans_in_result: String::new(),
    };

    let mut saga = dtmgrpc::SagaGrpc::new(&dtm, &gid);
    saga.add(
        format!("{busi}/busi.Busi/TransOut"),
        format!("{busi}/busi.Busi/TransOutRevert"),
        req.clone(),
    )?;
    saga.add(
        format!("{busi}/busi.Busi/TransIn"),
        format!("{busi}/busi.Busi/TransInRevert"),
        req,
    )?;

    saga.submit().await?;
    println!("submitted grpc saga gid={gid}");
    Ok(())
}
